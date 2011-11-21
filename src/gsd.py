# encoding: utf-8

#!/usr/bin/env python

import getopt
import sys
import os
import subprocess
import signal
import time
from collections import deque
import msgpack
import zmq
import errno
import logging

VERSION = r'APS10'
EMPTY = r''

def microtime():
    return int(round(time.time() * 1000 * 1000))

def millitime():
    return int(round(time.time() * 1000))

class Workers:
    def __init__(self):
        self.workers = {} # { wid : [created, activated, status] }
        self.queue = deque()

    def is_available(self):
        if self.queue:
            return True
        else:
            return False

    def add(self, wid):
        now = millitime()
        if wid not in self.workers:
            self.queue.append(wid)
            self.workers[wid] = [now, now, False]
        else:
            if self.workers[wid][2]:
                self.queue.append(wid)
            self.workers[wid][1 :] = [now, False]

    def remove(self, wid):
        try:
            del self.workers[wid]
        except KeyError, e:
            pass

        try:
            self.queue.remove(wid)
        except ValueError, e:
            pass

    def disable(self, wid):
        '''
        make it impossible to add or borrow this worker
        '''
        self.workers[wid][2] = False
        try:
            self.queue.remove(wid)
        except ValueError, e:
            pass

    def borrow(self):
        try:
            wid = self.queue.popleft()
        except IndexError:
            return None
        else:
            self.workers[wid][2] = True
            return wid

    def dead_since(self, timestamp):
        '''
        return a wid list of workers who haven't sent a heartbeat or response since timestamp
        '''
        return [wid for wid, worker in self.workers.items()
                if worker[1] < timestamp]

    def extra_spare(self, num):
        '''
        return a wid list of extra spare workers
        '''
        if num <= 0: return []
        return [wid for wid in self.queue][: num]

    def expired_list(self, timestamp, num):
        '''
        return a wid list of workers whose creation time is earlier than timestamp
        '''
        if num <= 0: return []
        return [wid for wid in self.queue
                if self.workers[wid][0] < timestamp][: num]

    def num(self):
        '''
        return the number of workers
        '''
        return len(self.workers)

    def num_spare(self):
        '''
        return the number of spare workers
        '''
        return len(self.queue)


class Device():
    def __init__(self, options):
        self.options = options
        self.workers = Workers()
        self.pendings = deque()
        self.pids = set()

        def create_socket(socktype, endpoints):
            socket = zmq.Socket(zmq.Context.instance(), socktype)
            socket.setsockopt(zmq.LINGER, 0)
            for endpoint in endpoints:
                socket.bind(endpoint)
            return socket

        self.client_socket = create_socket(zmq.XREP, self.options.feps)
        self.worker_socket = create_socket(zmq.XREP, self.options.beps)
        self.monitor_socket = create_socket(zmq.PUB, self.options.meps)

        self.interrupted = False
        self.last_maintain = 0

        signal.signal(signal.SIGCHLD, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):

        if signum == signal.SIGCHLD:
            pid, status = os.waitpid(-1, os.WNOHANG)
            while (pid, status) != (0, 0):
                self.pids.discard(pid)
                self.workers.remove(str(pid))
                try:
                    pid, status = os.waitpid(-1, os.WNOHANG)
                except OSError, e:
                    break

        if signum == signal.SIGTERM or signum == signal.SIGINT:
            for pid in self.pids:
                os.kill(pid, signum)
            self.stop()

    def start(self):

        # For monitor
        self.start_time    = int(round(time.time()))
        self.num_requests  = 0
        self.num_responses = 0
        self.num_forks     = 0

        logging.info(self.options.__dict__)
        self.loop()

    def stop(self):
        self.interrupted = True

    def maintain(self):
        now = millitime()

        if now - self.last_maintain < self.options.interval:
            return

        # remove dead workers
        for wid in self.workers.dead_since(now - self.options.timeout):
            try:
                os.kill(int(wid), signal.SIGTERM)
                logging.info('remove dead %s', wid)
            except OSError as e:
                logging.error("kill thread fail")
            self.pids.discard(int(wid))
            self.workers.remove(wid)

        # remove expired workers
        for wid in self.workers.expired_list(now - self.options.expire * 60 * 1000, self.workers.num() - self.options.minw):
            self.remove_worker(wid)
            logging.info('remove expired %s', wid)

        # remove extra spare workers
        for wid in self.workers.extra_spare(self.workers.num_spare() - self.options.spaw):
            self.remove_worker(wid)
            logging.info('remove extra spare %s', wid)

        # create new workers
        for i in xrange(self.workers.num(), self.options.minw + 1): # plus one for removing expired worker
            self.fork_worker()

        self.last_maintain = now

    def remove_worker(self, wid):
        self.workers.disable(wid)
        frames = [wid, EMPTY, VERSION, '\x02']
        self.worker_socket.send_multipart(frames)

    def fork_worker(self):
        if len(self.pids) < self.options.maxw:
            p = subprocess.Popen(self.options.args)
            self.pids.add(p.pid)
            self.num_forks = self.num_forks + 1
            logging.info('create %d', p.pid)

    def loop(self):
        poller = zmq.Poller()
        poller.register(self.client_socket, zmq.POLLIN)
        poller.register(self.worker_socket, zmq.POLLIN)

        while not self.interrupted:
            self.maintain()

            try:
                events = poller.poll(self.options.interval)
            except zmq.ZMQError as e:
                if e.errno == errno.EINTR:    # system interrupted
                    continue
                else:
                    pass

            for socket, flags in events:
                if socket == self.worker_socket:
                    self.handle_worker()
                elif socket == self.client_socket:
                    self.handle_client()
                else:
                    assert False

    def handle_client(self):
        while True:
            try:
                frames = self.client_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == errno.EAGAIN:   # no more message to handle
                    break
                else:
                    pass

            self.forward_to_worker(frames)

    def forward_to_worker(self, frames):
        i = frames.index(EMPTY)
        if frames[i+1] != VERSION:
            pass # handle version mismatch
        sequence, timestamp, expiry = msgpack.unpackb(frames[i+2])
        # handle expired

        wid = self.workers.borrow()
        if not wid:
            self.pendings.append(frames)
            self.fork_worker()
        else:
            frames = self.build_worker_request(wid, frames[:i], frames[i+2:])
            self.worker_socket.send_multipart(frames)
            self.num_requests = self.num_requests + 1

        return wid

    def handle_pendings(self):
        while self.pendings:
            frames = self.pendings.popleft()
            if not self.forward_to_worker(frames):
                break

    def handle_worker(self):
        while True:
            try:
                frames = self.worker_socket.recv_multipart(zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == errno.EAGAIN:   # no more message to handle
                    break
                else:
                    pass

            i = frames.index(EMPTY)
            assert(i == 1)
            wid = frames[0]

            if frames[i+1] != VERSION:
                pass # handle version mismatch

            command = frames[i+2]
            if command == '\x00':   # REQUEST
                j = frames.index(EMPTY, i+3)
                frames = self.build_client_reply(frames[i+3:j], frames[j+1:])
                self.client_socket.send_multipart(frames)
                self.num_responses = self.num_responses + 1
                self.workers.add(wid)
                self.handle_pendings()

            elif command == '\x01': # HEARTBEAT
                self.workers.add(wid)
                self.handle_pendings()

            elif command == '\x02': # GOODBYE
                self.remove_worker(wid)

            elif command == '\x03': # STATUS
                frames = self.build_status_reply(wid)
                self.worker_socket.send_multipart(frames)

            else:
                pass # handle unknown command

    def build_worker_request(self, wid, envelope, body):
        frames = [wid, EMPTY, VERSION, '\x00']
        frames.extend(envelope)
        frames.append(EMPTY)
        frames.extend(body)
        return frames

    def build_client_reply(self, envelope, body):
        frames = envelope[:]
        frames.append(EMPTY)
        frames.append(VERSION)
        frames.extend(body)
        return frames

    def build_status_reply(self, wid):
        frames = [wid, EMPTY, VERSION]
        frames.extend([str(self.options.pid),
                       self.options.feps[0],
                       str(self.start_time),
                       str(self.num_requests),
                       str(self.num_responses),
                       str(self.num_forks),
                       str(self.workers.num())])
        return frames


class Options:
    """
options:
    -h, --help
        Show this help message and exit

    -f, --frontend=<endpoint>
        The binding endpoints where clients will connect to
        [default: tcp://*:5000]

    -b, --backend=<endpoint>
        The binding endpoints where workers will connect to
        [default: ipc:///tmp/gsd-{pid}.ipc]

    -m, --monitor=<endpoint>
        The binding endpoints where monitor events will be published to

    -n, --min-worker=<num>
        The mininum number of workers should be started before accepting
        request [default: 1]

    -x, --max-worker=<num>
        The maxinum number of workers [default: 32]

    -s, --spare-worker=<num>
        The maxinum number of spare workers [default: 8]

    -t, --timeout=<milliseconds>
        Worker timeout in millisecond [default: 10000]

    -i, --interval=<milliseconds>
        Maintain interval in millisecond [default: 1000]

    -e, --expire=<minutes>
        Worker will be killed after the given minutes since creation [default 10]

    -d, --daemon

    -v, --verbose
    """

    def __init__(self, argv):
        self.prog = os.path.basename(argv[0])

        self.feps     = []    # frontend endpoints
        self.beps     = []    # backend endpoints
        self.meps     = []    # monitor endpoints
        self.minw     = 1     # min worker
        self.maxw     = 32    # max worker
        self.spaw     = 8     # spare worker
        self.timeout  = 10000 # worker timeout (miliseconds)
        self.interval = 1000  # maintain interval (miliseconds)
        self.expire   = 10    # worker expire time (minutes)
        self.daemon   = False # daemon
        self.args     = []    # worker command line

        self.errno = 0

        try:
            self.parse(argv)
        except:
            self.usage()
            self.errno = 1
            return

        if not self.args:
            self.usage()
            self.errno = 3
            return

        if self.daemon:
            self.daemonize(stdout = '/tmp/pygsd.log', stderr = '/tmp/pygsd.log')

        self.pid = os.getpid()

        if not self.feps:
            self.feps = ['tcp://*:5000']

        if not self.beps:
            self.beps = ['ipc:///tmp/gsd-%d.ipc' % self.pid]

    def parse(self, argv):
        opts, self.args = getopt.getopt(argv[1:], 'hf:b:m:n:x:s:t:i:e:dv', ['help',
            'frontend=', 'backend=', 'monitor=',
            'min-worker=', 'max-worker=', 'spare-worker=',
            'timeout=', 'interval=', 'expire=',
            'daemon=', 'verbose='])

        for o, a in opts:
            if o in ('-h', '--help'):
                self.usage()
                self.errno = 1
                return
            elif o in ('-f', '--frontend'):
                self.feps.append(a)
            elif o in ('-b', '--backend'):
                self.beps.append(a)
            elif o in ('-m', '--monitor'):
                self.meps.append(a)
            elif o in ('-n', '--min-worker'):
                self.minw = int(a)
            elif o in ('-x', '--max-worker'):
                self.maxw = int(a)
            elif o in ('-s', '--spare-worker'):
                self.spaw = int(a)
            elif o in ('-t', '--timeout='):
                self.timeout = int(a)
            elif o in ('-i', '--interval='):
                self.interval = int(a)
            elif o in ('-e', '--expire='):
                self.expire = int(a)
            elif o in ('-d', '--daemon'):
                self.daemon = True
            elif o in ('-v', '--verbose'):
                self.verbose = True
            else:
                pass


    def usage(self):
        print('usage: %s [options] <worker command line>' % self.prog)
        print self.__doc__

    def daemonize(self, stdin = '/dev/null', stdout = '/dev/null', stderr = '/dev/null'):
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.exit(4)

        os.chdir('/')
        os.umask(0)
        os.setsid()

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.exit(4)

        for f in sys.stdout, sys.stderr: f.flush()
        si = file(stdin, 'r')
        so = file(stdout, 'a+')
        se = file(stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())


def main():
    logging.basicConfig(
        filename = '/tmp/pygsd.log',
        format   = '%(asctime)s %(message)s',
        level    = logging.INFO
    )

    options = Options(sys.argv)

    if options.errno:
        return options.errno

    device = Device(options)
    device.start()


if __name__ == '__main__':
    sys.exit(main())

__all__ = []

