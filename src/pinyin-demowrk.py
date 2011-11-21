# -*- coding: utf-8 -*-

import pinyin
import zmq
import msgpack
import time
import os
import sys

VERSION=r'APS10'

socket = None
converter = pinyin.Converter()
interrupted = False

def loop():
    process_heartbeat()

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)

    global interrupted
    while not interrupted:
        events = poller.poll(1000)
        if os.getppid() == 1:
            break

        if events:
            process_request()
        else:
            process_heartbeat()

def process_request():
    frames = socket.recv_multipart()

    # killed by gsd
    i = frames.index('')
    command = frames[i + 2]
    if command == '\x02':
        global interrupted
        interrupted = True
        return

    i = frames.index('', 1)

    sequence, timestamp, expiry = msgpack.unpackb(frames[i+1])
    method = frames[i+2]
    params = msgpack.unpackb(frames[i+3])

    try:
        global converter
        ret = getattr(converter, method)(*params)
    except:
        ret = ''

    frames = frames[:i+1]
    now = int(round(time.time() * 1000))
    frames.append(msgpack.packb([sequence, now, 200]))
    frames.append(msgpack.packb(ret))

    socket.send_multipart(frames)


def process_heartbeat():
    now = int(round(time.time() * 1000))
    socket.send_multipart(['', VERSION, '\x01', msgpack.packb(now)])

if __name__ == '__main__':
    if len(sys.argv) < 2:
        endpoint = 'ipc:///tmp/gsd-{0}.ipc'.format(os.getppid())
    else:
        endpoint = sys.argv[1]

    socket = zmq.Socket(zmq.Context.instance(), zmq.XREQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.IDENTITY, str(os.getpid()))
    socket.connect(endpoint)

    loop()

