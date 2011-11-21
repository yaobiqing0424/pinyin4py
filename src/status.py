# encoding: utf-8

#!/usr/bin/env python

import sys
import zmq
import time

VERSION = r'APS10'

socket = zmq.Socket(zmq.Context.instance(), zmq.REQ)
socket.connect(sys.argv[1])
socket.send_multipart([VERSION, '\x03'])
status = socket.recv_multipart()

print 'Pid:', status[1]
print 'Frontend:', status[2]
print 'Start Time:', time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(status[3])))
print 'Requests:', status[4]
print 'Responses:', status[5]
print 'Forks:', status[6]
print 'Workers:', status[7]