import asyncore
import msgpack
from collections import deque
import random
import socket
import time

sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
ip, port = '::', 7000
nodeid = 0xbeef
msg = {'nodeid': nodeid,
	   'oper': None,
	   'echo': None,
	   'data': None}

def insert(data, echo):
	msg['data'] = data
	msg['echo'] = echo
	msg['oper'] = 'INSERT'
	return msgpack.packb(msg)

sendqueue = deque()

class MPDBClient(asyncore.dispatcher):
	def __init__(self):
		self.i = 1
		asyncore.dispatcher.__init__(self)
		self.create_socket(socket.AF_INET6, socket.SOCK_DGRAM)
		self.bind(('::',0))
		self.acked = set()
	
	def handle_read(self):
		data= self.recv(2048)
		unp = msgpack.unpackb(data)
		print 'RECEIVED', unp
		self.acked.add(unp['echo'])
	
	def needretry(self):
		return self.i >= ((max(self.acked) if self.acked else 1) + 3)
	
	def writable(self):
		time.sleep(.5)
		return True
	
	def handle_write(self):
		if self.needretry():
			for echo in filter(lambda x: x not in self.acked, xrange(self.i)):
				print 'write',echo
				self.sendto(insert({"a": 1}, echo), (ip, port))
			return
		else:
			if random.randint(1,10) not in [1,2,3]:
				print 'write',self.i
				self.sendto(insert({"a": 1}, self.i), (ip, port))
			else:
				print 'DROP',self.i
		self.i += 1

client = MPDBClient()
asyncore.loop()
