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
		self.unacked = {}
	
	def handle_read(self):
		data= self.recv(2048)
		unp = msgpack.unpackb(data)
		print 'RECEIVED', unp
		self.unacked.pop(unp['echo'])
	
	def writable(self):
		time.sleep(.5)
		return True
	
	def sendmaybe(self, msg):
		if random.randint(1,10) not in range(1,4):
			self.sendto(msg, (ip, port))
		else:
			print 'DROP'
	
	def handle_write(self):
		if len(self.unacked):
			for echo, struct in self.unacked.iteritems():
				print 'write',echo
				if time.time() - struct['sent'] > 10:
					print "TIMEOUT", echo
					continue # 10 second timeout on retry
				self.sendmaybe(struct['msg'])#, (ip, port))
			return
		else:
			msg = insert({"a": 1}, self.i)
			self.unacked[self.i] = {'msg': msg, 'sent': time.time()}
			if random.randint(1,10) not in [1,2,3]:
				print 'write',self.i
				self.sendmaybe(msg)#, (ip, port))
			else:
				print 'DROP',self.i
		self.i += 1

client = MPDBClient()
asyncore.loop()
