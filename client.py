import asyncore
import msgpack
import socket
import time
import random
from io import BytesIO

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

class MPDBClient(asyncore.dispatcher):
    def __init__(self):
        self.i = 1
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET6, socket.SOCK_DGRAM)
        self.bind(('::',0))
        self.window = {}
        self.windowStart = 1
        self.inair = 0

    def handle_read(self):
        data= self.recv(2048)
        buf = BytesIO(data)
        unpacker = msgpack.Unpacker(buf)
        for unp in unpacker:
            if random.randint(1,10) not in []:
                print 'RECEIVED', unp
                if unp['echo'] in self.window:
                    self.inair -= 1
                    self.window.pop(unp['echo'])
                    self.windowStart += 1
            else:
                print 'DROP RECV',unp['echo']
    
    def writable(self):
        time.sleep(2)
        return True
    
    def sendmaybe(self, msg):
        if random.randint(1,10) not in []:
            self.sendto(msg, (ip, port))
        else:
            print 'DROP'

    def handle_write(self):
        if len(self.window) == 5:
            # do resend
            for echo, msg in self.window.iteritems():
                print 'resending',echo
                time.sleep(2)
                self.sendmaybe(msg['msg'])
            return
        else:
            # send next one
            msg = insert({str(self.i): self.i}, self.i)
            self.window[self.i] = {'msg': msg, 'sent': time.time()}
            print 'write',self.i
            self.sendmaybe(msg)
            self.i += 1


        

client= MPDBClient()
asyncore.loop()
