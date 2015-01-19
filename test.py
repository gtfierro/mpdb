import msgpack
import socket
import time

sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
ip, port = '::', 7000
echo = 1
data = {'a': 1, 'b': 2}

msg = {'nodeid': 0xbeef,
       'oper': 'PERSIST',
	   'echo': echo,
	   'data': data}
sendbytes = msgpack.packb(msg)
sock.sendto(sendbytes, (ip, port))

time.sleep(.5)
echo = 2
msg['echo'] = echo
sendbytes = msgpack.packb(msg)
sock.sendto(sendbytes, (ip, port))

time.sleep(.5)
echo = 4
msg['echo'] = echo
sendbytes = msgpack.packb(msg)
sock.sendto(sendbytes, (ip, port))

time.sleep(.5)
echo = 3
msg['echo'] = echo
sendbytes = msgpack.packb(msg)
sock.sendto(sendbytes, (ip, port))
