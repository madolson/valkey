import socket, sys, threading
port = int(sys.argv[1])
def enc(*a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
def conn():
    return socket.create_connection(("127.0.0.1", port))
def readn(s, n):
    got = b""
    while got.count(b"\r\n") < n:
        d = s.recv(65536)
        if not d: break
        got += d
    return got
s = conn()
s.sendall(enc(b"NOSUCHCMD", b"x") + enc(b"GET") + enc(b"SET", b"k1", b"v1") + enc(b"GET", b"k1"))
print("errors then ok:", readn(s, 5))
# blocking command in a mixed pipeline: BLPOP on one engine, GETs on others queue behind it
b = conn()
b.sendall(enc(b"BLPOP", b"blk", b"5") + enc(b"GET", b"k1") + enc(b"GET", b"pipea") + enc(b"PING"))
p = conn()
p.sendall(enc(b"RPUSH", b"blk", b"hello"))
print("rpush:", readn(p, 1))
print("blpop then queued replies in order:", readn(b, 8))
q = conn()
q.sendall(enc(b"SET", b"qq", b"1") + enc(b"QUIT"))
print("quit:", readn(q, 2), "then EOF:", q.recv(10) == b"")
s.sendall(b"PING\r\n")
print("inline ping:", readn(s, 1))
