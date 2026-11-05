import socket, sys
port = int(sys.argv[1])
def enc(*a):
    return b"*%d\r\n" % len(a) + b"".join(b"$%d\r\n%s\r\n" % (len(x), x) for x in a)
s = socket.create_connection(("127.0.0.1", port))
keys = [b"pipe%c" % c for c in b"abcdefghijklmnop"]
req = b"".join(enc(b"SET", k, b"v-" + k) for k in keys) + b"".join(enc(b"GET", k) for k in keys) + enc(b"PING")
s.sendall(req)
want = b"+OK\r\n" * len(keys) + b"".join(b"$%d\r\nv-%s\r\n" % (len(k) + 2, k) for k in keys) + b"+PONG\r\n"
got = b""
while len(got) < len(want):
    d = s.recv(65536)
    if not d: break
    got += d
print("one write of %d SET + %d GET + PING across engines; replies in request order: %s" % (len(keys), len(keys), got == want))
print(got.replace(b"\r\n", b" ").decode())
