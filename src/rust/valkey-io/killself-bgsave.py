#!/usr/bin/env python3
"""CLIENT KILL SKIPME no during BGSAVE must still deliver its own reply.

Usage: killself-bgsave.py <port> [iterations, default 5] [fork|forkless, default fork]

With a fork child alive, freeClient() shuts the socket down before closing
it (networking.c). Over a Rust connection the `:N` reply was still in the
Rust output buffer and was discarded (the client saw EOF). Exits 1 on any
lost reply.
"""
import socket, sys, time

P = int(sys.argv[1])
N = int(sys.argv[2]) if len(sys.argv) > 2 else 5
METHOD = sys.argv[3] if len(sys.argv) > 3 else "fork"


def cmd(*a):
    b = b"*%d\r\n" % len(a)
    for x in a:
        x = x.encode()
        b += b"$%d\r\n%s\r\n" % (len(x), x)
    return b


def call(s, *a):
    s.sendall(cmd(*a))
    return s.recv(1000)


bad = 0
for i in range(N):
    others = [socket.create_connection(("127.0.0.1", P)) for _ in range(3)]
    s = socket.create_connection(("127.0.0.1", P))
    s.settimeout(3)
    call(s, "flushall")
    call(s, "set", "k", "v")
    call(s, "config", "set", "rdb-key-save-delay", "10000000")
    call(s, "config", "set", "bgsave-default-method", METHOD)
    call(s, "bgsave")
    time.sleep(0.05)
    s.sendall(cmd("client", "kill", "skipme", "no"))
    try:
        d = s.recv(100)
    except Exception as e:
        d = repr(e).encode()
    s2 = socket.create_connection(("127.0.0.1", P))
    s2.settimeout(3)
    call(s2, "config", "set", "rdb-key-save-delay", "0")
    call(s2, "flushall")
    for _ in range(100):
        if b"rdb_bgsave_in_progress:0" in call(s2, "info", "persistence"):
            break
        time.sleep(0.05)
    s2.close()
    for o in others:
        o.close()
    if not d.startswith(b":"):
        print("iter", i, "got", d)
        bad += 1
print("lost replies:", bad, "of", N)
sys.exit(1 if bad else 0)
