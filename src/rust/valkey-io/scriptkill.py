#!/usr/bin/env python3
"""Repro for the io/handoff SCRIPT KILL hang (scripting.tcl 'Timed out script
does not cause a false dead client'). Usage: killrepro.py <port> <iters>
Exit 1 on the first iteration where SCRIPT KILL gets no reply within 5 s."""
import socket, sys, time
P, N = int(sys.argv[1]), int(sys.argv[2])
def conn():
    s = socket.create_connection(("127.0.0.1", P)); s.settimeout(5); return s
def cmd(*a):
    b = b"*%d\r\n" % len(a)
    for x in a:
        x = x.encode() if isinstance(x, str) else x
        b += b"$%d\r\n%s\r\n" % (len(x), x)
    return b
def readline(s, buf):
    while b"\r\n" not in buf[0]:
        d = s.recv(65536)
        if not d: raise EOFError
        buf[0] += d
    l, buf[0] = buf[0].split(b"\r\n", 1)
    return l
r = conn(); rb = [b""]
r.sendall(cmd("config", "set", "lua-time-limit", "10")); readline(r, rb)
for i in range(N):
    rd = conn(); db = [b""]
    rd.sendall(cmd("eval", "while 1 do redis.call('ping') end", "0") + cmd("ping"))
    t0 = time.time()
    while True:
        r.sendall(cmd("ping"))
        try:
            l = readline(r, rb)
        except socket.timeout:
            print(f"HANG iter {i}: PING during the script got no reply in 5 s"); sys.exit(1)
        if l.startswith(b"-BUSY"): break
        if time.time() - t0 > 5: print("script never got busy", i); sys.exit(2)
        time.sleep(0.01)
    r.sendall(cmd("script", "kill"))
    try:
        l = readline(r, rb)
    except socket.timeout:
        print(f"HANG iter {i}: no reply to SCRIPT KILL"); sys.exit(1)
    while True:
        r.sendall(cmd("ping")); l = readline(r, rb)
        if l == b"+PONG": break
        time.sleep(0.01)
    a = readline(rd, db); b = readline(rd, db)
    assert b"killed" in a and b == b"+PONG", (a, b)
    rd.close()
print(f"OK {N} iterations")
