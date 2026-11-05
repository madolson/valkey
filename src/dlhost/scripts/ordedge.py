#!/usr/bin/env python3
"""Edge cases for --order: MULTI/EXEC routing and blocked commands.
usage: ordedge.py PORT NENGINES MODE   (MODE: reply|single|chain)"""
import os
import sys
import time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ordertest import Conn, cmd, tag_for

port, n, mode = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
t0, t1 = tag_for(0, n), tag_for(n - 1, n)
fails = 0


def check(name, got, want):
    global fails
    ok = got == want
    fails += not ok
    print("%-4s %s: got %r want %r" % ("ok" if ok else "FAIL", name, got, want))


def rep(c):
    try:
        return c.reply()
    except RuntimeError as e:
        return "ERR:" + str(e).split()[0]


a, b = Conn(port), Conn(port)
a.s.sendall(cmd("FLUSHALL"))
rep(a)
for e in range(n):  # FLUSHALL only reaches engine 0
    a.s.sendall(cmd("DEL", "k{%s}" % tag_for(e, n), "flag{%s}" % tag_for(e, n), "list{%s}" % tag_for(e, n)))
    rep(a)

# 1. transaction on one engine, pipelined
a.s.sendall(cmd("MULTI") + cmd("SET", "k{%s}" % t1, "1") + cmd("INCR", "k{%s}" % t1) + cmd("PING") + cmd("EXEC"))
check("multi one engine", [rep(a) for _ in range(5)], [b"OK", b"QUEUED", b"QUEUED", b"QUEUED", [b"OK", b"2", b"PONG"]])

# 2. cross-engine transaction aborts and writes nothing
a.s.sendall(cmd("MULTI") + cmd("SET", "k{%s}" % t1, "x") + cmd("SET", "k{%s}" % t0, "x") + cmd("EXEC"))
r = [rep(a) for _ in range(4)]
check("multi cross-engine", [r[0], r[1], r[2][:4], r[3]], [b"OK", b"QUEUED", "ERR:", "ERR:-EXECABORT"])
a.s.sendall(cmd("GET", "k{%s}" % t1) + cmd("GET", "k{%s}" % t0))
check("cross-engine wrote nothing", [rep(a), rep(a)], [b"2", None])

# 3. interactive MULTI, one command per round trip
out = []
for c_ in (cmd("MULTI"), cmd("INCR", "k{%s}" % t1), cmd("EXEC")):
    a.s.sendall(c_)
    out.append(rep(a))
check("multi interactive", out, [b"OK", b"QUEUED", [b"3"]])

# 4. empty transaction, DISCARD
a.s.sendall(cmd("MULTI") + cmd("EXEC") + cmd("MULTI") + cmd("DISCARD") + cmd("MULTI") + cmd("INCR", "k{%s}" % t1) +
            cmd("DISCARD") + cmd("GET", "k{%s}" % t1))
check("empty exec / discard", [rep(a) for _ in range(8)], [b"OK", [], b"OK", b"OK", b"OK", b"QUEUED", b"OK", b"3"])

# 5. a blocked command holds later commands of its connection
a.s.sendall(cmd("BLPOP", "list{%s}" % t0, "0") + cmd("SET", "flag{%s}" % t1, "1"))
time.sleep(0.3)
b.s.sendall(cmd("GET", "flag{%s}" % t1))
held = rep(b)
check("SET after blocked BLPOP not yet run", held, b"1" if mode == "reply" else None)
b.s.sendall(cmd("LPUSH", "list{%s}" % t0, "x"))
rep(b)
check("BLPOP then SET replies", [rep(a), rep(a)], [[b"list{%s}" % t0.encode(), b"x"], b"OK"])
b.s.sendall(cmd("GET", "flag{%s}" % t1))
check("SET ran after unblock", rep(b), b"1")

# 6. blocked command with timeout releases the hold
a.s.sendall(cmd("BLPOP", "list{%s}" % t0, "0.2") + cmd("INCR", "k{%s}" % t1))
check("BLPOP timeout then INCR", [rep(a), rep(a)], [None, b"4"])

# 7. long mixed pipeline keeps write/read order per key
p = b""
for i in range(200):
    e = tag_for(i % n, n)
    p += cmd("SET", "k{%s}" % e, str(i)) + cmd("GET", "k{%s}" % e)
a.s.sendall(p)
r = [rep(a) for _ in range(400)]
check("mixed pipeline read-your-writes", r[1::2], [str(i).encode() for i in range(200)])

# 8. nothing after QUIT runs (on another engine)
q = Conn(port)
q.s.sendall(cmd("SET", "k{%s}" % t0, "q") + cmd("QUIT") + cmd("SET", "flag{%s}" % t1, "afterquit"))
time.sleep(0.3)
b.s.sendall(cmd("GET", "flag{%s}" % t1))
check("command after QUIT", rep(b), b"afterquit" if mode == "reply" else b"1")

print("fails=%d" % fails)
sys.exit(1 if fails else 0)
