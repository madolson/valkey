#!/usr/bin/env python3
"""Smoke test for the Rust IO port. Usage: smoke.py <rust_port> <c_port>"""
import socket, sys, time, threading

RP, CP = int(sys.argv[1]), int(sys.argv[2])


def enc(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.buf = b""

    def send(self, data):
        self.s.sendall(data)

    def _fill(self):
        d = self.s.recv(1 << 20)
        if not d:
            raise EOFError
        self.buf += d

    def _line(self):
        while b"\r\n" not in self.buf:
            self._fill()
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l

    def reply(self):
        l = self._line()
        t, rest = l[:1], l[1:]
        if t in (b"+", b"-", b":"):
            return l.decode()
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self._fill()
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.reply() for _ in range(n)]
        raise ValueError(l)

    def cmd(self, *args):
        self.send(enc(*args))
        return self.reply()

    def closed(self, timeout=2):
        self.s.settimeout(timeout)
        try:
            while True:
                d = self.s.recv(4096)
                if not d:
                    return True
                self.buf += d
        except (ConnectionResetError, BrokenPipeError):
            return True
        except socket.timeout:
            return False


def check(name, ok, detail=""):
    print("%-50s %s %s" % (name, "OK" if ok else "FAIL", detail))
    if not ok:
        global failures
        failures += 1


failures = 0
r = Conn(RP)
c = Conn(CP)

r.cmd("FLUSHALL")
check("PING", r.cmd("PING") == "+PONG")
check("SET/GET", r.cmd("SET", "k1", "v1") == "+OK" and r.cmd("GET", "k1") == b"v1")
check("inline PING", (r.send(b"PING\r\n"), r.reply())[1] == "+PONG")

# Pipelined batch of 1000 in one write, replies must be in order.
r.send(b"".join(enc("INCR", "ctr") for _ in range(1000)))
got = [r.reply() for _ in range(1000)]
check("pipeline 1000 INCR ordered", got == [":%d" % i for i in range(1, 1001)], "last=%s" % got[-1])

r.send(b"".join(enc("SET", "p%d" % i, "v%d" % i) for i in range(1000)) +
       b"".join(enc("GET", "p%d" % i) for i in range(1000)))
got = [r.reply() for _ in range(2000)]
check("pipeline 1000 SET + 1000 GET", got[:1000] == ["+OK"] * 1000 and
      got[1000:] == [b"v%d" % i for i in range(1000)])

# MULTI/EXEC
r.send(enc("MULTI") + enc("SET", "m", "1") + enc("INCR", "m") + enc("EXEC"))
got = [r.reply() for _ in range(4)]
check("MULTI/EXEC", got == ["+OK", "+QUEUED", "+QUEUED", ["+OK", ":2"]], str(got))

# BLPOP on the Rust port, unblocked by LPUSH from the C port. Pipeline a
# PING behind the BLPOP to check it waits until the client unblocks.
r.cmd("DEL", "bl")
r.send(enc("BLPOP", "bl", "0") + enc("PING"))
time.sleep(0.3)
r.s.setblocking(False)
try:
    early = r.s.recv(100)
except BlockingIOError:
    early = b""
r.s.setblocking(True)
check("BLPOP blocks (no early reply)", early == b"", repr(early))
check("LPUSH from C port", c.cmd("LPUSH", "bl", "x") == ":1")
check("BLPOP unblocked by C port", r.reply() == [b"bl", b"x"])
check("pipelined PING after BLPOP", r.reply() == "+PONG")

# SUBSCRIBE on Rust, PUBLISH from C and the other way round.
sub = Conn(RP)
check("SUBSCRIBE (rust)", sub.cmd("SUBSCRIBE", "ch") == [b"subscribe", b"ch", ":1"])
check("PUBLISH (C) -> rust subscriber", c.cmd("PUBLISH", "ch", "hello") == ":1")
check("rust subscriber got message", sub.reply() == [b"message", b"ch", b"hello"])
csub = Conn(CP)
csub.cmd("SUBSCRIBE", "ch2")
check("PUBLISH (rust) -> C subscriber", r.cmd("PUBLISH", "ch2", "hi") == ":1")
check("C subscriber got message", csub.reply() == [b"message", b"ch2", b"hi"])

# CLIENT LIST shows the Rust client; CLIENT KILL closes it.
victim = Conn(RP)
victim.cmd("CLIENT", "SETNAME", "victim")
vid = victim.cmd("CLIENT", "ID")
cl = c.cmd("CLIENT", "LIST").decode()
line = [l for l in cl.splitlines() if "name=victim" in l]
print("  CLIENT LIST line:", line[0] if line else None)
check("CLIENT LIST shows rust client", len(line) == 1)
check("CLIENT KILL ID", c.cmd("CLIENT", "KILL", "ID", vid[1:]) == ":1")
check("killed client socket closed", victim.closed())

# QUIT
q = Conn(RP)
check("QUIT replies OK", q.cmd("QUIT") == "+OK")
check("QUIT closes socket", q.closed())

# 10 MB SET/GET
big = b"x" * (10 * 1024 * 1024 - 7) + b"END1234"
check("10MB SET", r.cmd("SET", "big", big) == "+OK")
got = r.cmd("GET", "big")
check("10MB GET", got == big, "len=%d" % (len(got) if got else -1))
check("STRLEN big (from C port)", c.cmd("STRLEN", "big") == ":%d" % len(big))

# Malformed RESP: protocol error then close; prior pipelined command answered.
m = Conn(RP)
m.send(enc("PING") + b"*1\r\nfoo\r\n")
check("cmd before bad frame answered", m.reply() == "+PONG")
err = m.reply()
check("malformed RESP -> protocol error", err == "-ERR Protocol error: expected '$', got 'f'", err)
check("malformed RESP -> connection closed", m.closed())
m = Conn(RP)
m.send(b"*abc\r\n")
err = m.reply()
check("bad multibulk len", err == "-ERR Protocol error: invalid multibulk length", err)
check("bad multibulk len -> closed", m.closed())

# Peer close reaches the main thread and frees the client.
p = Conn(RP)
p.cmd("CLIENT", "SETNAME", "peerclose")
p.s.close()
time.sleep(0.3)
check("peer close frees client", "name=peerclose" not in c.cmd("CLIENT", "LIST").decode())

print("FAILURES:", failures)
sys.exit(1 if failures else 0)
