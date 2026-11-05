#!/usr/bin/env python3
"""Per-connection execution-order test for the dlmopen host.

Producers send `SET job:<id> v` and `LPUSH queue <id>` in one write. The job
keys live on the last engine and the queue on engine 0, so the two commands
go to different instances. Consumers `BRPOP queue` and then `GET job:<id>`;
a nil means the LPUSH executed before the SET from the same connection.

usage: ordertest.py PORT NENGINES SECONDS [PRODUCERS] [CONSUMERS] [WINDOW]
WINDOW = SET/LPUSH pairs per producer write (default 1).
"""
import multiprocessing as mp
import socket
import sys
import time


def crc16(data):
    crc = 0
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else crc << 1
            crc &= 0xFFFF
    return crc


def engine_of(tag, n):
    return (crc16(tag.encode()) & 16383) * n // 16384


def tag_for(engine, n):
    i = 0
    while engine_of("t%d" % i, n) != engine:
        i += 1
    return "t%d" % i


def cmd(*args):
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l

    def reply(self):
        l = self._line()
        t = l[:1]
        if t == b"$":
            n = int(l[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(65536)
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v
        if t == b"*":
            n = int(l[1:])
            return None if n < 0 else [self.reply() for _ in range(n)]
        if t == b"-":
            raise RuntimeError(l.decode())
        return l[1:]


def producer(port, n, secs, pid, window, out):
    jt, qt = tag_for(n - 1, n), tag_for(0, n)
    c = Conn(port)
    i = 0
    end = time.time() + secs
    while time.time() < end:
        buf = []
        for _ in range(window):
            jid = "%d:%d" % (pid, i)
            buf.append(cmd("SET", "job:{%s}:%s" % (jt, jid), "v" * 32))
            buf.append(cmd("LPUSH", "queue{%s}" % qt, jid))
            i += 1
        c.s.sendall(b"".join(buf))
        for _ in range(2 * window):
            c.reply()
    out.put(("p", i))


def consumer(port, n, secs, out):
    jt, qt = tag_for(n - 1, n), tag_for(0, n)
    c = Conn(port)
    got = nil = 0
    end = time.time() + secs + 2
    while time.time() < end:
        c.s.sendall(cmd("BRPOP", "queue{%s}" % qt, "0.2"))
        r = c.reply()
        if r is None:
            if time.time() > end - 2:
                break
            continue
        jid = r[1].decode()
        c.s.sendall(cmd("GET", "job:{%s}:%s" % (jt, jid)))
        if c.reply() is None:
            nil += 1
        got += 1
    out.put(("c", got, nil))


def main():
    port, n, secs = int(sys.argv[1]), int(sys.argv[2]), float(sys.argv[3])
    np_ = int(sys.argv[4]) if len(sys.argv) > 4 else 8
    nc = int(sys.argv[5]) if len(sys.argv) > 5 else 8
    window = int(sys.argv[6]) if len(sys.argv) > 6 else 1
    print("job tag %s -> engine %d, queue tag %s -> engine %d" %
          (tag_for(n - 1, n), n - 1, tag_for(0, n), 0))
    out = mp.Queue()
    ps = [mp.Process(target=producer, args=(port, n, secs, i, window, out)) for i in range(np_)]
    ps += [mp.Process(target=consumer, args=(port, n, secs, out)) for _ in range(nc)]
    for p in ps:
        p.start()
    res = [out.get() for _ in ps]
    for p in ps:
        p.join()
    produced = sum(r[1] for r in res if r[0] == "p")
    got = sum(r[1] for r in res if r[0] == "c")
    nil = sum(r[2] for r in res if r[0] == "c")
    print("producers=%d consumers=%d window=%d secs=%g produced=%d consumed=%d nil_gets=%d rate=%.2e" %
          (np_, nc, window, secs, produced, got, nil, nil / got if got else 0))
    sys.exit(1 if nil else 0)


if __name__ == "__main__":
    main()
