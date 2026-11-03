#!/usr/bin/env python3
"""Input backpressure: a client blocked in BLPOP keeps pipelining PINGs.
Rust must stop reading, so the client's non-blocking send hits EAGAIN after
roughly socket buffers + ~2048 queued commands, not after the whole 64 MB."""
import socket, time, subprocess, sys
RP, CP = int(sys.argv[1]), int(sys.argv[2])
s = socket.create_connection(("127.0.0.1", RP))
s.sendall(b"*3\r\n$5\r\nBLPOP\r\n$2\r\nbp\r\n$1\r\n0\r\n")
time.sleep(0.2)
ping = b"*1\r\n$4\r\nPING\r\n"
chunk = ping * 4096
s.setblocking(False)
sent, deadline, pending = 0, time.time() + 3, b""
while sent < 64 << 20 and time.time() < deadline:
    if not pending:
        pending = chunk
    try:
        n = s.send(pending)
        sent += n
        pending = pending[n:]
    except BlockingIOError:
        time.sleep(0.01)
accepted = sent
c = socket.create_connection(("127.0.0.1", CP))
c.sendall(b"RPUSH bp x\r\n"); c.recv(100)
# Finish the partial frame so the stream stays valid.
s.setblocking(True)
s.sendall(pending[:(len(ping) - sent % len(ping)) % len(ping)])
sent += (len(ping) - sent % len(ping)) % len(ping)
print("bytes accepted while blocked: %d (%d PINGs) of %d attempted" % (accepted, accepted // len(ping), 64 << 20))
# Reading the replies drains everything; count PONGs.
want, got, buf = sent // len(ping), 0, b""
s.settimeout(10)
while got < want:
    buf += s.recv(1 << 20)
    got = buf.count(b"+PONG\r\n")
print("BLPOP reply then %d/%d PONGs after unblock" % (got, want))
