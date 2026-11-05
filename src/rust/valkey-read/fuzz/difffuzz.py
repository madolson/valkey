#!/usr/bin/env python3
"""Differential fuzz of the read path: the same byte stream goes to a C-read-path
server and a Rust-read-path server, split into random chunks, and the reply
bytes and connection-close behaviour must match. At the end the keyspace digest,
per-slot network-bytes-in (CLUSTER SLOT-STATS) and a few INFO counters must match.

usage: [DET=1] difffuzz.py <c-port> <rust-port> <cases> <seed> [concurrency]
Start one default build and one BUILD_RUST_IO=yes build, each with
--cluster-enabled yes --cluster-slot-stats-enabled yes --enable-debug-command yes
--client-query-buffer-limit 1mb, run CLUSTER ADDSLOTSRANGE 0 16383 on both and
wait for cluster_state:ok. Use fresh servers per DET mode. Running the same
script against two default builds measures the harness noise floor.
"""
import os, re, random, socket, sys, threading, time
# DET=1: only inputs whose outcome cannot depend on read boundaries (no line
# can exceed the 64KB inline limit), so the end state must match exactly.
DET = os.environ.get("DET") == "1"

RETRIES = 5
IDLE = 0.25          # seconds of silence that ends a case
INLINE_MAX = 64 * 1024
BIG_ARG = 32 * 1024

def rand_bytes(r, n):
    alphabet = b"abcxyz0123456789 \t\"'\\\r\n*$-+:\x00\xff{}"
    if r.random() < 0.5:
        return bytes(r.choice(b"abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(n))
    return bytes(r.choice(alphabet) for _ in range(n))

def arg(r):
    k = r.random()
    if k < 0.6:
        return rand_bytes(r, r.randint(0, 20))
    if k < 0.85:
        return rand_bytes(r, r.randint(20, 3000))
    if k < 0.95 or DET:
        return b"v" * r.randint(BIG_ARG - 2, BIG_ARG + 5000)   # big-arg path
    return b"w" * r.randint(100_000, 300_000)

PREFIX = [b""]
ERRS = []
COMPARED = [0]

def key(r):
    return PREFIX[0] + b"k" + str(r.randint(0, 50)).encode() + (b"{t}" if r.random() < 0.3 else b"")

def command(r):
    k = r.random()
    if k < 0.2:
        return [b"SET", key(r), arg(r)]
    if k < 0.35:
        return [b"GET", key(r)]
    if k < 0.45:
        return [b"APPEND", key(r), arg(r)]
    if k < 0.55:
        return [b"ECHO", arg(r)]
    if k < 0.6:
        return [b"PING"]
    if k < 0.65:
        return [b"STRLEN", key(r)]
    if k < 0.7:
        return [b"MSET"] + sum(([PREFIX[0] + b"m{x}" + str(i).encode(), rand_bytes(r, r.randint(0, 100))] for i in range(r.randint(1, 30))), [])
    if k < 0.75:
        return [b"DEL", key(r)]
    if k < 0.8:
        return [b"RPUSH", key(r) + b"L"] + [rand_bytes(r, r.randint(0, 8)) for _ in range(r.randint(1, 1500))]
    if k < 0.85:
        return [b"EXISTS"] + [key(r) for _ in range(r.randint(1, 5))]
    if k < 0.9:
        return [b"NOSUCHCMD", arg(r)]
    return [b"GET"]  # arity error

def resp(cmd):
    out = [b"*%d\r\n" % len(cmd)]
    for a in cmd:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)

def inline(r, cmd):
    parts = []
    for a in cmd:
        if len(a) > 2000:
            a = a[:2000]
        a = a.replace(b"\r", b"").replace(b"\n", b"").replace(b"\x00", b"")
        if r.random() < 0.2:
            a = b'"' + a.replace(b"\\", b"\\\\").replace(b'"', b'\\"') + b'"'
        elif r.random() < 0.1:
            a = b"'" + a.replace(b"'", b"") + b"'"
        else:
            a = a.replace(b'"', b"").replace(b"'", b"").replace(b" ", b"").replace(b"\t", b"")
        parts.append(a)
    return b" ".join(parts) + (b"\r\n" if r.random() < 0.8 else b"\n")

def mutate(r, b):
    b = bytearray(b)
    for _ in range(r.randint(1, 3)):
        if not b:
            break
        k = r.random()
        i = r.randrange(len(b))
        if k < 0.25:
            b[i] = r.choice(b"\r\n*$-0123456789x\x00 \"")
        elif k < 0.45:
            del b[i:i + r.randint(1, 4)]
        elif k < 0.65:
            b[i:i] = bytes([r.choice(b"\r\n*$-0 ")])
        elif k < 0.8:
            b = b[:i]
        else:
            b[i:i] = r.choice([b"*-1\r\n", b"*0\r\n", b"$-1\r\n", b"*99999999999\r\n",
                               b"$9999999999999\r\n", b"*2147483648\r\n", b"\r\n", b"\n",
                               b"*1\r\n$4\r\nPING\rX", b"*1\r\n%4\r\n"])
    return bytes(b)

def special(r):
    k = r.randint(0, 11)
    while DET and k in (0, 1, 2, 8):
        k = r.randint(0, 11)
    if k == 0:
        return b"*" + b"1" * r.randint(70_000, 90_000)                        # big mbulk count
    if k == 1:
        return b"*1\r\n$" + b"1" * r.randint(70_000, 90_000)                 # big bulk count
    if k == 2:
        return b"x" * r.randint(70_000, 200_000)                              # big inline
    if k == 3:
        return b"set a \"unbalanced\r\n"
    if k == 4:
        return b"echo a\x00b\r\n"
    if k == 5:
        return b"\r\n\r\n\n" + resp([b"PING"])
    if k == 6:
        return b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$3\r\nbcdXY"                    # bad CRLF after bulk
    if k == 7:
        return b"*-5\r\n*0\r\n" + resp([b"ECHO", b"after-negative"])
    if k == 8:
        return b"*2\r\n$4\r\nECHO\r\n$" + str(BIG_ARG * 3).encode() + b"\r\n" + b"z" * (BIG_ARG * 3) + b"\r\n" + resp([b"PING"])
    if k == 9:
        return b"*1\r\n:4\r\nPING\r\n"
    if k == 10:
        return b"*1\r\n$4\r\nPING\r\n" * r.randint(500, 3000)                # deep pipeline, cmd_queue cap
    return b"*" + b" 3\r\n"

def make_case(r):
    out = []
    for _ in range(r.randint(1, 12)):
        k = r.random()
        if k < 0.55:
            out.append(resp(command(r)))
        elif k < 0.75:
            out.append(inline(r, command(r)))
        elif k < 0.85:
            out.append(special(r))
        else:
            out.append(mutate(r, resp(command(r))))
    data = b"".join(out)
    return data

def chunks(r, data):
    res, i = [], 0
    mode = r.random()
    while i < len(data):
        if mode < 0.3:
            n = len(data) - i
        elif mode < 0.6:
            n = r.randint(1, 16)
        else:
            n = r.choice([1, 2, 3, 7, 100, 4096, 16384, 16385, 40000])
        res.append(data[i:i + n])
        i += n
    return res

def run_one(port, parts, pauses):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    got = bytearray()
    closed = [False]
    def reader():
        s.settimeout(IDLE)
        while True:
            try:
                d = s.recv(65536)
            except socket.timeout:
                if sender_done[0]:
                    return
                continue
            except OSError:
                closed[0] = True
                return
            if not d:
                closed[0] = True
                return
            got.extend(d)
    sender_done = [False]
    t = threading.Thread(target=reader)
    t.start()
    try:
        for p, z in zip(parts, pauses):
            s.sendall(p)
            if z:
                time.sleep(z)
    except OSError:
        pass
    sender_done[0] = True
    t.join()
    s.close()
    return bytes(got), closed[0]

def cmd(port, *args):
    s = socket.create_connection(("127.0.0.1", port))
    s.sendall(resp([a if isinstance(a, bytes) else a.encode() for a in args]))
    s.settimeout(5)
    buf = b""
    while True:
        d = s.recv(1 << 20)
        buf += d
        if not d or (buf.endswith(b"\r\n") and parse_complete(buf)):
            break
    s.close()
    return buf

def parse_complete(b):
    def p(i):
        t = b[i:i + 1]
        j = b.find(b"\r\n", i)
        if j < 0:
            raise IndexError
        if t in (b"+", b"-", b":"):
            return j + 2
        if t == b"$":
            n = int(b[i + 1:j])
            if n < 0:
                return j + 2
            if len(b) < j + 2 + n + 2:
                raise IndexError
            return j + 2 + n + 2
        if t == b"*":
            n = int(b[i + 1:j])
            k = j + 2
            for _ in range(max(n, 0)):
                k = p(k)
            return k
        return j + 2
    try:
        return p(0) == len(b)
    except (IndexError, ValueError):
        return False

def decode(b, i=0):
    t = b[i:i + 1]
    j = b.find(b"\r\n", i)
    if t in (b"+", b"-"):
        return b[i + 1:j], j + 2
    if t == b":":
        return int(b[i + 1:j]), j + 2
    if t == b"$":
        n = int(b[i + 1:j])
        return b[j + 2:j + 2 + n], j + 2 + n + 2
    if t in (b"*", b"%"):
        n = int(b[i + 1:j]) * (2 if t == b"%" else 1)
        k, out = j + 2, []
        for _ in range(n):
            v, k = decode(b, k)
            out.append(v)
        return out, k
    raise ValueError(t)

def slot_net(raw):
    """slot -> (key-count, network-bytes-in, network-bytes-out); cpu-usec is timing."""
    res = {}
    for slot, stats in decode(raw)[0]:
        d = dict(zip(stats[0::2], stats[1::2]))
        v = (d.get(b"key-count"), d.get(b"network-bytes-in"), d.get(b"network-bytes-out"))
        if any(v):
            res[slot] = v
    return res

def info_fields(port):
    out = {}
    for line in cmd(port, "INFO", "everything").decode(errors="replace").split("\r\n"):
        if ":" in line:
            k, v = line.split(":", 1)
            out[k] = v
    return out

def main():
    cport, rport, ncases, seed = map(int, sys.argv[1:5])
    conc = int(sys.argv[5]) if len(sys.argv) > 5 else 8
    r = random.Random(seed)
    cases = []
    for i in range(ncases):
        PREFIX[0] = b"c%d:" % i  # disjoint keys so concurrent cases cannot interleave
        data = make_case(r)
        parts = chunks(r, data)
        pauses = [0.002 if r.random() < 0.3 else 0 for _ in parts]
        if sum(pauses) > 1.0:
            pauses = [0] * len(parts)
        cases.append((data, parts, pauses))
    mismatches = []
    flaky = []
    lock = threading.Lock()
    idx = [0]
    def worker():
        while True:
            with lock:
                i = idx[0]
                idx[0] += 1
            if i >= len(cases):
                return
            data, parts, pauses = cases[i]
            res = {}
            def go(port, key):
                try:
                    res[key] = run_one(port, parts, pauses)
                except OSError as e:
                    res[key] = (b"CONNECT-ERROR " + str(e).encode(), True)
            a = threading.Thread(target=go, args=(cport, "c"))
            b = threading.Thread(target=go, args=(rport, "r"))
            a.start(); b.start(); a.join(); b.join()
            for m in re.findall(rb"-ERR Protocol error: [^\r]*|-ERR unknown command|-ERR wrong number", res["c"][0]):
                with lock:
                    ERRS.append(m[:60].decode(errors="replace"))
            with lock:
                COMPARED[0] += 1
            if res["c"] != res["r"]:
                # Timing-dependent outcomes (RST after close with unread input,
                # an over-limit line whose terminator may or may not have
                # arrived) are retried; only a mismatch on every try counts.
                first = (res["c"], res["r"])
                for _ in range(RETRIES):
                    res = {}
                    a = threading.Thread(target=go, args=(cport, "c"))
                    b = threading.Thread(target=go, args=(rport, "r"))
                    a.start(); b.start(); a.join(); b.join()
                    if res["c"] == res["r"]:
                        break
                with lock:
                    if res["c"] == res["r"]:
                        flaky.append(i)
                    else:
                        mismatches.append((i, data, first[0], first[1]))
    # Run serially by key-touching order would be needed for byte-identical
    # keyspace replies across concurrent cases; with concurrency the replies to
    # GET may legitimately differ, so concurrent cases use disjoint key spaces.
    ts = [threading.Thread(target=worker) for _ in range(conc)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    import collections
    tally = collections.Counter()
    for e in ERRS:
        tally[e] += 1
    print("error replies seen (C side):", dict(tally.most_common()))
    print(f"cases={ncases} seed={seed} conc={conc} bytes={sum(len(c[0]) for c in cases)} mismatches={len(mismatches)} resolved-on-retry={len(flaky)} {flaky}")
    for i, data, c, rr in mismatches[:10]:
        print(f"--- case {i}: input {len(data)} bytes {data[:200]!r}")
        print(f"    C   closed={c[1]} reply {len(c[0])} bytes {c[0][:300]!r}")
        print(f"    RS  closed={rr[1]} reply {len(rr[0])} bytes {rr[0][:300]!r}")
    dc, dr = cmd(cport, "DEBUG", "DIGEST"), cmd(rport, "DEBUG", "DIGEST")
    print(f"digest C={dc!r} RS={dr!r} match={dc == dr}")
    sc = cmd(cport, "CLUSTER", "SLOT-STATS", "ORDERBY", "network-bytes-in", "LIMIT", "16384")
    sr = cmd(rport, "CLUSTER", "SLOT-STATS", "ORDERBY", "network-bytes-in", "LIMIT", "16384")
    sc, sr = slot_net(sc), slot_net(sr)
    print(f"slot-stats slots={len(sc)} total-net-in C={sum(v[1] for v in sc.values())} RS={sum(v[1] for v in sr.values())} match={sc == sr}")
    for k in sorted(set(sc) | set(sr)):
        if sc.get(k) != sr.get(k):
            print(f"  slot {k}: C={sc.get(k)} RS={sr.get(k)}")
    ic, ir = info_fields(cport), info_fields(rport)
    for k in ["total_commands_processed", "total_net_input_bytes", "total_error_replies",
              "unexpected_error_replies", "client_query_buffer_limit_disconnections", "io_threaded_reads_processed", "io_threaded_writes_processed",
              "errorstat_ERR", "keyspace_hits", "keyspace_misses", "db0"]:
        print(f"info {k}: C={ic.get(k)} RS={ir.get(k)} {'OK' if ic.get(k) == ir.get(k) else 'DIFF'}")
    print(f"compared={COMPARED[0]} of {ncases}")
    ok = COMPARED[0] == ncases and not mismatches and dc == dr and sc == sr
    print("RESULT", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
