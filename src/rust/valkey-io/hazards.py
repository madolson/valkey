#!/usr/bin/env python3
"""Hazard tests for rust-io precheck mode.

Usage: hazards.py <valkey-server binary> [server args, default: --rust-io-mode precheck]
e.g. hazards.py src/valkey-server --rust-io-mode exec --rust-io-exec-model rtc --rust-io-threads 2

Every scenario forces the ordering it needs: a command is parsed and
prechecked by the Rust IO thread while the connection is blocked in BLPOP,
the state it was prechecked against is then changed from a second
connection, and the client is unblocked. The queued command must see the new
state. INFO rust_io counter deltas show which path each command took.
"""
import os, socket, subprocess, sys, time, shutil

BIN = sys.argv[1]
MODE = sys.argv[2:] or ["--rust-io-mode", "precheck"]
FAILS = 0


class Conn:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.settimeout(10)
        self.buf = b""

    def send(self, *cmds):
        out = b""
        for c in cmds:
            out += b"*%d\r\n" % len(c)
            for a in c:
                a = a if isinstance(a, bytes) else str(a).encode()
                out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)  # one write: one pipeline

    def _line(self):
        while b"\r\n" not in self.buf:
            d = self.s.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        l, self.buf = self.buf.split(b"\r\n", 1)
        return l

    def read(self):
        l = self._line()
        t, r = l[:1], l[1:]
        if t in (b"+", b"-", b":"):
            return (t + r).decode()
        if t == b"$":
            n = int(r)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(65536)
            v, self.buf = self.buf[:n], self.buf[n + 2:]
            return v.decode()
        if t in (b"*", b"%", b"~", b">"):
            n = int(r)
            if n < 0:
                return None
            if t == b"%":
                n *= 2
            return [self.read() for _ in range(n)]
        if t == b"_":
            return None
        return (t + r).decode()

    def call(self, *a):
        self.send(a)
        return self.read()

    def pending(self, timeout=0.3):
        self.s.settimeout(timeout)
        try:
            d = self.s.recv(65536)
            self.buf += d
            return len(self.buf) > 0
        except socket.timeout:
            return len(self.buf) > 0
        finally:
            self.s.settimeout(10)


def info(c):
    raw = c.call("INFO", "rust_io")
    d = {}
    for l in raw.split("\r\n"):
        if ":" in l and not l.startswith("#"):
            k, v = l.split(":", 1)
            if v.isdigit():
                d[k] = int(v)
    return d


def delta(a, b):
    return {k.replace("precheck_", ""): b[k] - a.get(k, 0) for k in b if b[k] != a.get(k, 0) and k.startswith("precheck_")}


def check(name, got, want):
    global FAILS
    ok = got == want
    if not ok:
        FAILS += 1
    print(f"  {'OK  ' if ok else 'FAIL'} {name}: got {got!r}" + ("" if ok else f" want {want!r}"))


def start(port, *extra):
    d = f"/tmp/rio-hazard-{port}"
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    args = ["taskset", "-c", "0-7", BIN, "--port", str(port)] + MODE + ["--rust-io-takeover", "yes",
            "--save", "", "--appendonly", "no", "--dir", d, "--enable-debug-command", "yes"] + list(extra)
    p = subprocess.Popen(args, stdout=open(d + "/log", "w"), stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port)).close()
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def client_id(c):
    return c.call("CLIENT", "ID")[1:]


def unblock(admin, victim_id):
    admin.call("CLIENT", "UNBLOCK", victim_id)


def scenario(title):
    print("\n== " + title)


# ---------------------------------------------------------------------------
P = 7301
srv = start(P, "--requirepass", "pw")
try:
    admin = Conn(P)
    check("admin auth", admin.call("AUTH", "pw"), "+OK")
    admin.call("SET", "k", "v")

    scenario("1. pipelined AUTH then GET on an unauthenticated connection")
    c = Conn(P)
    time.sleep(0.05)
    i0 = info(admin)
    c.send(("GET", "k"), ("AUTH", "pw"), ("GET", "k"), ("SET", "k2", "x"))
    r = [c.read() for _ in range(4)]
    i1 = info(admin)
    check("replies", r, ["-NOAUTH Authentication required.", "+OK", "v", "+OK"])
    print("  counters:", delta(i0, i1))
    c2 = Conn(P)
    time.sleep(0.05)
    c2.send(("AUTH", "wrong"), ("GET", "k"))
    r = [c2.read() for _ in range(2)]
    check("wrong password then GET", [r[0][:10], r[1]], ["-WRONGPASS", "-NOAUTH Authentication required."])

    scenario("2. ACL SETUSER revokes a permission while a prechecked GET waits")
    admin.call("ACL", "SETUSER", "u1", "on", ">p1", "~*", "+@all")
    a = Conn(P)
    check("auth u1", a.call("AUTH", "u1", "p1"), "+OK")
    aid = client_id(a)
    a.call("GET", "k")
    time.sleep(0.05)
    i0 = info(admin)
    a.send(("BLPOP", "blk", "0"), ("GET", "k"))  # GET prechecked FAST, queued
    time.sleep(0.2)
    admin.call("ACL", "SETUSER", "u1", "-get")
    unblock(admin, aid)
    r = [a.read() for _ in range(2)]
    i1 = info(admin)
    check("BLPOP unblocked", r[0], None)
    check("queued GET denied", r[1][:7], "-NOPERM")
    print("  reply:", r[1])
    print("  counters:", delta(i0, i1))
    log = admin.call("ACL", "LOG", "1")
    check("ACL LOG entry written on main thread", [log[0][i + 1] for i in range(0, len(log[0]), 2) if log[0][i] in ("reason", "object", "username")],
          ["command", "get", "u1"])
    # Pipelined in one connection: ACL is a barrier.
    admin.call("ACL", "SETUSER", "u1", "+get")
    i0 = info(admin)
    a.send(("GET", "k"), ("ACL", "SETUSER", "u1", "-get"), ("GET", "k"), ("ACL", "SETUSER", "u1", "+get"), ("GET", "k"))
    r = [a.read() for _ in range(5)]
    i1 = info(admin)
    check("same-connection pipeline", [r[0], r[1], r[2][:7], r[3], r[4]], ["v", "+OK", "-NOPERM", "+OK", "v"])
    print("  counters:", delta(i0, i1))
    # Key-pattern user: allowed and denied keys in one pipeline (predicted rejection keeps its position).
    admin.call("ACL", "SETUSER", "u2", "on", ">p2", "~app:*", "+@all")
    b = Conn(P)
    b.call("AUTH", "u2", "p2")
    b.call("PING")
    i0 = info(admin)
    b.send(("SET", "app:1", "a"), ("GET", "other"), ("GET", "app:1"), ("NOSUCHCMD",), ("GET", "app:1"))
    r = [b.read() for _ in range(5)]
    i1 = info(admin)
    check("key-pattern pipeline order", [r[0], r[1][:7], r[2], r[3][:24], r[4]],
          ["+OK", "-NOPERM", "a", "-ERR unknown command 'NO", "a"])
    print("  counters:", delta(i0, i1))

    scenario("3. CLIENT PAUSE WRITE starts while a prechecked SET waits")
    w = Conn(P)
    w.call("AUTH", "pw")
    wid = client_id(w)
    w.call("PING")
    time.sleep(0.05)
    i0 = info(admin)
    w.send(("BLPOP", "blk", "0"), ("SET", "paused", "1"), ("GET", "k"))
    time.sleep(0.2)
    admin.call("CLIENT", "PAUSE", "1500", "WRITE")
    t0 = time.time()
    unblock(admin, wid)
    check("BLPOP unblocked", w.read(), None)
    check("SET held while paused", w.pending(0.5), False)
    check("key not written during pause", admin.call("EXISTS", "paused"), ":0")
    r = [w.read(), w.read()]
    dt = time.time() - t0
    i1 = info(admin)
    check("SET then GET after pause", r, ["+OK", "v"])
    check("SET waited for the pause (>1.2s)", dt > 1.2, True)
    print(f"  waited {dt:.2f}s; counters:", delta(i0, i1))

    scenario("5. MULTI, commands, EXEC pipelined")
    m = Conn(P)
    m.call("AUTH", "pw")
    m.call("DEL", "ma")
    i0 = info(admin)
    m.send(("MULTI",), ("SET", "ma", "1"), ("INCR", "ma"), ("EXEC",), ("GET", "ma"))
    r = [m.read() for _ in range(5)]
    i1 = info(admin)
    check("replies", r, ["+OK", "+QUEUED", "+QUEUED", ["+OK", ":2"], "2"])
    print("  counters:", delta(i0, i1))
    m.send(("MULTI",), ("SET", "ma", "1"), ("BADCMD",), ("EXEC",))
    r = [m.read() for _ in range(4)]
    check("EXECABORT on queued error", [r[0], r[1], r[2][:4], r[3][:10]], ["+OK", "+QUEUED", "-ERR", "-EXECABORT"])
    # MULTI started while a prechecked command waits (session changes under it).
    mid = client_id(m)
    m.send(("BLPOP", "blk", "0"), ("SET", "ma", "5"))
    time.sleep(0.2)
    unblock(admin, mid)
    check("BLPOP", m.read(), None)
    check("SET", m.read(), "+OK")
    m.send(("SELECT", "1"), ("SET", "ma", "db1"), ("SELECT", "0"), ("GET", "ma"))
    r = [m.read() for _ in range(4)]
    check("SELECT pipelined", r, ["+OK", "+OK", "+OK", "5"])
    check("db1 value", admin.call("SELECT", "1") and admin.call("GET", "ma"), "db1")
    admin.call("SELECT", "0")
    print("  final counters:", {k: v for k, v in info(admin).items() if v})
finally:
    srv.kill()
    srv.wait()

# ---------------------------------------------------------------------------
scenario("4. slot migrating / moving while a prechecked command waits (2-node cluster)")
P1, P2 = 7311, 7312
n1 = start(P1, "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf")
n2 = start(P2, "--cluster-enabled", "yes", "--cluster-config-file", "nodes.conf")
try:
    c1, c2 = Conn(P1), Conn(P2)
    c1.call("CLUSTER", "ADDSLOTSRANGE", "0", "16383")
    c1.call("CLUSTER", "MEET", "127.0.0.1", str(P2))
    id1, id2 = c1.call("CLUSTER", "MYID"), c2.call("CLUSTER", "MYID")
    for _ in range(100):
        if "cluster_state:ok" in c1.call("CLUSTER", "INFO") and "cluster_state:ok" in c2.call("CLUSTER", "INFO") \
                and id1 in c2.call("CLUSTER", "NODES"):
            break
        time.sleep(0.1)
    slot = int(c1.call("CLUSTER", "KEYSLOT", "{t}")[1:])
    c1.call("SET", "{t}present", "here")
    v = Conn(P1)
    vid = client_id(v)
    v.call("GET", "{t}present")
    time.sleep(0.05)
    i0 = info(c1)
    # Prechecked against "slot owned, stable": GET present, GET missing.
    v.send(("BLPOP", "{t}blk", "0"), ("GET", "{t}present"), ("GET", "{t}missing"))
    time.sleep(0.2)
    check("SETSLOT MIGRATING", c1.call("CLUSTER", "SETSLOT", str(slot), "MIGRATING", id2), "+OK")
    unblock(c1, vid)
    r = [v.read() for _ in range(3)]
    i1 = info(c1)
    check("existing key served while migrating", r[1], "here")
    check("missing key gets ASK", r[2], f"-ASK {slot} 127.0.0.1:{P2}")
    print("  counters:", delta(i0, i1))
    # Now the slot moves away entirely.
    check("SETSLOT IMPORTING", c2.call("CLUSTER", "SETSLOT", str(slot), "IMPORTING", id1), "+OK")
    check("MIGRATE", c1.call("MIGRATE", "127.0.0.1", str(P2), "", "0", "5000", "KEYS", "{t}present"), "+OK")
    i0 = info(c1)
    # Block on a key in another, stable slot: {t} is migrating, so a BLPOP on
    # {t}blk would get ASK instead of blocking.
    v.send(("BLPOP", "{x}blk", "0"), ("GET", "{t}present"))
    time.sleep(0.2)
    check("SETSLOT NODE on target", c2.call("CLUSTER", "SETSLOT", str(slot), "NODE", id2), "+OK")
    check("SETSLOT NODE on source", c1.call("CLUSTER", "SETSLOT", str(slot), "NODE", id2), "+OK")
    unblock(c1, vid)
    r = [v.read() for _ in range(2)]
    i1 = info(c1)
    check("BLPOP blocked then unblocked", r[0], None)
    check("queued GET gets MOVED", r[1], f"-MOVED {slot} 127.0.0.1:{P2}")
    print("  counters:", delta(i0, i1))
    # Fast path still taken for an owned slot.
    i0 = info(c1)
    v.send(*[("SET", "{x}k", str(j)) for j in range(100)])
    r = [v.read() for _ in range(100)]
    i1 = info(c1)
    check("owned-slot pipeline", r.count("+OK"), 100)
    print("  counters:", delta(i0, i1))
finally:
    for p in (n1, n2):
        p.kill()
        p.wait()

print(f"\nFAILURES: {FAILS}")
sys.exit(1 if FAILS else 0)
