#!/usr/bin/env python3
"""End-to-end benchmark driver for the Rust IO layer.

Server on cores 0-3, valkey-benchmark on cores 4-7 (--threads 4).
Each cell is (config, test, pipeline). A cell is calibrated once (a short
warm-up run), then measured --reps times; the cell row holds the median of
each metric. Every run measures over an exact window (--secs, after --lead s
of load) using server-side counters, because valkey-benchmark only detects
completion on a 250 ms timer, which quantizes its RPS and wall time.

  e2e.py --server src/valkey-server --label base [--profile quick|full]
         [--out cells.csv] [--raw raw.csv] [--reps 3] [--secs 6]
         [--configs c-io2,io-t1] [--tests set,get] [--pipes 1,16]

Configs: c-ioT (C, io-threads T, counts the main thread), and
io-tN / precheck-tN / handoff-tN / rtc-tN (rust-io-threads N, which does not
count the main thread). Compare c-ioT with <rust>-t(T-1).

Metrics per run:
  rps                       total_commands_processed delta / window
  p50_ms, p99_ms, bench_rps valkey-benchmark --csv (whole run; bench_rps is quantized)
  main_cpu_pct              main thread on-CPU time / wall (schedstat, ns)
  main_ns_per_req           main thread on-CPU ns / requests
  io_cpu_pct                sum over IO threads (io_thd_*, rust-io-*)
  proc_cpu_pct              sum over all server threads
  client_cpu_pct            valkey-benchmark on-CPU time / window (schedstat)
  main_cs_per_kreq          main thread context switches per 1000 requests
  instr_per_req, cycles_per_req   perf stat on the main thread, if the PMU
                            is available (blank otherwise)
  foreign_cpu_pct           host busy CPU not used by the server or the client
                            (other tenants of the host; contamination check)
  load1                     host 1-min loadavg before the run
"""
import argparse, csv, io, os, statistics, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
# E2E_SERVER_CPUS / E2E_CLIENT_CPUS / E2E_CLIENT_THREADS override the layout
# (e.g. for configs with more threads than the server's four cores).
SERVER_CPUS = os.environ.get("E2E_SERVER_CPUS", "0-3")
CLIENT_CPUS = os.environ.get("E2E_CLIENT_CPUS", "4-7")
CLIENT_THREADS = os.environ.get("E2E_CLIENT_THREADS", "4")

PROFILES = {
    "quick": ["c-io2", "c-io4", "io-t1", "io-t3", "precheck-t1", "precheck-t3"],
    "full": ["c-io1", "c-io2", "c-io4", "io-t1", "io-t2", "io-t3", "precheck-t1", "precheck-t2", "precheck-t3",
             "handoff-t1", "handoff-t3", "rtc-t1", "rtc-t2", "rtc-t3"],
}
METRICS = ["rps", "p50_ms", "p99_ms", "bench_rps", "main_cpu_pct", "main_ns_per_req", "io_cpu_pct", "proc_cpu_pct",
           "client_cpu_pct", "main_cs_per_kreq", "foreign_cpu_pct", "instr_per_req", "cycles_per_req", "wall_s", "load1"]


def cfg_args(name):
    if name.startswith("c-io"):
        return ["--io-threads", name[4:]]
    mode, t = name.rsplit("-t", 1)
    a = ["--rust-io-takeover", "yes", "--rust-io-threads", t]
    a += {"io": ["--rust-io-mode", "io"], "precheck": ["--rust-io-mode", "precheck"],
          "handoff": ["--rust-io-mode", "exec", "--rust-io-exec-model", "handoff"],
          "rtc": ["--rust-io-mode", "exec", "--rust-io-exec-model", "rtc"]}[mode]
    return a


def schedstat(pid, tid):
    try:
        with open("/proc/%d/task/%d/schedstat" % (pid, tid)) as f:
            return int(f.read().split()[0])
    except FileNotFoundError:
        return 0


def comm(pid, tid):
    with open("/proc/%d/task/%d/comm" % (pid, tid)) as f:
        return f.read().strip()


def ctxsw(pid, tid):
    n = 0
    with open("/proc/%d/task/%d/status" % (pid, tid)) as f:
        for line in f:
            if "ctxt_switches" in line:
                n += int(line.split()[1])
    return n


def snapshot(pid):
    tids = [int(t) for t in os.listdir("/proc/%d/task" % pid)]
    return {t: (schedstat(pid, t), comm(pid, t)) for t in tids}, None


def is_io_thread(name):
    return name.startswith("io_thd") or name.startswith("rust-io-")


class Server:
    def __init__(self, a, name):
        self.a = a
        d = "/tmp/rio-bench-e2e-%d" % a.port
        os.makedirs(d, exist_ok=True)
        self.p = subprocess.Popen(["taskset", "-c", SERVER_CPUS, a.server, "--port", str(a.port), "--save", "",
                                   "--appendonly", "no", "--dir", d, "--logfile", d + "/server.log"]
                                  + cfg_args(name) + a.extra.split())
        for _ in range(100):
            if self.cli("PING").strip() == "PONG":
                return
            time.sleep(0.1)
        raise RuntimeError("server did not start: %s" % name)

    def cli(self, *args):
        return subprocess.run(["taskset", "-c", CLIENT_CPUS, self.a.cli, "-p", str(self.a.port)] + list(args),
                              capture_output=True, text=True).stdout

    def stop(self):
        self.cli("SHUTDOWN", "NOSAVE")
        self.p.wait()


PERF_OK = None


def perf_ok():
    global PERF_OK
    if PERF_OK is None:
        r = subprocess.run(["sudo", "-n", "perf", "stat", "-x,", "-e", "instructions", "--", "true"],
                           capture_output=True, text=True)
        PERF_OK = r.returncode == 0 and "not supported" not in r.stderr and "not counted" not in r.stderr
    return PERF_OK


def commands_processed(srv):
    for line in srv.cli("INFO", "stats").splitlines():
        if line.startswith("total_commands_processed:"):
            return int(line.split(":")[1])
    raise RuntimeError("no total_commands_processed")


def host_busy_ns():
    """Busy time of all CPUs (from /proc/stat), in ns."""
    tot = 0
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu") and line[3].isdigit():
                v = [int(x) for x in line.split()[1:]]
                tot += sum(v) - v[3] - v[4]
    return tot * 1e9 / os.sysconf("SC_CLK_TCK")


def tasks_ns(pid):
    """utime+stime of a process including exited threads, in ns."""
    try:
        with open("/proc/%d/stat" % pid) as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) * 1e9 / os.sysconf("SC_CLK_TCK")
    except FileNotFoundError:
        return 0


def run_once(a, srv, test, pipe, est_rps, use_perf=True):
    """One run. valkey-benchmark gets enough requests for lead + window + 1 s;
    every throughput and CPU metric comes from an exact inner window of
    --secs seconds (server-side command count, schedstat), so valkey-benchmark's
    250 ms completion quantization does not enter. p50/p99 are valkey-benchmark's
    over the whole run."""
    pid = srv.p.pid
    n = int(est_rps * (a.lead + a.secs) * 1.15) // 1000 * 1000
    cmd = ["taskset", "-c", CLIENT_CPUS, a.bench, "-p", str(a.port), "-t", test, "-P", str(pipe), "-c",
           str(a.clients), "-n", str(n), "-d", str(a.datasize), "-r", str(a.keyspace), "--threads", CLIENT_THREADS, "--csv"]
    with open("/proc/loadavg") as f:
        load1 = float(f.read().split()[0])
    bp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    time.sleep(a.lead)
    c0, s0, cs0, k0, h0 = commands_processed(srv), snapshot(pid), ctxsw(pid, pid), tasks_ns(bp.pid), host_busy_ns()
    t0 = time.time()
    perf = None
    if use_perf and a.perf and perf_ok():
        perf = subprocess.Popen(["sudo", "-n", "perf", "stat", "-x,", "-e", "instructions,cycles", "-t", str(pid),
                                 "--", "sleep", str(a.secs)], stderr=subprocess.PIPE, text=True)
        err = perf.communicate()[1]
    else:
        time.sleep(a.secs)
    c1, s1, cs1, k1, h1 = commands_processed(srv), snapshot(pid), ctxsw(pid, pid), tasks_ns(bp.pid), host_busy_ns()
    t1 = time.time()
    short = bp.poll() is not None  # load ended inside the window: invalid
    out = bp.communicate()[0]
    done = bp.returncode == 0
    if short:
        return None
    wall = t1 - t0
    reqs = max(c1 - c0 - 1, 1)  # the INFO call itself is one command
    instr = cycles = ""
    if perf:
        for line in err.splitlines():
            f = line.split(",")
            if len(f) > 2 and f[0].isdigit():
                if f[2].startswith("instructions"):
                    instr = round(int(f[0]) / reqs, 1)
                elif f[2].startswith("cycles"):
                    cycles = round(int(f[0]) / reqs, 1)
    rows = list(csv.reader(io.StringIO(out)))
    r = dict(zip(rows[0], rows[1])) if done and len(rows) > 1 else {}
    s0, s1 = s0[0], s1[0]

    def d(pred):
        return sum(s1[t][0] - s0.get(t, (0,))[0] for t in s1 if pred(t, s1[t][1]))

    main_ns = d(lambda t, c: t == pid)
    io_ns = d(lambda t, c: is_io_thread(c))
    all_ns = d(lambda t, c: True)
    return {"rps": round(reqs / wall, 1), "p50_ms": float(r.get("p50_latency_ms", "nan")),
            "p99_ms": float(r.get("p99_latency_ms", "nan")), "bench_rps": float(r.get("rps", "nan")),
            "main_cpu_pct": round(100.0 * main_ns / 1e9 / wall, 1), "main_ns_per_req": round(main_ns / reqs, 1),
            "io_cpu_pct": round(100.0 * io_ns / 1e9 / wall, 1), "proc_cpu_pct": round(100.0 * all_ns / 1e9 / wall, 1),
            "client_cpu_pct": round(100.0 * (k1 - k0) / 1e9 / wall, 1),
            "main_cs_per_kreq": round(1000.0 * (cs1 - cs0) / reqs, 3),
            "foreign_cpu_pct": round(100.0 * ((h1 - h0) - all_ns - (k1 - k0)) / 1e9 / wall, 1),
            "instr_per_req": instr, "cycles_per_req": cycles, "wall_s": round(wall, 3), "load1": load1, "n": n}


def calibrate(a, srv, test, pipe):
    """Warm-up (also fills keys for GET): run open-ended load and return the
    server-side rate over 1 s after 1 s of load."""
    cmd = ["taskset", "-c", CLIENT_CPUS, a.bench, "-p", str(a.port), "-t", test, "-P", str(pipe), "-c",
           str(a.clients), "-n", "1000000000", "-d", str(a.datasize), "-r", str(a.keyspace), "--threads", CLIENT_THREADS,
           "-q"]
    bp = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    c0, t0 = commands_processed(srv), time.time()
    time.sleep(1.0)
    c1, t1 = commands_processed(srv), time.time()
    bp.kill()
    bp.wait()
    time.sleep(0.2)
    return (c1 - c0) / (t1 - t0)


def git_sha(a):
    if a.sha:
        return a.sha
    v = subprocess.run([a.server, "--version"], capture_output=True, text=True).stdout
    for tok in v.split():
        if tok.startswith("sha="):
            return tok[4:]
    return "unknown"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", default=os.path.join(SRC, "valkey-server"))
    ap.add_argument("--bench", default=os.path.join(SRC, "valkey-benchmark"))
    ap.add_argument("--cli", default=os.path.join(SRC, "valkey-cli"))
    ap.add_argument("--profile", default="quick", choices=sorted(PROFILES))
    ap.add_argument("--configs", help="comma list, overrides the profile's configs")
    ap.add_argument("--tests", default="set,get")
    ap.add_argument("--pipes", default="1,16")
    ap.add_argument("--clients", type=int, default=200)
    ap.add_argument("--datasize", type=int, default=64)
    ap.add_argument("--keyspace", type=int, default=1000000, help="valkey-benchmark -r (use a small one for large values)")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--secs", type=float, default=5.0, help="measurement window per run")
    ap.add_argument("--lead", type=float, default=1.0, help="seconds of load before the window opens")
    ap.add_argument("--label", required=True)
    ap.add_argument("--sha")
    ap.add_argument("--out", default="e2e.csv", help="one row per cell (medians), appended")
    ap.add_argument("--raw", help="one row per run, appended (default: <out>.raw.csv)")
    ap.add_argument("--port", type=int, default=7911)
    ap.add_argument("--extra", default="", help="extra server arguments")
    ap.add_argument("--no-perf", dest="perf", action="store_false")
    ap.add_argument("--retry-spread", type=float, default=5.0,
                    help="repeat a cell once when its reps' RPS spread exceeds this %%; keep the tighter set")
    ap.add_argument("--timing-dir", help="RUST_IO_TIMING builds: CONFIG RESETSTAT before each cell and save "
                                         "INFO rust_io_timing after it to <dir>/<config>-<test>-P<pipe>.txt")
    a = ap.parse_args()
    a.raw = a.raw or a.out.rsplit(".csv", 1)[0] + ".raw.csv"
    sha = git_sha(a)
    configs = a.configs.split(",") if a.configs else PROFILES[a.profile]
    tests, pipes = a.tests.split(","), [int(p) for p in a.pipes.split(",")]
    key = ["sha", "label", "profile", "config", "test", "pipeline", "clients"]

    def writer(path, cols):
        new = not os.path.exists(path)
        f = open(path, "a", newline="")
        w = csv.writer(f)
        if new:
            w.writerow(cols)
        return f, w

    fo, wo = writer(a.out, key + ["reps", "n"] + METRICS + ["rps_min", "rps_max", "rps_spread_pct"])
    fr, wr = writer(a.raw, key + ["rep", "n"] + METRICS + ["attempt"])
    for name in configs:
        srv = Server(a, name)
        try:
            if tests[0] == "get":
                # GET cells normally follow SET cells, which create the keys.
                # Without one, fill the keyspace first so GETs hit.
                subprocess.run(["taskset", "-c", CLIENT_CPUS, a.bench, "-p", str(a.port), "-t", "set", "-P", "16",
                                "-c", str(a.clients), "-n", str(min(1000000, 3 * a.keyspace)), "-d", str(a.datasize),
                                "-r", str(a.keyspace),
                                "--threads", CLIENT_THREADS, "-q"], capture_output=True)
            for pipe in pipes:
                for test in tests:
                    # Warm-up and calibration (also fills keys for GET).
                    est = calibrate(a, srv, test, pipe)
                    if a.timing_dir:
                        srv.cli("CONFIG", "RESETSTAT")
                    for attempt in (1, 2):
                      runs = []
                      for rep in range(1, a.reps + 1):
                        m = None
                        while m is None:
                            m = run_once(a, srv, test, pipe, est)
                            if m is None:
                                est *= 2
                                print("  window outlasted the load; retrying with 2x requests", flush=True)
                        n = m["n"]
                        runs.append(m)
                        k = [sha, a.label, a.profile, name, test, pipe, a.clients]
                        wr.writerow(k + [rep, n] + [m[x] for x in METRICS] + [attempt]); fr.flush()
                      rps = [r["rps"] for r in runs]
                      if attempt == 1 and (max(rps) - min(rps)) > a.retry_spread / 100.0 * statistics.median(rps):
                          print("  %s %s P%d: rep RPS spread above %.0f%%; repeating the cell once" %
                                (name, test, pipe, a.retry_spread), flush=True)
                          first = runs
                          continue
                      if attempt == 2:
                          r1 = [r["rps"] for r in first]
                          if (max(r1) - min(r1)) / statistics.median(r1) < (max(rps) - min(rps)) / statistics.median(rps):
                              runs = first
                      break
                    if a.timing_dir:
                        os.makedirs(a.timing_dir, exist_ok=True)
                        with open(os.path.join(a.timing_dir, "%s-%s-P%d.txt" % (name, test, pipe)), "w") as tf:
                            tf.write(srv.cli("INFO", "rust_io_timing"))
                    med = {}
                    for x in METRICS:
                        vals = [r[x] for r in runs if r[x] != ""]
                        med[x] = round(statistics.median(vals), 3) if vals else ""
                    rps = [r["rps"] for r in runs]
                    row = [sha, a.label, a.profile, name, test, pipe, a.clients, a.reps, n] + \
                          [med[x] for x in METRICS] + \
                          [min(rps), max(rps), round(100.0 * (max(rps) - min(rps)) / med["rps"], 2)]
                    wo.writerow(row); fo.flush()
                    print("%-12s %-3s P%-2d rps=%9.0f p50=%.3f p99=%.3f main=%5.1f%% %6.0f ns/req io=%5.1f%% "
                          "proc=%5.1f%% client=%5.1f%% spread=%.1f%% foreign=%.0f%%" %
                          (name, test, pipe, med["rps"], med["p50_ms"], med["p99_ms"], med["main_cpu_pct"],
                           med["main_ns_per_req"], med["io_cpu_pct"], med["proc_cpu_pct"], med["client_cpu_pct"],
                           row[-1], med["foreign_cpu_pct"]), flush=True)
        finally:
            srv.stop()


if __name__ == "__main__":
    main()
