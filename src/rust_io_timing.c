/* Main-thread half of the sampled stage timing (RUST_IO_TIMING).
 *
 * The IO thread tags one parsed command in N with a trace id (VioCmd.trace).
 * The main thread keeps sampled commands in three places, so an unsampled
 * command costs one compare per hook:
 *   queued   dequeued from Rust, matched by argv pointer at processCommand();
 *   current  the one sampled command executing (call() entry and exit);
 *   replied  waiting for its reply copy into Rust (connRustWritev), by conn.
 * The Rust IO thread stamps the last stage when the bytes hit the socket. */
#include "server.h"
#include "rust_io_timing.h"

#if defined(USE_RUST_IO) && defined(RUST_IO_TIMING)
#include "rust/valkey-io/include/valkey_io.h"

#define RIT_MAX 256

typedef struct {
    uint32_t id;
    void **argv;
    connection *conn; /* compared, never dereferenced */
    uint64_t seq;
} ritEntry;

int rit_nq = 0, rit_nr = 0;
uint64_t rit_fa_ticks = 0, rit_fa_n = 0;
static uint64_t rit_fa_tsc0 = 0, rit_fa_ns0 = 0;

uint64_t ritTicks(void) {
    return __builtin_ia32_rdtsc();
}

static uint64_t ritMonoNs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}
client *rit_cur_c = NULL;
static ritEntry rit_q[RIT_MAX], rit_r[RIT_MAX], rit_cur;
static int rit_cur_depth = -1; /* server.in_call at call() entry; -1 before */
static uint64_t rit_seq = 0;
static unsigned long long rit_evicted = 0;

/* Append to a list; when full, evict the oldest (a trace that never got a
 * reply written: closed connection, no reply). Lists are in seq order. */
static void ritPush(ritEntry *list, int *n, ritEntry e) {
    if (*n == RIT_MAX) {
        vio_timing_drop(list[0].id);
        rit_evicted++;
        memmove(list, list + 1, (RIT_MAX - 1) * sizeof(ritEntry));
        (*n)--;
    }
    list[(*n)++] = e;
}

static void ritRemove(ritEntry *list, int *n, int i) {
    memmove(list + i, list + i + 1, (*n - i - 1) * sizeof(ritEntry));
    (*n)--;
}

/* The current command is done executing (or never reached call()). */
static void ritRetireCurrent(void) {
    if (!rit_cur_c) return;
    ritPush(rit_r, &rit_nr, rit_cur);
    rit_cur_c = NULL;
    rit_cur_depth = -1;
}

void ritDequeued(uint32_t id, void **argv) {
    vio_timing_stamp(id, RIT_DEQUEUED);
    ritPush(rit_q, &rit_nq, (ritEntry){.id = id, .argv = argv, .seq = rit_seq++});
}

void ritProcess(client *c) {
    for (int i = 0; i < rit_nq; i++) {
        if (rit_q[i].argv != (void **)c->argv) continue;
        ritRetireCurrent();
        rit_cur = rit_q[i];
        rit_cur.conn = c->conn;
        rit_cur_c = c;
        ritRemove(rit_q, &rit_nq, i);
        vio_timing_stamp(rit_cur.id, RIT_PROCESS);
        return;
    }
}

void ritCall(client *c, int stage) {
    UNUSED(c);
    if (stage == RIT_CALL) {
        if (rit_cur_depth != -1) return; /* nested call() of EXEC or a script */
        rit_cur_depth = server.in_call;
        vio_timing_stamp(rit_cur.id, RIT_CALL);
    } else if (stage == RIT_CALL_EXIT && rit_cur_depth == server.in_call) {
        vio_timing_stamp(rit_cur.id, RIT_CALL_EXIT);
        ritRetireCurrent();
    }
}

/* Before the reply bytes go to Rust: the oldest pending trace of this
 * connection is handed to vio_conn_writev(), which tags the bytes. */
void ritWritev(connection *conn) {
    if (rit_cur_c && rit_cur.conn == conn) ritRetireCurrent(); /* replied without call() */
    for (int i = 0; i < rit_nr; i++) {
        if (rit_r[i].conn != conn) continue;
        vio_timing_stamp(rit_r[i].id, RIT_REPLY_COPIED);
        vio_timing_next_write(rit_r[i].id);
        ritRemove(rit_r, &rit_nr, i);
        return;
    }
}

static const char *rit_names[] = {"read",     "parsed",     "argv",      "precheck",     "enqueued", "dequeued",
                                  "process_entry", "call_entry", "call_exit", "reply_copied", "written",  "end_to_end"};

sds ritCatInfo(sds info) {
    info = sdscatprintf(info,
                        "# Rust_io_timing\r\n"
                        "rust_io_timing_sample_n:%u\r\n"
                        "rust_io_timing_dropped:%llu\r\n"
                        "rust_io_timing_evicted:%llu\r\n"
                        "rust_io_timing_queued:%d\r\n"
                        "rust_io_timing_replied:%d\r\n",
                        vio_timing_sample_n(), (unsigned long long)vio_timing_dropped(), rit_evicted, rit_nq, rit_nr);
    if (!rit_fa_tsc0) rit_fa_tsc0 = ritTicks(), rit_fa_ns0 = ritMonoNs();
    double ns_per_tick = (double)(ritMonoNs() - rit_fa_ns0) / (double)(ritTicks() - rit_fa_tsc0 + 1);
    info = sdscatprintf(info, "free_argv_calls:%llu\r\nfree_argv_ns_per_call:%.1f\r\n", (unsigned long long)rit_fa_n,
                        rit_fa_n ? (double)rit_fa_ticks * ns_per_tick / (double)rit_fa_n : 0.0);
    /* Stage k = stamp k minus the latest earlier stamp present. */
    for (unsigned k = 1; k < sizeof(rit_names) / sizeof(rit_names[0]); k++) {
        VioTimingStat s;
        vio_timing_stat(k, &s);
        info = sdscatprintf(info, "stage_%s:count=%llu,p50=%llu,p90=%llu,p99=%llu,max=%llu\r\n", rit_names[k],
                            (unsigned long long)s.count, (unsigned long long)s.p50_ns, (unsigned long long)s.p90_ns,
                            (unsigned long long)s.p99_ns, (unsigned long long)s.max_ns);
    }
    return info;
}

void ritReset(void) {
    vio_timing_reset();
    rit_fa_ticks = rit_fa_n = 0;
    rit_fa_tsc0 = ritTicks();
    rit_fa_ns0 = ritMonoNs();
    rit_evicted = 0;
}
#endif
