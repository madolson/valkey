/*
 * valkey-dlhost: loads N valkey-server instances into one process with
 * dlmopen(LM_ID_NEWLM), so each has its own copy of every global, and puts
 * M shared IO threads in front of them.
 *
 * The IO threads own the client sockets. They parse RESP only far enough to
 * find command boundaries and the first key, hash the key to a slot (CRC16
 * with hash tags), and push the raw command bytes into the owning instance's
 * inbox. Instance i owns slots [i*16384/N, (i+1)*16384/N). Each instance
 * runs in standalone mode on its own thread; the host enforces ownership.
 * Replies come back per (connection, instance) as byte streams; the IO
 * thread splits them into whole RESP replies and writes them in request
 * order, so a pipeline may mix instances.
 *
 * Usage: valkey-dlhost [options] [-- extra valkey-server args]
 *   --engines N --io-threads M --port P --so path --rundir dir
 *   --pin-io "cpu,cpu,..."   pin IO thread k to the k-th cpu
 *   --pin-engines "cpu,..."  pin engine i to the i-th cpu (sets server-cpulist)
 *   --copy-dlopen            dlopen a private copy per engine instead of dlmopen
 *   --node-ports             also listen on port+1..port+N-1 and answer CLUSTER
 *                            SLOTS with engine i's range on port+i, so a
 *                            cluster client sends each connection one engine's
 *                            keys (no mixed pipelines)
 *   --raw                    hand engines raw RESP to re-parse on their main
 *                            thread, instead of argv built on the IO thread
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "dlhost.h"

#define MAXN 16
#define HOSTE MAXN /* pseudo-engine: replies the host generates itself */
#define MAXIO 64
#define DLHOST_MSG_REPLY 3
#define DLHOST_MSG_ECLOSED 4

uint16_t crc16(const char *buf, int len);

typedef struct buf {
    char *p;
    size_t len, cap, off;
} buf;

typedef struct hconn {
    int fd;
    int tid;
    atomic_int refs;
    int closed;
    int dirty; /* on the IO thread's dirty list */
    int want_out;
    int close_engine; /* 1 + engine that closed its client (QUIT, kill) */
    struct hconn *dirty_next;
    buf in, out;
    buf rep[MAXN + 1];
    uint8_t *order;
    size_t ohead, otail, ocap; /* ring of engine ids, one per request */
    void *eslot[MAXN];
} hconn;

typedef struct iothread {
    int id;
    int ep;
    int lfd[MAXN];
    dlhostInbox inbox; /* replies from engines */
    pthread_t tid;
    int cpu;
    dlhostMsg *chain_head[MAXN], *chain_tail[MAXN];
    hconn *dirty;
} iothread;

static int N = 2, M = 2, port = 10100;
static const char *so_path = "./libvalkey-engine.so";
static const char *rundir = "/tmp/chunky-dlmopen-run";
static int copy_dlopen = 0;
static int raw_mode = 0;
static int node_ports = 0;
static dlhostEngineParseFn eng_parse[MAXN];
static void (*ns_ctype_init[MAXN])(void);

static void *failingMorecore(ptrdiff_t n) {
    (void)n;
    return NULL;
}
static int pin_io[MAXIO], npin_io = 0;
static int pin_eng[MAXN], npin_eng = 0;
static dlhostInbox engine_inbox[MAXN];
/* Replies an engine queued this event-loop iteration, per IO thread. Only
 * engine i's thread touches pending_head[i][*]. */
static dlhostMsg *pending_head[MAXN][MAXIO], *pending_tail[MAXN][MAXIO];
static iothread io[MAXIO];
static atomic_int engines_ready;
static int extra_argc;
static char **extra_argv;

/* ---------------- buffers ---------------- */

static void bufReserve(buf *b, size_t need) {
    if (b->len + need <= b->cap) return;
    if (b->off) {
        memmove(b->p, b->p + b->off, b->len - b->off);
        b->len -= b->off;
        b->off = 0;
        if (b->len + need <= b->cap) return;
    }
    size_t cap = b->cap ? b->cap : 16384;
    while (cap < b->len + need) cap *= 2;
    b->p = realloc(b->p, cap);
    b->cap = cap;
}

static void bufAppend(buf *b, const void *p, size_t n) {
    bufReserve(b, n);
    memcpy(b->p + b->len, p, n);
    b->len += n;
}

/* ---------------- inbox ---------------- */

/* Push a chain [first..last]; wake the consumer if the inbox was empty. */
static void inboxPush(dlhostInbox *ib, dlhostMsg *first, dlhostMsg *last) {
    dlhostMsg *old = atomic_load_explicit(&ib->head, memory_order_relaxed);
    do {
        last->next = old;
    } while (!atomic_compare_exchange_weak_explicit(&ib->head, &old, first, memory_order_release,
                                                    memory_order_relaxed));
    if (old == NULL) {
        uint64_t one = 1;
        if (write(ib->wake_fd, &one, sizeof(one)) < 0) {
        }
    }
}

static dlhostMsg *inboxTakeAll(dlhostInbox *ib) {
    dlhostMsg *m = atomic_exchange_explicit(&ib->head, NULL, memory_order_acquire);
    dlhostMsg *rev = NULL;
    while (m) {
        dlhostMsg *next = m->next;
        m->next = rev;
        rev = m;
        m = next;
    }
    return rev;
}

/* ---------------- refcounting ---------------- */

static void hconnRelease(hconn *hc) {
    if (atomic_fetch_sub_explicit(&hc->refs, 1, memory_order_acq_rel) != 1) return;
    free(hc->in.p);
    free(hc->out.p);
    for (int i = 0; i < N; i++) free(hc->rep[i].p);
    free(hc->rep[HOSTE].p);
    free(hc->order);
    free(hc);
}

static dlhostMsg *msgNew(int type, hconn *hc, size_t len) {
    dlhostMsg *m = malloc(sizeof(*m) + len);
    m->next = NULL;
    m->type = type;
    m->hconn = hc;
    m->engine_slot = NULL;
    m->len = len;
    m->pos = 0;
    m->parsed = NULL;
    m->nparsed = 0;
    atomic_fetch_add_explicit(&hc->refs, 1, memory_order_relaxed);
    return m;
}

/* ---------------- callbacks from engines (engine threads) ---------------- */

static void opReply(void *h, int idx, const struct iovec *iov, int iovcnt) {
    hconn *hc = h;
    size_t n = 0;
    for (int i = 0; i < iovcnt; i++) n += iov[i].iov_len;
    dlhostMsg *m = msgNew(DLHOST_MSG_REPLY, hc, n);
    m->pos = idx;
    size_t o = 0;
    for (int i = 0; i < iovcnt; i++) {
        memcpy(m->data + o, iov[i].iov_base, iov[i].iov_len);
        o += iov[i].iov_len;
    }
    int t = hc->tid;
    if (pending_tail[idx][t])
        pending_tail[idx][t]->next = m;
    else
        pending_head[idx][t] = m;
    pending_tail[idx][t] = m;
}

static void opFlush(int idx) {
    for (int t = 0; t < M; t++) {
        if (!pending_head[idx][t]) continue;
        inboxPush(&io[t].inbox, pending_head[idx][t], pending_tail[idx][t]);
        pending_head[idx][t] = pending_tail[idx][t] = NULL;
    }
}

static void opEngineClosed(void *h, int idx, int by_engine) {
    hconn *hc = h;
    opFlush(idx); /* keep replies ahead of the close */
    if (by_engine) {
        dlhostMsg *m = msgNew(DLHOST_MSG_ECLOSED, hc, 0);
        m->pos = idx;
        inboxPush(&io[hc->tid].inbox, m, m);
    }
    hconnRelease(hc); /* the engine client's reference */
}

static void opEngineRef(void *h) {
    atomic_fetch_add_explicit(&((hconn *)h)->refs, 1, memory_order_relaxed);
}

static void opMsgDone(dlhostMsg *m) {
    hconn *hc = m->hconn;
    free(m);
    hconnRelease(hc);
}

static void opReady(int idx) {
    (void)idx;
    atomic_fetch_add(&engines_ready, 1);
}

static const dlhostOps ops = {
    .reply = opReply,
    .flush = opFlush,
    .engine_closed = opEngineClosed,
    .engine_ref = opEngineRef,
    .msg_done = opMsgDone,
    .ready = opReady,
};

/* ---------------- RESP ---------------- */

/* Length of the CRLF-terminated line at p, including CRLF, or 0. */
static size_t lineLen(const char *p, size_t len) {
    const char *nl = memchr(p, '\n', len);
    return nl ? (size_t)(nl - p) + 1 : 0;
}

/* Bytes in one complete reply at p, 0 if incomplete. */
static size_t respSkip(const char *p, size_t len) {
    if (len == 0) return 0;
    size_t l = lineLen(p, len);
    if (!l) return 0;
    long long n;
    switch (p[0]) {
    case '$':
    case '!':
    case '=':
        n = strtoll(p + 1, NULL, 10);
        if (n < 0) return l;
        if (l + n + 2 > len) return 0;
        return l + n + 2;
    case '*':
    case '~':
    case '>':
    case '%':
    case '|': {
        n = strtoll(p + 1, NULL, 10);
        if (n < 0) return l;
        if (p[0] == '%' || p[0] == '|') n *= 2;
        size_t o = l;
        for (long long i = 0; i < n; i++) {
            size_t k = respSkip(p + o, len - o);
            if (!k) return 0;
            o += k;
        }
        if (p[0] == '|') { /* an attribute precedes the real reply */
            size_t k = respSkip(p + o, len - o);
            if (!k) return 0;
            o += k;
        }
        return o;
    }
    default: return l;
    }
}

static int keyHashSlot(const char *key, int keylen) {
    int s, e;
    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;
    if (s == keylen) return crc16(key, keylen) & 16383;
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}') break;
    if (e == keylen || e == s + 1) return crc16(key, keylen) & 16383;
    return crc16(key + s + 1, e - s - 1) & 16383;
}

/* Commands whose argv[1] is not a key. They go to engine 0. */
static const char *keyless[] = {"ping", "echo", "info", "config", "client", "command", "hello", "auth",
                                "select", "dbsize", "flushall", "flushdb", "debug", "save", "bgsave", "script",
                                "function", "cluster", "memory", "latency", "slowlog", "acl", "publish",
                                "subscribe", "eval", "evalsha", "fcall", "scan", "keys", "multi", "exec",
                                "wait", "shutdown", "module", NULL};

static size_t keyless_len[64];

static int isKeyless(const char *c, size_t l) {
    if (l == 3 && (c[0] | 0x20) != 'a' && (c[0] | 0x20) != 'd') return 0; /* GET, SET, ... */
    for (int i = 0; keyless[i]; i++)
        if (keyless_len[i] == l && !strncasecmp(keyless[i], c, l)) return 1;
    return 0;
}

/* Parse one request at p. Returns its length (0 if incomplete, -1 on
 * protocol error) and sets *engine. */
static long parseRequest(const char *p, size_t len, int *engine) {
    *engine = 0;
    if (p[0] != '*') { /* inline command: route to engine 0 */
        size_t l = lineLen(p, len);
        return l ? (long)l : 0;
    }
    size_t l = lineLen(p, len);
    if (!l) return 0;
    long argc = strtol(p + 1, NULL, 10);
    size_t o = l;
    const char *a0 = NULL, *a1 = NULL;
    long l0 = 0, l1 = 0;
    for (long i = 0; i < argc; i++) {
        if (o >= len) return 0;
        if (p[o] != '$') return -1;
        size_t hl = lineLen(p + o, len - o);
        if (!hl) return 0;
        long al = strtol(p + o + 1, NULL, 10);
        if (al < 0) return -1;
        if (o + hl + al + 2 > len) return 0;
        if (i == 0) a0 = p + o + hl, l0 = al;
        if (i == 1) a1 = p + o + hl, l1 = al;
        o += hl + al + 2;
    }
    if (argc == 2 && l0 == 7 && l1 == 5 && !strncasecmp(a0, "cluster", 7) && !strncasecmp(a1, "slots", 5)) {
        *engine = HOSTE;
        return (long)o;
    }
    if (argc >= 2 && N > 1 && !isKeyless(a0, l0)) *engine = keyHashSlot(a1, (int)l1) * N / 16384;
    return (long)o;
}

/* ---------------- IO thread ---------------- */

static void orderPush(hconn *hc, int e) {
    if (hc->otail - hc->ohead == hc->ocap) {
        size_t ncap = hc->ocap ? hc->ocap * 2 : 64;
        uint8_t *n = malloc(ncap);
        for (size_t i = 0; i < hc->ocap; i++) n[i] = hc->order[(hc->ohead + i) % hc->ocap];
        free(hc->order);
        hc->order = n;
        hc->otail = hc->otail - hc->ohead;
        hc->ohead = 0;
        hc->ocap = ncap;
    }
    hc->order[hc->otail % hc->ocap] = (uint8_t)e;
    hc->otail++;
}

static void markDirty(iothread *t, hconn *hc) {
    if (hc->dirty) return;
    hc->dirty = 1;
    hc->dirty_next = t->dirty;
    t->dirty = hc;
}

static void chainAdd(iothread *t, int e, dlhostMsg *m) {
    m->next = NULL;
    if (t->chain_tail[e])
        t->chain_tail[e]->next = m;
    else
        t->chain_head[e] = m;
    t->chain_tail[e] = m;
}

static void connClose(iothread *t, hconn *hc) {
    if (hc->closed) return;
    hc->closed = 1;
    epoll_ctl(t->ep, EPOLL_CTL_DEL, hc->fd, NULL);
    close(hc->fd);
    for (int e = 0; e < N; e++) {
        dlhostMsg *m = msgNew(DLHOST_MSG_CLOSE, hc, 0);
        m->engine_slot = &hc->eslot[e];
        chainAdd(t, e, m);
    }
    /* The IO thread's reference is dropped once it is off the dirty list. */
    if (!hc->dirty) hconnRelease(hc);
}

/* CLUSTER SLOTS as the host presents itself: one node owning everything, or
 * with --node-ports, engine i's range served on port+i. */
static void appendClusterSlots(buf *b) {
    char tmp[256];
    int nodes = node_ports ? N : 1;
    int l = snprintf(tmp, sizeof(tmp), "*%d\r\n", nodes);
    bufAppend(b, tmp, l);
    for (int i = 0; i < nodes; i++) {
        int from = i * 16384 / nodes, to = (i + 1) * 16384 / nodes - 1;
        char id[41];
        snprintf(id, sizeof(id), "%040d", i + 1);
        l = snprintf(tmp, sizeof(tmp), "*3\r\n:%d\r\n:%d\r\n*3\r\n$9\r\n127.0.0.1\r\n:%d\r\n$40\r\n%s\r\n", from, to,
                     port + i, id);
        bufAppend(b, tmp, l);
    }
}

#define MAXSPANS 1024
static void connRead(iothread *t, hconn *hc) {
    if (hc->close_engine) return; /* nothing after QUIT runs */
    for (;;) {
        bufReserve(&hc->in, 65536);
        ssize_t n = read(hc->fd, hc->in.p + hc->in.len, hc->in.cap - hc->in.len);
        if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) {
            connClose(t, hc);
            return;
        }
        if (n < 0) break;
        hc->in.len += n;
        if ((size_t)n < 65536) break;
    }
    /* Split into requests, then copy each engine's requests into one msg. */
    long start[MAXSPANS], len[MAXSPANS];
    int eng[MAXSPANS];
    for (;;) {
        int ns = 0;
        size_t tot[MAXN + 1] = {0};
        size_t o = hc->in.off;
        while (ns < MAXSPANS && o < hc->in.len) {
            int e;
            long l = parseRequest(hc->in.p + o, hc->in.len - o, &e);
            if (l < 0) {
                connClose(t, hc);
                return;
            }
            if (l == 0) break;
            start[ns] = o;
            len[ns] = l;
            eng[ns] = e;
            tot[e] += l;
            ns++;
            o += l;
        }
        if (ns == 0) break;
        dlhostMsg *m[MAXN + 1] = {0};
        int inl[MAXN + 1] = {0};
        for (int i = 0; i < ns; i++)
            if (hc->in.p[start[i]] != '*') inl[eng[i]] = 1;
        for (int e = 0; e < N; e++) {
            if (!tot[e]) continue;
            if (!raw_mode && !inl[e]) {
                /* Build the engine's argv here, on the IO thread. */
                struct iovec iov[MAXSPANS];
                int n = 0;
                for (int i = 0; i < ns; i++)
                    if (eng[i] == e) {
                        iov[n].iov_base = hc->in.p + start[i];
                        iov[n].iov_len = len[i];
                        n++;
                    }
                m[e] = msgNew(DLHOST_MSG_PARSED, hc, 0);
                m[e]->engine_slot = &hc->eslot[e];
                m[e]->parsed = eng_parse[e](iov, n);
                m[e]->nparsed = n;
                continue;
            }
            m[e] = msgNew(DLHOST_MSG_DATA, hc, tot[e]);
            m[e]->engine_slot = &hc->eslot[e];
            m[e]->len = 0;
        }
        for (int i = 0; i < ns; i++) {
            if (eng[i] == HOSTE) {
                appendClusterSlots(&hc->rep[HOSTE]);
                markDirty(t, hc);
                orderPush(hc, HOSTE);
                continue;
            }
            dlhostMsg *x = m[eng[i]];
            if (x->type == DLHOST_MSG_DATA) {
                memcpy(x->data + x->len, hc->in.p + start[i], len[i]);
                x->len += len[i];
            }
            orderPush(hc, eng[i]);
        }
        for (int e = 0; e < N; e++)
            if (m[e]) chainAdd(t, e, m[e]);
        hc->in.off = o;
        if (ns < MAXSPANS) break;
    }
    if (hc->in.off == hc->in.len) hc->in.off = hc->in.len = 0;
}

static void connFlush(iothread *t, hconn *hc) {
    int close_now = 0;
    while (hc->ohead != hc->otail) {
        int e = hc->order[hc->ohead % hc->ocap];
        buf *b = &hc->rep[e];
        size_t k = respSkip(b->p + b->off, b->len - b->off);
        if (!k) {
            /* The engine that closed will not reply again. */
            if (hc->close_engine == e + 1) close_now = 1;
            break;
        }
        bufAppend(&hc->out, b->p + b->off, k);
        b->off += k;
        if (b->off == b->len) b->off = b->len = 0;
        hc->ohead++;
    }
    while (hc->out.off < hc->out.len) {
        ssize_t n = write(hc->fd, hc->out.p + hc->out.off, hc->out.len - hc->out.off);
        if (n < 0) {
            if (errno == EAGAIN) break;
            connClose(t, hc);
            return;
        }
        hc->out.off += n;
    }
    if (hc->out.off == hc->out.len) hc->out.off = hc->out.len = 0;
    if (hc->close_engine && (close_now || hc->ohead == hc->otail)) {
        connClose(t, hc); /* unwritten output is dropped, as on a socket */
        return;
    }
    int want = hc->out.len != 0;
    if (want != hc->want_out) {
        struct epoll_event ev = {.events = EPOLLIN | (want ? EPOLLOUT : 0), .data.ptr = hc};
        epoll_ctl(t->ep, EPOLL_CTL_MOD, hc->fd, &ev);
        hc->want_out = want;
    }
}

static void handleReplies(iothread *t) {
    uint64_t v;
    if (read(t->inbox.wake_fd, &v, sizeof(v)) < 0) {
    }
    dlhostMsg *m = inboxTakeAll(&t->inbox);
    while (m) {
        dlhostMsg *next = m->next;
        hconn *hc = m->hconn;
        if (!hc->closed) {
            if (m->type == DLHOST_MSG_REPLY) {
                bufAppend(&hc->rep[m->pos], m->data, m->len);
                markDirty(t, hc);
            } else if (m->type == DLHOST_MSG_ECLOSED) {
                /* Close once every earlier reply is written (QUIT, kill). */
                hc->close_engine = (int)m->pos + 1;
                markDirty(t, hc);
            }
        }
        opMsgDone(m);
        m = next;
    }
}

static void *ioMain(void *arg) {
    iothread *t = arg;
    if (t->cpu >= 0) {
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(t->cpu, &cs);
        pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
    }
    for (int e = 0; e < N; e++) ns_ctype_init[e]();
    char name[16];
    snprintf(name, sizeof(name), "dlhost-io%d", t->id);
    pthread_setname_np(pthread_self(), name);
    struct epoll_event evs[256];
    for (;;) {
        int n = epoll_wait(t->ep, evs, 256, -1);
        for (int i = 0; i < n; i++) {
            void *p = evs[i].data.ptr;
            if (p >= (void *)t->lfd && p < (void *)(t->lfd + MAXN)) {
                for (;;) {
                    int fd = accept4(*(int *)p, NULL, NULL, SOCK_NONBLOCK);
                    if (fd < 0) break;
                    int one = 1;
                    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                    hconn *hc = calloc(1, sizeof(*hc));
                    hc->fd = fd;
                    hc->tid = t->id;
                    atomic_init(&hc->refs, 1);
                    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = hc};
                    epoll_ctl(t->ep, EPOLL_CTL_ADD, fd, &ev);
                }
            } else if (p == &t->inbox) {
                handleReplies(t);
            } else {
                hconn *hc = p;
                if (hc->closed) continue;
                if (evs[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) connRead(t, hc);
                if (!hc->closed && (evs[i].events & EPOLLOUT)) markDirty(t, hc);
            }
        }
        for (int e = 0; e < N; e++) {
            if (!t->chain_head[e]) continue;
            inboxPush(&engine_inbox[e], t->chain_head[e], t->chain_tail[e]);
            t->chain_head[e] = t->chain_tail[e] = NULL;
        }
        while (t->dirty) {
            hconn *hc = t->dirty;
            t->dirty = hc->dirty_next;
            hc->dirty = 0;
            if (!hc->closed)
                connFlush(t, hc);
            else
                hconnRelease(hc);
        }
        /* connFlush may have closed connections and queued CLOSE msgs. */
        for (int e = 0; e < N; e++) {
            if (!t->chain_head[e]) continue;
            inboxPush(&engine_inbox[e], t->chain_head[e], t->chain_tail[e]);
            t->chain_head[e] = t->chain_tail[e] = NULL;
        }
    }
    return NULL;
}

/* ---------------- engines ---------------- */

typedef struct {
    int idx;
    dlhostEngineMainFn main;
    int argc;
    char **argv;
} engineArgs;

static void *engineThread(void *arg) {
    engineArgs *a = arg;
    char name[16];
    snprintf(name, sizeof(name), "engine%d", a->idx);
    pthread_setname_np(pthread_self(), name);
    int rc = a->main(a->argc, a->argv);
    fprintf(stderr, "dlhost: engine %d main returned %d\n", a->idx, rc);
    exit(rc);
}

static void parseCpus(const char *s, int *out, int *n) {
    *n = 0;
    while (*s && *n < MAXIO) {
        out[(*n)++] = atoi(s);
        s = strchr(s, ',');
        if (!s) break;
        s++;
    }
}

static void loadEngine(int i) {
    void *h;
    if (copy_dlopen) {
        char path[512], cmd[1200];
        snprintf(path, sizeof(path), "%s/engine-%d.so", rundir, i);
        snprintf(cmd, sizeof(cmd), "cp '%s' '%s'", so_path, path);
        if (system(cmd) != 0) exit(1);
        h = dlopen(path, RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
    } else {
        h = dlmopen(LM_ID_NEWLM, so_path, RTLD_NOW | RTLD_LOCAL);
    }
    if (!h) {
        fprintf(stderr, "dlhost: loading engine %d failed: %s\n", i, dlerror());
        exit(1);
    }
    dlhostEngineSetupFn setup = (dlhostEngineSetupFn)dlsym(h, "dlhostEngineSetup");
    dlhostEngineMainFn emain = (dlhostEngineMainFn)dlsym(h, "valkey_engine_main");
    eng_parse[i] = (dlhostEngineParseFn)dlsym(h, "dlhostEngineParse");
    /* IO threads call into the namespace (dlhostEngineParse), whose libc
     * never ran its per-thread init on them; they call this first. */
    ns_ctype_init[i] = (void (*)(void))dlsym(h, "__ctype_init");
    /* glibc 2.26 lets every namespace's main arena grow with brk; glibc
     * 2.27+ forbids it outside the base namespace. Make it mmap instead. */
    void **morecore = dlsym(h, "__morecore");
    if (morecore) *morecore = (void *)failingMorecore;
    if (!setup || !emain || !eng_parse[i] || !ns_ctype_init[i] || !morecore) {
        fprintf(stderr, "dlhost: engine %d missing symbols: %s\n", i, dlerror());
        exit(1);
    }
    engine_inbox[i].wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    atomic_init(&engine_inbox[i].head, NULL);
    setup(&ops, i, &engine_inbox[i]);

    char dir[512];
    snprintf(dir, sizeof(dir), "%s/e%d", rundir, i);
    mkdir(dir, 0755);
    engineArgs *a = calloc(1, sizeof(*a));
    a->idx = i;
    a->main = emain;
    a->argv = calloc(extra_argc + 32, sizeof(char *));
    int c = 0;
    a->argv[c++] = strdup("valkey-server");
    a->argv[c++] = strdup("--port");
    a->argv[c++] = strdup("0");
    a->argv[c++] = strdup("--unixsocket");
    a->argv[c++] = malloc(600);
    snprintf(a->argv[c - 1], 600, "%s/valkey.sock", dir);
    a->argv[c++] = strdup("--dir");
    a->argv[c++] = strdup(dir);
    a->argv[c++] = strdup("--logfile");
    a->argv[c++] = malloc(600);
    snprintf(a->argv[c - 1], 600, "%s/valkey.log", dir);
    a->argv[c++] = strdup("--save");
    a->argv[c++] = strdup("");
    a->argv[c++] = strdup("--appendonly");
    a->argv[c++] = strdup("no");
    if (npin_eng > i) {
        a->argv[c++] = strdup("--server-cpulist");
        a->argv[c++] = malloc(16);
        snprintf(a->argv[c - 1], 16, "%d", pin_eng[i]);
    }
    for (int k = 0; k < extra_argc; k++) a->argv[c++] = strdup(extra_argv[k]);
    a->argc = c;
    /* Start the engine thread with the namespace's own pthread_create. A
     * thread from the host's libpthread never runs the namespace libc's
     * per-thread init (__ctype_init), so tolower() faults on a NULL table. */
    typedef int (*pcreate_fn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
    typedef int (*pattr_fn)(pthread_attr_t *);
    typedef int (*pstack_fn)(pthread_attr_t *, size_t);
    pcreate_fn pcreate = (pcreate_fn)dlsym(h, "pthread_create");
    pattr_fn pinit = (pattr_fn)dlsym(h, "pthread_attr_init");
    pstack_fn pstack = (pstack_fn)dlsym(h, "pthread_attr_setstacksize");
    if (!pcreate || !pinit || !pstack) {
        fprintf(stderr, "dlhost: engine %d has no pthread_create: %s\n", i, dlerror());
        exit(1);
    }
    pthread_t tid;
    pthread_attr_t attr;
    pinit(&attr);
    pstack(&attr, 16 << 20);
    if (pcreate(&tid, &attr, engineThread, a) != 0) {
        fprintf(stderr, "dlhost: engine %d pthread_create failed\n", i);
        exit(1);
    }
}

static void *noopThread(void *arg) {
    return arg;
}

static void onTerm(int sig) {
    (void)sig;
    _exit(0);
}

int main(int argc, char **argv) {
    int i;
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--engines") && i + 1 < argc)
            N = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--io-threads") && i + 1 < argc)
            M = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--port") && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--so") && i + 1 < argc)
            so_path = argv[++i];
        else if (!strcmp(argv[i], "--rundir") && i + 1 < argc)
            rundir = argv[++i];
        else if (!strcmp(argv[i], "--pin-io") && i + 1 < argc)
            parseCpus(argv[++i], pin_io, &npin_io);
        else if (!strcmp(argv[i], "--pin-engines") && i + 1 < argc)
            parseCpus(argv[++i], pin_eng, &npin_eng);
        else if (!strcmp(argv[i], "--node-ports"))
            node_ports = 1;
        else if (!strcmp(argv[i], "--raw"))
            raw_mode = 1;
        else if (!strcmp(argv[i], "--copy-dlopen"))
            copy_dlopen = 1;
        else if (!strcmp(argv[i], "--")) {
            i++;
            break;
        } else {
            fprintf(stderr, "unknown option %s\n", argv[i]);
            return 1;
        }
    }
    extra_argc = argc - i;
    extra_argv = argv + i;
    if (N < 1 || N > MAXN || M < 1 || M > MAXIO) return 1;
    for (int k = 0; keyless[k]; k++) keyless_len[k] = strlen(keyless[k]);
    signal(SIGPIPE, SIG_IGN);
    /* ld.so allocates the engines' dynamic TLS (__tls_get_addr) with this
     * namespace's malloc, from threads the engines' libpthread created.
     * Those threads never ran this libc's __ctype_init, and arena_get2()
     * parses /sys with isspace() via get_nprocs() unless arena_max is set. */
    /* This libc's internal locks (malloc arenas included) skip the lock
     * prefix until its own libpthread has created a thread. Engine threads
     * come from the namespaces' libpthread, yet ld.so allocates their TLS
     * and DTVs with this libc's malloc, concurrently. Create one thread now
     * so this libc knows it is multithreaded. */
    pthread_t noop;
    pthread_create(&noop, NULL, noopThread, NULL);
    pthread_join(noop, NULL);
    mallopt(M_ARENA_MAX, 2 * MAXIO);
    /* Every libc copy caches the program break in its own __curbrk, so two
     * copies growing their main arena with brk shrink each other's heap.
     * No copy may use brk; this one included. */
    __morecore = failingMorecore;
    /* Engines chdir() to their --dir, and the cwd is process-wide. */
    static char so_abs[4096];
    if (!realpath(so_path, so_abs)) {
        perror(so_path);
        return 1;
    }
    so_path = so_abs;
    mkdir(rundir, 0755);

    /* Start engines one at a time: concurrent startup across namespaces
     * crashed (see the report). */
    for (int e = 0; e < N; e++) {
        loadEngine(e);
        for (int k = 0; k < 10000 && atomic_load(&engines_ready) < e + 1; k++) usleep(1000);
        if (atomic_load(&engines_ready) < e + 1) {
            fprintf(stderr, "dlhost: engine %d not ready\n", e);
            return 1;
        }
    }

    for (int k = 0; k < M; k++) {
        iothread *t = &io[k];
        t->id = k;
        t->cpu = npin_io > k ? pin_io[k] : -1;
        t->ep = epoll_create1(EPOLL_CLOEXEC);
        t->inbox.wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        atomic_init(&t->inbox.head, NULL);
        struct epoll_event ev = {.events = EPOLLIN};
        for (int pi = 0; pi < (node_ports ? N : 1); pi++) {
            t->lfd[pi] = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
            int one = 1;
            setsockopt(t->lfd[pi], SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
            setsockopt(t->lfd[pi], SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
            struct sockaddr_in sa = {.sin_family = AF_INET,
                                     .sin_port = htons(port + pi),
                                     .sin_addr.s_addr = htonl(0x7f000001)};
            if (bind(t->lfd[pi], (struct sockaddr *)&sa, sizeof(sa)) < 0 || listen(t->lfd[pi], 1024) < 0) {
                perror("bind/listen");
                return 1;
            }
            ev.data.ptr = &t->lfd[pi];
            epoll_ctl(t->ep, EPOLL_CTL_ADD, t->lfd[pi], &ev);
        }
        ev.data.ptr = &t->inbox;
        epoll_ctl(t->ep, EPOLL_CTL_ADD, t->inbox.wake_fd, &ev);
        pthread_create(&t->tid, NULL, ioMain, t);
    }
    /* Each engine installed valkey's SIGTERM/SIGINT handlers, and the last
     * one won: a signal would shut down only that engine's copy of `server`
     * before its exit() takes the whole process down. Exit directly. */
    signal(SIGTERM, onTerm);
    signal(SIGINT, onTerm);
    fprintf(stderr, "dlhost: %d engines (%s), %d io threads, port %d, %s\n", N,
            copy_dlopen ? "dlopen copies" : "dlmopen", M, port, raw_mode ? "raw RESP" : "argv parsed on IO threads");
    for (;;) pause();
}
