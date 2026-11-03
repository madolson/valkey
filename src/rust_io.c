/*
 * rust_io.c - engine side of the valkey-io Rust IO layer (cut point 1).
 *
 * Rust owns client sockets, reads, RESP parsing and writes. For each Rust
 * connection the engine has an ordinary client backed by a connection of
 * type "rust". Parsed commands are moved into the client's cmd_queue and the
 * standard processInputBuffer() path runs them, so blocking, MULTI, pause,
 * pipelining order and reply handling are the engine's own. Replies leave
 * through the connection type's write/writev, which copy into the Rust
 * outbound buffer.
 *
 * See src/rust/valkey-io/DESIGN.md for the threading model and FFI surface.
 */
#include "server.h"
#include "connhelpers.h"
#include "rust_io.h"
#include "rust_io_timing.h"
#include "connection_external.h"

#ifdef USE_RUST_IO

#include "rust/valkey-io/include/valkey_io.h"

/* Not exported through server.h. */
void trimCommandQueue(client *c);

rustIoConfig rust_io_config = {RUST_IO_MODE_OFF, 0, 1, 0};
int rust_io_precheck_active = 0;

configEnum rust_io_mode_enum[] = {
    {"off", RUST_IO_MODE_OFF},
    {"io", RUST_IO_MODE_IO},
    {"precheck", RUST_IO_MODE_PRECHECK},
    {NULL, 0},
};

/* Max commands kept in a client's cmd_queue. The rest wait in Rust, which
 * stops reading the socket once its own queue is full. */
#define RUST_IO_CMD_QUEUE_MAX 1024
#define RUST_IO_POP_BATCH 128
#define COMMAND_QUEUE_MIN_CAPACITY 16

typedef struct rustConnection {
    connection c;
    const VioConn *h; /* our reference; NULL once closed */
    bool stalled;     /* in rust_stalled: commands wait in Rust */
    int pending_err;  /* VIO_ERR_* seen but not yet replied, -1 once replied */
    int pending_err_byte;
    unsigned long long pulled; /* commands moved into cmd_queue so far */
    VioSession sess;           /* last session published to Rust (precheck) */
} rustConnection;

static list *rust_stalled; /* client ids */
static int rust_started = 0;
/* bind list at startup, joined, for rustIoListenerUnchanged(). */
static sds rust_bind_at_start = NULL;

static sds rustIoJoinBind(void) {
    sds s = sdsempty();
    for (int j = 0; j < server.bindaddr_count; j++) s = sdscatfmt(s, "%s ", server.bindaddr[j]);
    return s;
}

/* CONFIG SET bind/port under takeover: Rust cannot re-bind, so only a set
 * to the startup value is accepted (CONFIG SET of the current value, as in
 * "CONFIG sanity"). */
int rustIoListenerUnchanged(void) {
    if (!rust_started || server.port != rust_started) return 0;
    sds cur = rustIoJoinBind();
    int same = rust_bind_at_start && sdscmp(cur, rust_bind_at_start) == 0;
    sdsfree(cur);
    return same;
}

/* ---------------------------------------------------------------------------
 * Callbacks from Rust IO threads. Allocation only; no engine state.
 * ------------------------------------------------------------------------- */

static void **rustIoCreateArgv(const unsigned char *const *ptrs, const size_t *lens, int argc) {
    robj **argv = zmalloc(sizeof(robj *) * argc);
    for (int j = 0; j < argc; j++) argv[j] = createStringObject((const char *)ptrs[j], lens[j]);
    return (void **)argv;
}

static void rustIoFreeArgv(void **argv, int argc) {
    for (int j = 0; j < argc; j++) decrRefCount(argv[j]);
    zfree(argv);
}

/* ---------------------------------------------------------------------------
 * Command handoff
 * ------------------------------------------------------------------------- */

static int cmdQueuePending(client *c) {
    return c->cmd_queue.len - c->cmd_queue.off;
}

static void rustIoStall(rustConnection *rc, client *c) {
    if (rc->stalled) return;
    rc->stalled = true;
    listAddNodeTail(rust_stalled, (void *)(uintptr_t)c->id);
}

/* Move parsed commands from Rust into c->cmd_queue and prepare them
 * (lookup, arity, slot) on the main thread.
 *
 * STAGE 2 HOOK: in precheck mode prepareCommandGeneric() and the checks at
 * the top of processCommand() move to the Rust thread, before this handoff. */
static int rustIoPull(rustConnection *rc, client *c) {
    cmdQueue *q = &c->cmd_queue;
    VioCmd cmds[RUST_IO_POP_BATCH];
    unsigned long long bytes = 0;
    int pulled = 0;

    while (rc->pending_err == 0 && cmdQueuePending(c) < RUST_IO_CMD_QUEUE_MAX) {
        size_t want = RUST_IO_CMD_QUEUE_MAX - cmdQueuePending(c);
        if (want > RUST_IO_POP_BATCH) want = RUST_IO_POP_BATCH;
        size_t n = vio_conn_pop_cmds(rc->h, cmds, want);
        if (n == 0) break;

        if (q->off > 0) {
            memmove(q->cmds, q->cmds + q->off, (q->len - q->off) * sizeof(parsedCommand));
            q->len -= q->off;
            q->off = 0;
        }
        if (q->len + n > q->cap) {
            size_t cap = q->cap ? q->cap : COMMAND_QUEUE_MIN_CAPACITY;
            while (cap < q->len + n) cap *= 2;
            q->cap = cap;
            q->cmds = zrealloc(q->cmds, q->cap * sizeof(parsedCommand));
        }
        for (size_t i = 0; i < n; i++) {
            if (cmds[i].err) {
                rc->pending_err = cmds[i].err;
                rc->pending_err_byte = cmds[i].err_byte;
                break;
            }
            parsedCommand *p = &q->cmds[q->len++];
            p->read_flags = READ_FLAGS_PARSING_COMPLETED;
            p->argc = cmds[i].argc;
            p->argv = (robj **)cmds[i].argv;
            p->argv_len = cmds[i].argc;
            p->argv_len_sum = cmds[i].argv_len_sum;
            p->input_bytes = cmds[i].input_bytes;
            p->slot = -1;
            p->cmd = NULL;
            p->rust_precheck = cmds[i].token;
#ifdef RUST_IO_TIMING
            if (cmds[i].trace) ritDequeued(cmds[i].trace, cmds[i].argv);
#endif
            if (cmds[i].token && !(cmds[i].read_flags & READ_FLAGS_RUST_PREPARE_ON_MAIN)) {
                /* Precheck mode: lookup, arity and slot ran on the IO thread. */
                p->cmd = cmds[i].cmd;
                p->slot = cmds[i].slot;
                p->read_flags = cmds[i].read_flags | READ_FLAGS_RUST_PRECHECKED;
            } else {
                if (cmds[i].token) p->read_flags |= READ_FLAGS_RUST_PRECHECKED;
                prepareCommandGeneric(p->argv, p->argc, &p->read_flags, &p->cmd, &p->slot);
            }
            bytes += cmds[i].input_bytes;
            pulled++;
        }
    }
    rc->pulled += pulled;

    if (pulled) {
        /* What handleReadResult() accounts for a socket read. */
        server.stat_total_reads_processed++;
        server.stat_net_input_bytes += bytes;
        c->net_input_bytes += bytes;
        c->last_interaction = server.unixtime;
    }
    return pulled;
}

const VioSession *rustIoConnSession(connection *conn) {
    if (!connIsExternal(conn)) return NULL;
    return &((rustConnection *)conn)->sess;
}

/* Publish the session fields the precheck reads. The generation changes only
 * when a field does; 'consumed' lets Rust lift a barrier once the engine has
 * run the barrier command. */
static void rustIoPublishSession(rustConnection *rc, client *c) {
    if (!rust_io_precheck_active || !rc->h) return;
    VioSession *s = &rc->sess;
    unsigned int flags = rustPrecheckSessionFlags(c);
    int dbid = c->db ? c->db->id : -1;
    if (s->user != (void *)c->user || s->flags != flags || s->dbid != dbid) {
        s->user = c->user;
        s->flags = flags;
        s->dbid = dbid;
        s->gen++;
    }
    s->consumed = rc->pulled - cmdQueuePending(c);
    vio_conn_set_session(rc->h, s);
}

static void rustIoProtocolError(client *c, int err, int byte) {
    const char *msg = NULL;
    switch (err) {
    case VIO_ERR_BIG_INLINE: msg = "Protocol error: too big inline request"; break;
    case VIO_ERR_BIG_MULTIBULK: msg = "Protocol error: too big mbulk count string"; break;
    case VIO_ERR_INVALID_MULTIBULK_LEN: msg = "Protocol error: invalid multibulk length"; break;
    case VIO_ERR_UNAUTH_MULTIBULK_LEN: msg = "Protocol error: unauthenticated multibulk length"; break;
    case VIO_ERR_UNAUTH_BULK_LEN: msg = "Protocol error: unauthenticated bulk length"; break;
    case VIO_ERR_BIG_BULK_COUNT: msg = "Protocol error: too big bulk count string"; break;
    case VIO_ERR_INVALID_BULK_LEN: msg = "Protocol error: invalid bulk length"; break;
    case VIO_ERR_UNBALANCED_QUOTES: msg = "Protocol error: unbalanced quotes in request"; break;
    case VIO_ERR_NUL_IN_INLINE: msg = "Protocol error: embedded NUL byte in inline request"; break;
    case VIO_ERR_INVALID_CRLF: msg = "Protocol error: invalid CRLF in request"; break;
    case VIO_ERR_UNEXPECTED_CHAR: break;
    default: serverPanic("Unknown rust-io protocol error %d", err);
    }
    if (msg)
        addReplyError(c, msg);
    else
        addReplyErrorFormat(c, "Protocol error: expected '$', got '%c'", byte);
    serverLog(LL_VERBOSE, "Protocol error (%d) from rust-io client id=%llu", err, (unsigned long long)c->id);
    c->flag.close_after_reply = 1;
    c->flag.protocol_error = 1;
}

/* Whether rustIoDeliver() would make progress for this client now. */
static bool rustIoCanProgress(rustConnection *rc, client *c) {
    if (!rc->h || !rc->c.read_handler) return false;
    if (c->flag.close_asap || c->flag.close_after_reply) return false;
    if (rc->pending_err == 0 && cmdQueuePending(c) < RUST_IO_CMD_QUEUE_MAX && vio_conn_pending_cmds(rc->h) > 0)
        return true;
    if (!c->flag.blocked && canParseCommand(c) && (cmdQueuePending(c) > 0 || rc->pending_err > 0)) return true;
    return false;
}

/* Pull and execute what Rust parsed for this connection. Mirrors the tail
 * of readQueryFromClient(). The client may be freed. */
static void rustIoDeliver(rustConnection *rc) {
    const VioConn *h = rc->h;
    client *c = connGetPrivateData(&rc->c);
    if (!h || !c) return;
    if (c->flag.close_asap || c->flag.close_after_reply) return;
    /* protectClient() removes the read handler: hold input in Rust. */
    if (!rc->c.read_handler) {
        if (vio_conn_pending_cmds(h)) rustIoStall(rc, c);
        return;
    }

    rustIoPull(rc, c);
    if (rc->pending_err > 0 || vio_conn_pending_cmds(h)) rustIoStall(rc, c);

    if (c->flag.blocked) return; /* resumed by processUnblockedClients() */
    if (processInputBuffer(c) == C_ERR) return;
    if (rc->pending_err > 0 && cmdQueuePending(c) == 0 && canParseCommand(c)) {
        rustIoProtocolError(c, rc->pending_err, rc->pending_err_byte);
        rc->pending_err = -1;
    }
    trimCommandQueue(c);
    vio_conn_set_auth_required(h, authRequired(c));
    rustIoPublishSession(rc, c);
    /* May free the client (close_asap). */
    beforeNextClient(c);
}

/* ---------------------------------------------------------------------------
 * Accept and readiness
 * ------------------------------------------------------------------------- */

static void rustIoAccept(const VioConn *h) {
    rustConnection *rc = (rustConnection *)connCreateExternal(sizeof(rustConnection), vio_conn_fd(h));
    rc->h = vio_conn_retain(h);
    vio_conn_set_udata(h, rc);

    struct ClientFlags flags = {0};
    char cip[NET_IP_STR_LEN] = {0};
    int cport = 0;
    anetFdToString(rc->c.fd, cip, sizeof(cip), &cport, 1);
    serverLog(LL_VERBOSE, "Accepted %s:%d (rust-io)", cip, cport);
    if (server.tcpkeepalive) anetKeepAlive(NULL, rc->c.fd, server.tcpkeepalive);
    acceptCommonHandler(&rc->c, flags, cip);

    rc = vio_conn_udata(h);
    if (rc && connGetPrivateData(&rc->c)) {
        vio_conn_set_auth_required(h, authRequired(connGetPrivateData(&rc->c)));
        rustIoPublishSession(rc, connGetPrivateData(&rc->c));
    }
}

static void rustIoHandleReady(const VioConn *h) {
    uint32_t ev = vio_conn_take_events(h);
    if (ev & VIO_EV_NEW) rustIoAccept(h);

    rustConnection *rc = vio_conn_udata(h);
    if (!rc) return;
    if ((ev & VIO_EV_WRITABLE) && rc->c.write_handler) {
        if (!callHandler(&rc->c, rc->c.write_handler)) return;
    }

    rustIoDeliver(rc);

    if (ev & VIO_EV_QBLIMIT) {
        rc = vio_conn_udata(h);
        if (!rc) return;
        client *c = connGetPrivateData(&rc->c);
        if (c) {
            /* handleQbLimitReached() dumps c->querybuf, which is NULL here. */
            sds ci = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_WARNING, "Closing client that reached max query buffer length: %s", ci);
            sdsfree(ci);
            freeClientAsync(c);
            server.stat_client_qbuf_limit_disconnections++;
        }
        return;
    }

    if (ev & VIO_EV_CLOSED) {
        rc = vio_conn_udata(h);
        if (!rc) return;
        rc->c.state = CONN_STATE_CLOSED;
        client *c = connGetPrivateData(&rc->c);
        if (c) {
            serverLog(LL_VERBOSE, "Client closed connection (rust-io) id=%llu", (unsigned long long)c->id);
            freeClientAsync(c);
        }
    }
}

static int rustIoDrainReady(void) {
    const VioConn *h;
    int n = 0;
    while ((h = vio_main_next_ready()) != NULL) {
        rustIoHandleReady(h);
        vio_conn_release(h);
        n++;
    }
    return n;
}

static void rustIoEventHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);
    vio_main_begin_drain();
    rustIoDrainReady();
}

/* Publish engine-wide state the IO threads read without the engine lock
 * (the default user's auth requirement and the precheck ACL snapshot), take
 * the rest of a ready batch, and serve clients whose input waits in Rust
 * (cmd_queue full, blocked, protected). Called from beforeSleep() through
 * connTypeProcessPendingData(). */
static int connRustProcessPendingData(void) {
    vio_set_default_auth_required(!(DefaultUser->flags & USER_FLAG_NOPASS) || (DefaultUser->flags & USER_FLAG_DISABLED));
    rustPrecheckPublish();
    /* A long command started from rustIoEventHandler() leaves the rest of
     * its batch in Rust's main-thread list, behind an eventfd that was
     * already drained. processEventsWhileBlocked() reaches it only here
     * (SCRIPT KILL from a connection in the same batch never ran). */
    int processed = rust_started ? rustIoDrainReady() : 0;
    if (!rust_stalled || listLength(rust_stalled) == 0) return processed;
    unsigned long n = listLength(rust_stalled);
    uint64_t *ids = zmalloc(sizeof(uint64_t) * n);
    listIter li;
    listNode *ln;
    unsigned long i = 0;
    listRewind(rust_stalled, &li);
    while ((ln = listNext(&li))) ids[i++] = (uint64_t)(uintptr_t)listNodeValue(ln);
    listEmpty(rust_stalled);

    for (i = 0; i < n; i++) {
        client *c = lookupClientByID(ids[i]);
        if (!c || !c->conn || !connIsExternal(c->conn)) continue;
        rustConnection *rc = (rustConnection *)c->conn;
        rc->stalled = false;
        if (!rc->h) continue;
        if (rustIoCanProgress(rc, c)) {
            rustIoDeliver(rc);
            processed++;
        } else if (vio_conn_pending_cmds(rc->h) || rc->pending_err > 0) {
            rustIoStall(rc, c);
        }
    }
    zfree(ids);
    return processed;
}

static int connRustHasPendingData(void) {
    if (rust_started && vio_main_has_ready()) return 1;
    if (!rust_stalled || listLength(rust_stalled) == 0) return 0;
    listIter li;
    listNode *ln;
    listRewind(rust_stalled, &li);
    while ((ln = listNext(&li))) {
        client *c = lookupClientByID((uint64_t)(uintptr_t)listNodeValue(ln));
        if (!c || !c->conn || !connIsExternal(c->conn)) continue;
        if (rustIoCanProgress((rustConnection *)c->conn, c)) return 1;
    }
    return 0;
}

/* ---------------------------------------------------------------------------
 * Transport ops for connection_external.c
 * ------------------------------------------------------------------------- */

static ssize_t rustExtWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    rustConnection *rc = (rustConnection *)conn;
    if (!rc->h) {
        errno = EBADF;
        return -1;
    }
    RUST_IO_TIMING_WRITEV(conn);
    return vio_conn_writev(rc->h, iov, iovcnt);
}

static void rustExtClose(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (!rc->h) return;
    /* Rust flushes what we already handed over, then closes the fd. */
    vio_conn_close(rc->h);
    vio_conn_release(rc->h);
    rc->h = NULL;
}

static void rustExtShutdown(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (rc->h) vio_conn_shutdown(rc->h);
}

static void rustExtWantWritable(connection *conn, int on) {
    rustConnection *rc = (rustConnection *)conn;
    if (rc->h) vio_conn_want_writable(rc->h, on);
}

/* unprotectClient(): input held in Rust can flow again. */
static void rustExtReadResumed(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    client *c = connGetPrivateData(conn);
    if (c && rc->h && vio_conn_pending_cmds(rc->h)) rustIoStall(rc, c);
}

static const externalConnOps rust_ext_ops = {
    .writev = rustExtWritev,
    .close = rustExtClose,
    .shutdown = rustExtShutdown,
    .want_writable = rustExtWantWritable,
    .read_resumed = rustExtReadResumed,
    .has_pending_data = connRustHasPendingData,
    .process_pending_data = connRustProcessPendingData,
};

/* ---------------------------------------------------------------------------
 * Startup
 * ------------------------------------------------------------------------- */

int rustIoTakesOverPort(void) {
    return rust_io_config.mode != RUST_IO_MODE_OFF && rust_io_config.takeover;
}

int rustIoInitListeners(void) {
    if (rust_io_config.mode == RUST_IO_MODE_OFF) return 0;
    int port = rust_io_config.takeover ? server.port : rust_io_config.port;
    if (port == 0) return 0;

    VioConfig cfg = {
        .bindaddr = server.bindaddr_count ? server.bindaddr[0] : NULL,
        .port = port,
        .threads = rust_io_config.threads,
        .proto_max_bulk_len = &server.proto_max_bulk_len,
        .client_max_querybuf_len = &server.client_max_querybuf_len,
        .cbs = {.create_argv = rustIoCreateArgv, .free_argv = rustIoFreeArgv},
    };
    rust_io_precheck_active = rust_io_config.mode == RUST_IO_MODE_PRECHECK;
    if (rust_io_precheck_active) {
        extern int (*rustPrecheckCmdFn)(void *, const VioSession *, int, VioCmd *);
        rustPrecheckInit();
        rustPrecheckPublish();
        cfg.cbs.precheck_begin = rustPrecheckBegin;
        cfg.cbs.precheck_end = rustPrecheckEnd;
        cfg.cbs.precheck_cmd = rustPrecheckCmdFn;
    }
    int efd = vio_start(&cfg);
    if (efd != -1)
        vio_set_default_auth_required(!(DefaultUser->flags & USER_FLAG_NOPASS) || (DefaultUser->flags & USER_FLAG_DISABLED));
    if (efd == -1) {
        serverLog(LL_WARNING, "Failed listening on rust-io port %d: %s, aborting.", port, strerror(errno));
        exit(1);
    }
    serverAssert(connExternalAttach(&rust_ext_ops) == C_OK);
    if (aeCreateFileEvent(server.el, efd, AE_READABLE, rustIoEventHandler, NULL) == AE_ERR)
        serverPanic("Unrecoverable error creating rust-io event handler.");
    rust_stalled = listCreate();
    rust_started = port;
    rust_bind_at_start = rustIoJoinBind();
    serverLog(LL_NOTICE, "Rust IO layer listening on %s:%d with %d thread(s)%s", cfg.bindaddr ? cfg.bindaddr : "*",
              port, rust_io_config.threads, rust_io_precheck_active ? ", precheck" : "");
    return 1;
}

/* End of beforeSleep(), also inside processEventsWhileBlocked() (sleeping
 * 0: the main thread does not block there). Returns 1 when the main thread
 * must poll without blocking: Rust handed it work while it was awake, and
 * skipped the eventfd for it. */
int rustIoBeforePoll(int sleeping) {
    if (!rust_started) return 0;
    vio_main_flush_kicks();
    return sleeping ? vio_main_before_poll() : 0;
}

/* End of afterSleep(), where rustIoEventHandler() would run next: take the
 * connections handed over while the main thread was awake (no eventfd was
 * written for them). Not earlier in beforeSleep(): running them there
 * delays the replies of the batch just executed, and a single IO thread
 * waits for those. */
void rustIoAfterPoll(void) {
    if (!rust_started) return;
    if (vio_main_after_poll()) rustIoDrainReady();
}

/* Shutdown: flushReplicasOutputBuffers() only hands replica output to Rust,
 * whose IO threads die with the process. Give them a bounded chance to
 * write it, so the last commands (e.g. the auto failover request) reach the
 * replicas as they would from a socket write. */
void rustIoFlushReplicas(long long timeout_ms) {
    if (!rust_started) return;
    vio_main_flush_kicks(); /* kick batching may still hold the replica kicks */
    long long deadline = mstime() + timeout_ms;
    while (mstime() < deadline) {
        size_t pending = 0;
        listIter li;
        listNode *ln;
        listRewind(server.replicas, &li);
        while ((ln = listNext(&li))) {
            client *r = listNodeValue(ln);
            if (!r->conn || !connIsExternal(r->conn)) continue;
            rustConnection *rc = (rustConnection *)r->conn;
            if (rc->h) pending += vio_conn_pending_out(rc->h);
        }
        if (pending == 0) return;
        usleep(1000);
    }
}

void rustIoLogReady(void) {
    if (rust_started) serverLog(LL_NOTICE, "Ready to accept connections rust");
}

#endif /* USE_RUST_IO */
