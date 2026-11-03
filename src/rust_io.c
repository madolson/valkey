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
#include "engine_io.h"
#include "memory_prefetch.h"
#include "io_threads.h"

#ifdef USE_RUST_IO

#include "rust/valkey-io/include/valkey_io.h"

/* VioCallbacks.precheck_batch, in rust_io_precheck.c. */
size_t rustPrecheckBatch(void *snap, const VioSession *sess, int barrier_active, VioCmd *cmds, size_t n);

/* VioCmd is engine_io.h's engineCmd. */
static_assert(sizeof(VioCmd) == sizeof(engineCmd), "VioCmd layout");
static_assert(offsetof(VioCmd, argv) == offsetof(engineCmd, argv), "VioCmd layout");
static_assert(offsetof(VioCmd, argc) == offsetof(engineCmd, argc), "VioCmd layout");
static_assert(offsetof(VioCmd, err) == offsetof(engineCmd, err), "VioCmd layout");
static_assert(offsetof(VioCmd, err_byte) == offsetof(engineCmd, err_byte), "VioCmd layout");
static_assert(offsetof(VioCmd, argv_len_sum) == offsetof(engineCmd, argv_len_sum), "VioCmd layout");
static_assert(offsetof(VioCmd, input_bytes) == offsetof(engineCmd, input_bytes), "VioCmd layout");
static_assert(offsetof(VioCmd, cmd) == offsetof(engineCmd, cmd), "VioCmd layout");
static_assert(offsetof(VioCmd, slot) == offsetof(engineCmd, slot), "VioCmd layout");
static_assert(offsetof(VioCmd, read_flags) == offsetof(engineCmd, read_flags), "VioCmd layout");
static_assert(offsetof(VioCmd, token) == offsetof(engineCmd, token), "VioCmd layout");
#ifdef RUST_IO_TIMING
static_assert(offsetof(VioCmd, trace) == offsetof(engineCmd, trace), "VioCmd layout");
#endif

/* Not exported through server.h. */
void trimCommandQueue(client *c);
int _writeToClient(client *c);

rustIoConfig rust_io_config = {RUST_IO_MODE_OFF, 0, 1, 0, -1};
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

typedef struct rustConnection {
    connection c;
    VioConn *h; /* our reference; NULL once closed */
    bool stalled;     /* in rust_stalled: commands wait in Rust */
    int pending_err;  /* VIO_ERR_* seen but not yet replied, -1 once replied */
    int pending_err_byte;
    unsigned long long pulled; /* commands moved into cmd_queue so far */
    VioSession sess;           /* session last written to Rust's view (precheck) */
    unsigned long long consumed_pub; /* consumed count last published */
    int auth_pub;                    /* auth_required last published, -1 none */
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

/* VioCallbacks.create_argvs: the argv of every command in one parsed batch,
 * in one call. Runs on a Rust IO thread. */
static void rustIoCreateArgvs(const unsigned char *const *ptrs, const size_t *lens, VioCmd *cmds, size_t n) {
    size_t k = 0;
    for (size_t i = 0; i < n; i++) {
        if (cmds[i].err) continue;
        int argc = cmds[i].argc;
        robj **argv = zmalloc(sizeof(robj *) * argc);
        for (int j = 0; j < argc; j++, k++) argv[j] = createStringObject((const char *)ptrs[k], lens[k]);
        cmds[i].argv = (void **)argv;
    }
}

/* Also frees argv the engine returned (vio_conn_return_argv): entries the
 * engine still referenced were already released and set to NULL. */
static void rustIoFreeArgv(void **argv, int argc) {
    for (int j = 0; j < argc; j++)
        if (argv[j]) decrRefCount(argv[j]);
    zfree(argv);
}

/* tryOffloadFreeArgvToIOThreads() for external connections: free the argv
 * on the IO thread that built it. Same rule as the C IO threads: objects
 * someone else still references (refcount > 1, e.g. a value SET stored)
 * are decremented here. */
int rustIoTryOffloadFreeArgv(client *c, int argc, robj **argv) {
    rustConnection *rc = (rustConnection *)c->conn;
    /* With one IO thread that thread is the bottleneck at high pipelining
     * (the main thread is about 50% idle at -P 16): freeing there cost 9%
     * GET throughput. Offload only when the IO threads have room. */
    if (!rc->h || argc == 0 || rust_io_config.threads < 2) return C_ERR;
    int keep = 0;
    for (int j = 0; j < argc; j++) {
        if (argv[j]->refcount > 1) {
            decrRefCount(argv[j]);
            argv[j] = NULL;
        } else {
            keep = 1;
        }
    }
    if (keep)
        vio_conn_return_argv(rc->h, (void **)argv, argc);
    else
        zfree(argv);
    return C_OK;
}

/* ---------------------------------------------------------------------------
 * Command handoff
 * ------------------------------------------------------------------------- */

static void rustIoStall(rustConnection *rc, client *c) {
    if (rc->stalled) return;
    rc->stalled = true;
    listAddNodeTail(rust_stalled, (void *)(uintptr_t)c->id);
}

/* Move parsed commands from Rust into c->cmd_queue (engineSubmit()). */
static int rustIoPull(rustConnection *rc, client *c) {
    VioCmd cmds[RUST_IO_POP_BATCH];
    unsigned long long bytes = 0;
    int pulled = 0;

    while (rc->pending_err == 0 && engineCmdQueuePending(c) < RUST_IO_CMD_QUEUE_MAX) {
        size_t want = RUST_IO_CMD_QUEUE_MAX - engineCmdQueuePending(c);
        if (want > RUST_IO_POP_BATCH) want = RUST_IO_POP_BATCH;
        size_t n = vio_conn_pop_cmds(rc->h, cmds, want);
        if (n == 0) break;
        pulled += engineSubmit(c, (const engineCmd *)cmds, n, &rc->pending_err, &rc->pending_err_byte, &bytes);
    }
    rc->pulled += pulled;
    if (pulled) engineAccountRead(c, bytes);
    return pulled;
}

const VioSession *rustIoConnSession(connection *conn) {
    if (!connIsExternal(conn)) return NULL;
    return &((rustConnection *)conn)->sess;
}

/* Publish what Rust reads about this client after a delivery: the parser's
 * auth limits, and in precheck mode the session view. The session fields go
 * into Rust's seqlock only when one changed (the generation moves with
 * them); 'consumed' lets Rust lift a barrier once the engine has run the
 * barrier command. Nothing crosses when nothing changed. */
static void rustIoPublish(rustConnection *rc, client *c) {
    if (!rc->h) return;
    /* Counted in io mode too: a delivery that popped commands is what
     * resumes a connection whose parse Rust held after one unauthenticated
     * command (vio_conn_publish() kicks it). */
    unsigned long long consumed = rc->pulled - engineCmdQueuePending(c);
    if (rust_io_precheck_active) {
        VioSession *s = &rc->sess;
        unsigned int flags = rustPrecheckSessionFlags(c);
        int dbid = c->db ? c->db->id : -1;
        if (s->gen == 0 || s->user != (void *)c->user || s->flags != flags || s->dbid != dbid) {
            s->user = c->user;
            s->flags = flags;
            s->dbid = dbid;
            s->gen++;
            vio_conn_set_session(rc->h, s);
        }
    }
    int auth = authRequired(c);
    if (consumed != rc->consumed_pub || auth != rc->auth_pub) {
        rc->consumed_pub = consumed;
        rc->auth_pub = auth;
        vio_conn_publish(rc->h, consumed, auth);
    }
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
    if (rc->pending_err == 0 && engineCmdQueuePending(c) < RUST_IO_CMD_QUEUE_MAX && vio_conn_pending_cmds(rc->h) > 0)
        return true;
    if (!c->flag.blocked && canParseCommand(c) && (engineCmdQueuePending(c) > 0 || rc->pending_err > 0)) return true;
    return false;
}

/* After a client's commands ran: the protocol error that ended its input,
 * the queue trim, and what Rust reads about the client. */
static void rustIoAfterCommands(rustConnection *rc, client *c) {
    if (rc->pending_err > 0 && engineCmdQueuePending(c) == 0 && canParseCommand(c)) {
        rustIoProtocolError(c, rc->pending_err, rc->pending_err_byte);
        rc->pending_err = -1;
    }
    trimCommandQueue(c);
    rustIoPublish(rc, c);
}

/* Pull what Rust parsed for this connection into its command queue. Returns
 * the client when it has commands to run now, NULL otherwise. */
static client *rustIoDeliverPull(rustConnection *rc) {
    VioConn *h = rc->h;
    client *c = connGetPrivateData(&rc->c);
    if (!h || !c) return NULL;
    if (c->flag.close_asap || c->flag.close_after_reply) return NULL;
    /* protectClient() removes the read handler: hold input in Rust. */
    if (!rc->c.read_handler) {
        if (vio_conn_pending_cmds(h)) rustIoStall(rc, c);
        return NULL;
    }

    rustIoPull(rc, c);
    if (rc->pending_err > 0 || vio_conn_pending_cmds(h)) rustIoStall(rc, c);

    if (c->flag.blocked) return NULL; /* resumed by processUnblockedClients() */
    return c;
}

/* Pull and execute what Rust parsed for this connection. Mirrors the tail
 * of readQueryFromClient(). The client may be freed. */
static void rustIoDeliver(rustConnection *rc) {
    client *c = rustIoDeliverPull(rc);
    if (!c) return;
    if (processInputBuffer(c) == C_ERR) return;
    rustIoAfterCommands(rc, c);
    /* May free the client (close_asap). */
    beforeNextClient(c);
}

/* Cross-connection batch (plan step 21), the C IO threads' scheme
 * (handleReadJobs(), processClientIOReadsDone()): the commands of every
 * ready connection go into the key-prefetch batch first, which prefetches
 * the keys of all of them and then runs them (processClientsCommandsBatch()
 * runs processPendingCommandAndInputBuffer() and beforeNextClient() per
 * client). The per-client tail runs afterwards, by client id, since a
 * client can be freed meanwhile. Returns 1 when the client went into the
 * batch, 0 when it was handled here. */
#define RUST_IO_BATCH_MAX_QUEUED 2

static int rustIoDeliverBatched(rustConnection *rc, uint64_t *id) {
    client *c = rustIoDeliverPull(rc);
    if (!c) return 0;
    *id = c->id;
    /* A deep queue (pipelining) is prefetched per client by
     * processInputBuffer() already; batching it measured -4% to -9% GET
     * RPS at -P 16. The batch pays off for shallow queues (-P 1: -8% to
     * -12% main-thread ns/request). */
    int queued = engineCmdQueuePending(c);
    if (queued == 0 || queued > RUST_IO_BATCH_MAX_QUEUED || addCommandToBatchAndProcessIfFull(c) == C_ERR) {
        if (processInputBuffer(c) == C_ERR) return 0;
        rustIoAfterCommands(rc, c);
        beforeNextClient(c);
        return 0;
    }
    return 1;
}

#define RUST_IO_DRAIN_BATCH 128
extern int ProcessingEventsWhileBlocked; /* networking.c */

/* Run the batch and each batched client's tail. */
static void rustIoFlushBatch(uint64_t *ids, int n) {
    processClientsCommandsBatch();
    for (int i = 0; i < n; i++) {
        client *c = lookupClientByID(ids[i]);
        if (!c || !c->conn || !connIsExternal(c->conn)) continue;
        rustIoAfterCommands((rustConnection *)c->conn, c);
    }
}

/* ---------------------------------------------------------------------------
 * Zero-copy replies (DESIGN.md "Zero-copy replies")
 *
 * The contract is the C IO threads' (trySendWriteToIOThreads(),
 * ioThreadWriteToClient(), processClientIOWriteDone()): the engine seals the
 * reply chunk by snapshotting io_last_reply_block/io_last_bufpos, marks the
 * client CLIENT_PENDING_IO and hands it to the connection's IO thread. That
 * thread builds the iovec with _writeToClient() and writes from engine
 * memory; the engine releases what was written (_postWriteToClient()) when
 * the job comes back. Every engine path that would touch an in-flight block
 * already checks io_write_state, and freeClient() defers while it is not
 * idle.
 * ------------------------------------------------------------------------- */

/* Set on a Rust IO thread while it runs a zero-copy write job: the
 * connection's writev goes to the socket directly. */
static _Thread_local int rust_direct_fd = -1;

/* Jobs handed to Rust and not yet returned (must be 0 when idle). */
static long long rust_writes_outstanding = 0;
static unsigned long long rust_writes_submitted = 0;

int rustIoTrySendWrite(client *c) {
    rustConnection *rc = (rustConnection *)c->conn;
    if (rust_io_config.zero_copy_min < 0 || !rc->h) return C_ERR;
    if (c->io_write_state != CLIENT_IDLE) return C_OK; /* already in flight */
    if (c->io_read_state != CLIENT_IDLE) return C_ERR;
    if (!clientHasPendingReplies(c)) return C_ERR;
    /* Replicas, primary and slot-migration links keep the copy path: their
     * output lives in the shared replication backlog blocks. */
    int type = getClientType(c);
    if ((type != CLIENT_TYPE_NORMAL && type != CLIENT_TYPE_PUBSUB) || c->flag.lua_debug) return C_ERR;
    /* Small replies are copied (the token round trip costs more than the
     * copy). Referenced bulk strings (buf_encoded) and reply blocks are
     * always large. */
    if (!c->flag.buf_encoded && listLength(c->reply) == 0 && (long long)c->bufpos < (long long)rust_io_config.zero_copy_min) return C_ERR;

    /* Seal: as trySendWriteToIOThreads() for a non-replica. */
    c->io_last_reply_block = listLast(c->reply);
    if (c->io_last_reply_block) {
        clientReplyBlock *block = (clientReplyBlock *)listNodeValue(c->io_last_reply_block);
        c->io_last_bufpos = block->used;
    } else {
        c->io_last_bufpos = (size_t)c->bufpos;
    }
    /* Unlike a C IO-thread write, the engine keeps executing this client's
     * pipelined commands while the job is in flight, and upsertPayloadHeader()
     * would extend the last payload header, which lies inside the sealed
     * chunk the IO thread is walking. Start new headers after the seal. */
    c->last_header = NULL;
    if (c->io_last_reply_block) ((clientReplyBlock *)listNodeValue(c->io_last_reply_block))->last_header = NULL;
    c->write_flags = 0;
    c->io_write_state = CLIENT_PENDING_IO;
    RUST_IO_TIMING_WRITEV(c->conn); /* stage "reply_copied": handed over */
    rust_writes_outstanding++;
    rust_writes_submitted++;
    vio_conn_submit_write(rc->h, c, 0);
    return C_OK;
}

/* Rust IO thread: write the sealed chunk (ioThreadWriteToClient()). */
static int rustIoWriteJob(void *job, int fd, int can_write) {
    client *c = job;
    int r = 0;
    c->nwritten = 0;
    if (can_write) {
        ioThreadsEnterExternal(1);
        rust_direct_fd = fd;
        _writeToClient(c);
        rust_direct_fd = -1;
        ioThreadsEnterExternal(0);
        if (!(c->write_flags & WRITE_FLAGS_WRITE_ERROR))
            r = 1;
        else
            r = connGetState(c->conn) == CONN_STATE_CONNECTED ? 0 : -1;
    } else {
        /* Not written: the engine re-arms its write handler. */
        c->write_flags |= WRITE_FLAGS_WRITE_ERROR;
    }
    atomic_thread_fence(memory_order_release);
    c->io_write_state = CLIENT_COMPLETED_IO;
    return r;
}

/* Engine thread: release what the IO thread wrote. */
static void rustIoDrainDone(void) {
    void *job;
    while ((job = vio_main_next_done()) != NULL) {
        client *c = job;
        rust_writes_outstanding--;
        serverAssert(rust_writes_outstanding >= 0 && c->io_write_state == CLIENT_COMPLETED_IO);
        processClientIOWriteDone(c);
    }
}

/* CONFIG SET proto-max-bulk-len / client-query-buffer-limit. */
void rustIoPublishLimits(void) {
    if (rust_started) vio_set_limits(server.proto_max_bulk_len, server.client_max_querybuf_len);
}

void rustIoFlushKicks(void) {
    if (rust_started) vio_main_flush_kicks();
}

sds rustIoCatInfo(sds info) {
    return sdscatprintf(info,
                        "rust_io_zero_copy_min_bytes:%d\r\n"
                        "rust_io_writes_submitted:%llu\r\n"
                        "rust_io_writes_outstanding:%lld\r\n",
                        rust_io_config.zero_copy_min, rust_writes_submitted, rust_writes_outstanding);
}

/* ---------------------------------------------------------------------------
 * Accept and readiness
 * ------------------------------------------------------------------------- */

static void rustIoAccept(VioConn *h) {
    rustConnection *rc = (rustConnection *)connCreateExternal(sizeof(rustConnection), vio_conn_fd(h));
    rc->h = vio_conn_retain(h);
    vio_conn_set_udata(h, rc);
    rc->auth_pub = -1;

    struct ClientFlags flags = {0};
    char cip[NET_IP_STR_LEN] = {0};
    int cport = 0;
    anetFdToString(rc->c.fd, cip, sizeof(cip), &cport, 1);
    serverLog(LL_VERBOSE, "Accepted %s:%d (rust-io)", cip, cport);
    if (server.tcpkeepalive) anetKeepAlive(NULL, rc->c.fd, server.tcpkeepalive);
    acceptCommonHandler(&rc->c, flags, cip);

    rc = vio_conn_udata(h);
    if (rc && connGetPrivateData(&rc->c)) {
        rustIoPublish(rc, connGetPrivateData(&rc->c));
    }
}

/* Returns 1 when the client's commands went into the prefetch batch. */
static int rustIoHandleReady(VioConn *h, uint64_t *id) {
    int batched = 0;
    uint32_t ev = vio_conn_take_events(h);
    if (ev & VIO_EV_NEW) rustIoAccept(h);

    rustConnection *rc = vio_conn_udata(h);
    if (!rc) return 0;
    if ((ev & VIO_EV_WRITABLE) && rc->c.write_handler) {
        if (!callHandler(&rc->c, rc->c.write_handler)) return 0;
    }

    if (ev & (VIO_EV_QBLIMIT | VIO_EV_CLOSED))
        rustIoDeliver(rc);
    else
        batched = rustIoDeliverBatched(rc, id);

    if (ev & VIO_EV_QBLIMIT) {
        rc = vio_conn_udata(h);
        if (!rc) return batched;
        client *c = connGetPrivateData(&rc->c);
        if (c) {
            /* handleQbLimitReached() dumps c->querybuf, which is NULL here. */
            sds ci = catClientInfoString(sdsempty(), c, server.hide_user_data_from_log);
            serverLog(LL_WARNING, "Closing client that reached max query buffer length: %s", ci);
            sdsfree(ci);
            freeClientAsync(c);
            server.stat_client_qbuf_limit_disconnections++;
        }
        return batched;
    }

    if (ev & VIO_EV_CLOSED) {
        rc = vio_conn_udata(h);
        if (!rc) return batched;
        rc->c.state = CONN_STATE_CLOSED;
        client *c = connGetPrivateData(&rc->c);
        if (c) {
            serverLog(LL_VERBOSE, "Client closed connection (rust-io) id=%llu", (unsigned long long)c->id);
            freeClientAsync(c);
        }
    }
    return batched;
}

static int rustIoDrainReady(void) {
    VioConn *h;
    int n = 0;
    uint64_t ids[RUST_IO_DRAIN_BATCH];
    int nids = 0;
    rustIoDrainDone();
    /* Nested in processEventsWhileBlocked(): run what is left of the outer
     * call's batch first, as processClientIOReadsDone() does. */
    if (ProcessingEventsWhileBlocked) processClientsCommandsBatch();
    while ((h = vio_main_next_ready()) != NULL) {
        uint64_t id;
        if (rustIoHandleReady(h, &id)) ids[nids++] = id;
        vio_conn_release(h);
        n++;
        if (nids == RUST_IO_DRAIN_BATCH) {
            rustIoFlushBatch(ids, nids);
            nids = 0;
        }
    }
    if (nids) rustIoFlushBatch(ids, nids);
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
    /* Admission control: the IO threads refuse past maxclients + slack. */
    vio_set_maxclients(server.maxclients);
    server.stat_rejected_conn += vio_take_rejected();
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
    if (rust_direct_fd >= 0) return writev(rust_direct_fd, iov, iovcnt);
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

static int rustExtDetach(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (!rc->h) return 0;
    /* A kick may still sit in the main thread's batch; the detach writes
     * Shared.out regardless, but keep the order simple. */
    vio_main_flush_kicks();
    return vio_conn_detach(rc->h);
}

static void rustExtAttach(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (rc->h) vio_conn_attach(rc->h);
}

static const externalConnOps rust_ext_ops = {
    .detach = rustExtDetach,
    .attach = rustExtAttach,
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
        .proto_max_bulk_len = server.proto_max_bulk_len,
        .client_max_querybuf_len = server.client_max_querybuf_len,
        .cbs = {.create_argvs = rustIoCreateArgvs, .free_argv = rustIoFreeArgv, .write_job = rustIoWriteJob},
    };
    rust_io_precheck_active = rust_io_config.mode == RUST_IO_MODE_PRECHECK;
    if (rust_io_precheck_active) {
        rustPrecheckInit();
        rustPrecheckPublish();
        cfg.cbs.precheck_begin = rustPrecheckBegin;
        cfg.cbs.precheck_end = rustPrecheckEnd;
        cfg.cbs.precheck_batch = rustPrecheckBatch;
    }
    int efd = vio_start(&cfg);
    if (efd != -1)
        vio_set_default_auth_required(!(DefaultUser->flags & USER_FLAG_NOPASS) || (DefaultUser->flags & USER_FLAG_DISABLED));
    if (efd == -1) {
        serverLog(LL_WARNING, "Failed listening on rust-io port %d: %s, aborting.", port, strerror(errno));
        exit(1);
    }
    serverAssert(connExternalAttach(&rust_ext_ops) == C_OK);
    /* The key-prefetch batch rustIoDrainReady() fills (only the C IO threads
     * create it otherwise). */
    prefetchCommandsBatchInit();
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
