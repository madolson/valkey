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

#ifdef USE_RUST_IO

#include "rust/valkey-io/include/valkey_io.h"

/* Not exported through server.h. */
void trimCommandQueue(client *c);

rustIoConfig rust_io_config = {RUST_IO_MODE_OFF, 0, 1, 0, RUST_IO_EXEC_HANDOFF};

configEnum rust_io_mode_enum[] = {
    {"off", RUST_IO_MODE_OFF},
    {"io", RUST_IO_MODE_IO},
    {"precheck", RUST_IO_MODE_PRECHECK},
    {"exec", RUST_IO_MODE_EXEC},
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
} rustConnection;

static ConnectionType CT_Rust;
static list *rust_stalled; /* client ids */
static int rust_started = 0;

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
            prepareCommandGeneric(p->argv, p->argc, &p->read_flags, &p->cmd, &p->slot);
            bytes += cmds[i].input_bytes;
            pulled++;
        }
    }

    if (pulled) {
        /* What handleReadResult() accounts for a socket read. */
        server.stat_total_reads_processed++;
        server.stat_net_input_bytes += bytes;
        c->net_input_bytes += bytes;
        c->last_interaction = server.unixtime;
    }
    return pulled;
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
    /* May free the client (close_asap). */
    beforeNextClient(c);
}

/* ---------------------------------------------------------------------------
 * Accept and readiness
 * ------------------------------------------------------------------------- */

static void rustIoAccept(const VioConn *h) {
    rustConnection *rc = zcalloc(sizeof(*rc));
    rc->c.type = &CT_Rust;
    rc->c.fd = vio_conn_fd(h);
    rc->c.iovcnt = IOV_MAX;
    rc->c.state = CONN_STATE_ACCEPTING;
    rc->h = h;
    vio_conn_retain(h);
    vio_conn_set_udata(h, rc);

    struct ClientFlags flags = {0};
    char cip[NET_IP_STR_LEN] = {0};
    int cport = 0;
    anetFdToString(rc->c.fd, cip, sizeof(cip), &cport, 1);
    serverLog(LL_VERBOSE, "Accepted %s:%d (rust-io)", cip, cport);
    if (server.tcpkeepalive) anetKeepAlive(NULL, rc->c.fd, server.tcpkeepalive);
    acceptCommonHandler(&rc->c, flags, cip);

    rc = vio_conn_udata(h);
    if (rc && connGetPrivateData(&rc->c)) vio_conn_set_auth_required(h, authRequired(connGetPrivateData(&rc->c)));
}

/* Also the exec-rtc entry point, on a Rust IO thread (rust_io_exec.c). */
void rustIoHandleReady(const VioConn *h) {
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

static void rustIoEventHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);
    vio_main_begin_drain();
    const VioConn *h;
    while ((h = vio_main_next_ready()) != NULL) {
        rustIoHandleReady(h);
        vio_conn_release(h);
    }
}

/* Clients whose input waits in Rust (cmd_queue full, blocked, protected).
 * Called from beforeSleep() through connTypeProcessPendingData(). */
static int connRustProcessPendingData(void) {
    if (!rust_stalled || listLength(rust_stalled) == 0) return 0;
    int processed = 0;
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
        if (!c || !c->conn || !connIsRust(c->conn)) continue;
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
    if (!rust_stalled || listLength(rust_stalled) == 0) return 0;
    listIter li;
    listNode *ln;
    listRewind(rust_stalled, &li);
    while ((ln = listNext(&li))) {
        client *c = lookupClientByID((uint64_t)(uintptr_t)listNodeValue(ln));
        if (!c || !c->conn || !connIsRust(c->conn)) continue;
        if (rustIoCanProgress((rustConnection *)c->conn, c)) return 1;
    }
    return 0;
}

/* ---------------------------------------------------------------------------
 * ConnectionType "rust"
 * ------------------------------------------------------------------------- */

static int connRustGetType(void) {
    return CONN_TYPE_RUST;
}

static int connRustAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    if (anetFdToString(conn->fd, ip, ip_len, port, remote) == 0) return 0;
    conn->last_errno = errno;
    return -1;
}

static int connRustIsLocal(connection *conn) {
    char cip[NET_IP_STR_LEN + 1] = {0};
    if (connRustAddr(conn, cip, sizeof(cip) - 1, NULL, 1) == -1) return -1;
    return !strncmp(cip, "127.", 4) || !strcmp(cip, "::1");
}

static void connRustShutdown(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (rc->h) vio_conn_shutdown(rc->h);
}

static void connRustClose(connection *conn) {
    rustConnection *rc = (rustConnection *)conn;
    if (rc->h) {
        /* Rust flushes what we already handed over, then closes the fd. */
        vio_conn_close(rc->h);
        vio_conn_release(rc->h);
        rc->h = NULL;
    }
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }
    zfree(rc);
}

static int connRustAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;
    if (!callHandler(conn, accept_handler)) return C_ERR;
    return C_OK;
}

static int connRustWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    rustConnection *rc = (rustConnection *)conn;
    ssize_t n;
    if (!rc->h) {
        errno = EBADF;
        n = -1;
    } else {
        n = vio_conn_writev(rc->h, iov, iovcnt);
    }
    if (n < 0 && errno != EAGAIN) {
        conn->last_errno = errno;
        if (conn->state == CONN_STATE_CONNECTED) conn->state = CONN_STATE_ERROR;
    }
    return (int)n;
}

static int connRustWrite(connection *conn, const void *data, size_t data_len) {
    struct iovec iov = {.iov_base = (void *)data, .iov_len = data_len};
    return connRustWritev(conn, &iov, 1);
}

/* Input arrives through rustIoDeliver(), never through read(). */
static int connRustRead(connection *conn, void *buf, size_t buf_len) {
    UNUSED(conn);
    UNUSED(buf);
    UNUSED(buf_len);
    errno = EAGAIN;
    return -1;
}

static int connRustSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    rustConnection *rc = (rustConnection *)conn;
    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    if (rc->h) vio_conn_want_writable(rc->h, func != NULL);
    return C_OK;
}

static int connRustSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    rustConnection *rc = (rustConnection *)conn;
    conn->read_handler = func;
    /* unprotectClient(): input held in Rust can flow again. */
    client *c = connGetPrivateData(conn);
    if (func && c && rc->h && vio_conn_pending_cmds(rc->h)) rustIoStall(rc, c);
    return C_OK;
}

static const char *connRustGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static ssize_t connRustSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connRustWrite(conn, ptr, size);
}

static ssize_t connRustSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connRustRead(conn, ptr, size);
}

static ssize_t connRustSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connRustRead(conn, ptr, size);
}

static int connRustIsClosing(connection *conn) {
    return conn->state != CONN_STATE_CONNECTED;
}

static ConnectionType CT_Rust = {
    .get_type = connRustGetType,
    .init = NULL,
    .cleanup = NULL,
    .configure = NULL,
    .ae_handler = NULL,
    .accept_handler = NULL,
    .addr = connRustAddr,
    .is_local = connRustIsLocal,
    .listen = NULL,
    .closeListener = NULL,
    .conn_create = NULL,
    .conn_create_accepted = NULL,
    .shutdown = connRustShutdown,
    .close = connRustClose,
    .connect = NULL,
    .blocking_connect = NULL,
    .accept = connRustAccept,
    .write = connRustWrite,
    .writev = connRustWritev,
    .read = connRustRead,
    .set_write_handler = connRustSetWriteHandler,
    .set_read_handler = connRustSetReadHandler,
    .get_last_error = connRustGetLastError,
    .sync_write = connRustSyncWrite,
    .sync_read = connRustSyncRead,
    .sync_readline = connRustSyncReadLine,
    .has_pending_data = connRustHasPendingData,
    .process_pending_data = connRustProcessPendingData,
    .postpone_update_state = NULL,
    .update_state = NULL,
    .get_peer_cert = NULL,
    .get_peer_user = NULL,
    .connIntegrityChecked = NULL,
    .is_closing = connRustIsClosing,
};

int RegisterConnectionTypeRust(void) {
    return connTypeRegister(&CT_Rust);
}

int connIsRust(connection *conn) {
    return conn && conn->type == &CT_Rust;
}

/* ---------------------------------------------------------------------------
 * Startup
 * ------------------------------------------------------------------------- */

int rustIoTakesOverPort(void) {
    return rust_io_config.mode != RUST_IO_MODE_OFF && rust_io_config.takeover;
}

int rustIoInitListeners(void) {
    if (rust_io_config.mode == RUST_IO_MODE_OFF) return 0;
    if (rust_io_config.mode == RUST_IO_MODE_PRECHECK) {
        serverLog(LL_WARNING, "rust-io-mode precheck is not implemented yet, running as 'io'");
    }
    int port = rust_io_config.takeover ? server.port : rust_io_config.port;
    if (port == 0) return 0;
    if (rust_io_config.mode == RUST_IO_MODE_EXEC && rustIoExecInit() != 0) serverPanic("rust-io exec init failed");

    VioConfig cfg = {
        .bindaddr = server.bindaddr_count ? server.bindaddr[0] : NULL,
        .port = port,
        .threads = rust_io_config.threads,
        .proto_max_bulk_len = &server.proto_max_bulk_len,
        .client_max_querybuf_len = &server.client_max_querybuf_len,
        .cbs = {.create_argv = rustIoCreateArgv, .free_argv = rustIoFreeArgv},
    };
    int efd = vio_start(&cfg);
    if (efd == -1) {
        serverLog(LL_WARNING, "Failed listening on rust-io port %d: %s, aborting.", port, strerror(errno));
        exit(1);
    }
    if (aeCreateFileEvent(server.el, efd, AE_READABLE, rustIoEventHandler, NULL) == AE_ERR)
        serverPanic("Unrecoverable error creating rust-io event handler.");
    rust_stalled = listCreate();
    rust_started = port;
    serverLog(LL_NOTICE, "Rust IO layer listening on %s:%d with %d thread(s)%s",
              cfg.bindaddr ? cfg.bindaddr : "*", port, rust_io_config.threads,
              rust_io_config.mode != RUST_IO_MODE_EXEC                ? ""
              : rust_io_config.exec_model == RUST_IO_EXEC_RTC ? ", exec-rtc"
                                                                      : ", exec-handoff");
    return 1;
}

void rustIoLogReady(void) {
    if (rust_started) serverLog(LL_NOTICE, "Ready to accept connections rust");
}

/* STAGE 3 HOOK: in exec mode Rust would invoke cmd->proc itself, through a
 * call()-equivalent entry point exported from here, holding the module GIL
 * (moduleAcquireGIL()/moduleReleaseGIL(), src/module.c) the way the main
 * thread releases it around aeApiPoll in beforeSleep()/afterSleep(). */

#endif /* USE_RUST_IO */
