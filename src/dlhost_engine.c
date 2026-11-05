/*
 * dlhost_engine.c - the engine side of the dlmopen host experiment.
 *
 * Compiled only into libvalkey-engine.so (VALKEY_DLHOST). The host owns the
 * client sockets and parses RESP to route each command by slot. It hands this
 * instance raw RESP bytes per connection through an MPSC inbox and wakes it
 * with an eventfd. Each host connection becomes one ordinary client here,
 * whose connection type reads from the host's bytes and writes replies back
 * to the host instead of to a socket.
 */
#ifdef VALKEY_DLHOST
#include "server.h"
#include "connhelpers.h"
#include "dlhost/dlhost.h"

#include <sys/eventfd.h>

#define DLHOST_SLOT_CLOSED ((void *)1)

void trimCommandQueue(client *c); /* networking.c */

static const dlhostOps *host_ops;
static int engine_idx;
static dlhostInbox *inbox;

typedef struct hostConn {
    connection c;
    void *hconn;
    void **slot;
    dlhostMsg *in_head, *in_tail; /* input not yet read by the client */
    /* Chained dispatch: sequenced messages not yet fully handed over, the
     * end of the run the client is executing, and the waiting list link. */
    dlhostMsg *run_head, *run_tail;
    int running;
    unsigned long long run_end;
    struct hostConn *wprev, *wnext;
    int on_wait_list;
} hostConn;

/* Connections whose current run blocked; checked in beforeSleep. */
static hostConn *wait_list;

static int hcGetType(void) {
    return CONN_TYPE_SOCKET;
}

static int hcAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    UNUSED(conn);
    UNUSED(remote);
    valkey_strlcpy(ip, "127.0.0.1", ip_len);
    if (port) *port = 0;
    return 0;
}

static int hcIsLocal(connection *conn) {
    UNUSED(conn);
    return 1;
}

static void hcShutdown(connection *conn) {
    UNUSED(conn);
}

static void freeParsedMsg(dlhostMsg *m) {
    if (m->parsed) { /* free what dlhostEngineParse() built and nobody took */
        parsedCommand *pc = m->parsed;
        for (int i = m->cur; i < m->nparsed; i++) {
            for (int j = 0; j < pc[i].argc; j++) decrRefCount(pc[i].argv[j]);
            zfree(pc[i].argv);
        }
        zfree(pc);
        m->parsed = NULL;
    }
    host_ops->msg_done(m);
}

static void waitListRemove(hostConn *hc) {
    if (!hc->on_wait_list) return;
    if (hc->wprev)
        hc->wprev->wnext = hc->wnext;
    else
        wait_list = hc->wnext;
    if (hc->wnext) hc->wnext->wprev = hc->wprev;
    hc->wprev = hc->wnext = NULL;
    hc->on_wait_list = 0;
}

static void waitListAdd(hostConn *hc) {
    if (hc->on_wait_list) return;
    hc->wprev = NULL;
    hc->wnext = wait_list;
    if (wait_list) wait_list->wprev = hc;
    wait_list = hc;
    hc->on_wait_list = 1;
}

static void hcClose(connection *conn) {
    hostConn *hc = (hostConn *)conn;
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }
    while (hc->in_head) {
        dlhostMsg *m = hc->in_head;
        hc->in_head = m->next;
        host_ops->msg_done(m);
    }
    while (hc->run_head) {
        dlhostMsg *m = hc->run_head;
        hc->run_head = m->next;
        freeParsedMsg(m);
    }
    waitListRemove(hc);
    int by_engine = *hc->slot != DLHOST_SLOT_CLOSED;
    *hc->slot = DLHOST_SLOT_CLOSED;
    host_ops->engine_closed(hc->hconn, engine_idx, by_engine);
    zfree(hc);
}

static int hcAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    conn->state = CONN_STATE_CONNECTED;
    if (!callHandler(conn, accept_handler)) return C_ERR;
    return C_OK;
}

static int hcWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    hostConn *hc = (hostConn *)conn;
    ssize_t n = 0;
    for (int i = 0; i < iovcnt; i++) n += iov[i].iov_len;
    host_ops->reply(hc->hconn, engine_idx, iov, iovcnt);
    return (int)n;
}

static int hcWrite(connection *conn, const void *data, size_t len) {
    struct iovec iov = {.iov_base = (void *)data, .iov_len = len};
    return hcWritev(conn, &iov, 1);
}

static int hcRead(connection *conn, void *buf, size_t len) {
    hostConn *hc = (hostConn *)conn;
    size_t done = 0;
    while (hc->in_head && done < len) {
        dlhostMsg *m = hc->in_head;
        size_t n = m->len - m->pos;
        if (n > len - done) n = len - done;
        memcpy((char *)buf + done, m->data + m->pos, n);
        m->pos += n;
        done += n;
        if (m->pos == m->len) {
            hc->in_head = m->next;
            if (!hc->in_head) hc->in_tail = NULL;
            host_ops->msg_done(m);
        }
    }
    if (done == 0) {
        errno = EAGAIN;
        return -1;
    }
    return (int)done;
}

/* Feed queued input to the client until it is consumed or the client stops
 * reading (read handler removed, or close scheduled). */
static void hcDrain(hostConn *hc) {
    connection *conn = &hc->c;
    while (hc->in_head && conn->read_handler && conn->state == CONN_STATE_CONNECTED) {
        if (!callHandler(conn, conn->read_handler)) return;
    }
}

static int hcSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    /* Writes always complete, so a pending write handler can run now. */
    return C_OK;
}

static int hcSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    conn->read_handler = func;
    return C_OK;
}

static const char *hcGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static ssize_t hcSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return hcWrite(conn, ptr, size);
}

static ssize_t hcSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return hcRead(conn, ptr, size);
}

static int hcIsClosing(connection *conn) {
    return conn->state != CONN_STATE_CONNECTED;
}

static ConnectionType CT_Host = {
    .get_type = hcGetType,
    .addr = hcAddr,
    .is_local = hcIsLocal,
    .shutdown = hcShutdown,
    .close = hcClose,
    .accept = hcAccept,
    .write = hcWrite,
    .writev = hcWritev,
    .read = hcRead,
    .set_write_handler = hcSetWriteHandler,
    .set_read_handler = hcSetReadHandler,
    .get_last_error = hcGetLastError,
    .sync_write = hcSyncWrite,
    .sync_read = hcSyncRead,
    .sync_readline = hcSyncRead,
    .is_closing = hcIsClosing,
};

static hostConn *hcCreate(dlhostMsg *m) {
    hostConn *hc = zcalloc(sizeof(*hc));
    hc->c.type = &CT_Host;
    hc->c.fd = -1;
    hc->c.iovcnt = IOV_MAX;
    hc->c.state = CONN_STATE_CONNECTED;
    hc->hconn = m->hconn;
    hc->slot = m->engine_slot;
    host_ops->engine_ref(m->hconn);
    client *c = createClient(&hc->c);
    UNUSED(c);
    *hc->slot = hc;
    return hc;
}

/* Parse on the IO thread, the way the engine's own IO threads run
 * parseMultibulk off the main thread. Input is complete and well formed: the
 * host checked it. */
void *dlhostEngineParse(const struct iovec *cmds, int n) {
    parsedCommand *pc = zmalloc(sizeof(*pc) * n);
    for (int i = 0; i < n; i++) {
        const char *p = cmds[i].iov_base;
        parsedCommand *x = &pc[i];
        memset(x, 0, sizeof(*x));
        x->argc = (int)strtol(p + 1, NULL, 10);
        x->argv_len = x->argc;
        x->argv = zmalloc(sizeof(robj *) * x->argc);
        p = strchr(p, '\n') + 1;
        for (int j = 0; j < x->argc; j++) {
            long len = strtol(p + 1, NULL, 10);
            p = strchr(p, '\n') + 1;
            x->argv[j] = createStringObject(p, len);
            x->argv_len_sum += len;
            p += len + 2;
        }
        x->read_flags = READ_FLAGS_PARSING_COMPLETED;
        x->slot = -1;
        /* As prepareCommandGeneric() (server.c), which IO threads also run. */
        if (x->argc) {
            x->cmd = lookupCommand(x->argv, x->argc);
            if (!x->cmd)
                x->read_flags |= READ_FLAGS_COMMAND_NOT_FOUND;
            else if (!commandCheckArity(x->cmd, x->argc, NULL))
                x->read_flags |= READ_FLAGS_BAD_ARITY;
        }
        x->input_bytes = cmds[i].iov_len;
    }
    return pc;
}

static void queueCommands(client *c, parsedCommand *pc, int n) {
    cmdQueue *q = &c->cmd_queue;
    serverAssert(q->len + n < 65535);
    if (q->len + n > q->cap) {
        q->cap = q->len + n;
        q->cmds = zrealloc(q->cmds, sizeof(parsedCommand) * q->cap);
    }
    memcpy(q->cmds + q->len, pc, sizeof(parsedCommand) * n);
    q->len += n;
}

/* Run the client's queue. Returns C_ERR if the client was freed. */
static int runQueue(client *c) {
    if (processInputBuffer(c) == C_ERR) return C_ERR;
    trimCommandQueue(c);
    beforeNextClient(c);
    return C_OK;
}

/* Append pre-parsed commands to the client's command queue and run them;
 * processInputBuffer() consumes the queue, and resumes it after a block. */
static void handleParsed(hostConn *hc, dlhostMsg *m) {
    client *c = connGetPrivateData(&hc->c);
    queueCommands(c, m->parsed, m->nparsed);
    zfree(m->parsed);
    m->parsed = NULL;
    host_ops->msg_done(m);
    runQueue(c);
}

/* Every command handed to the client has executed: nothing queued, not
 * blocked or about to resume. A client closing after QUIT never finishes, so
 * nothing later on the connection runs. */
static int runFinished(client *c) {
    return c->cmd_queue.off >= c->cmd_queue.len && !c->flag.blocked && !c->flag.unblocked &&
           !c->flag.pending_command && !c->flag.close_after_reply && !c->flag.close_asap && c->cmd == NULL;
}

/* Chained dispatch: hand the client the next run of consecutive sequence
 * numbers once the command before it has executed on whichever engine owns
 * it, and tell the host when the run has executed. */
static void pumpRuns(hostConn *hc) {
    client *c = connGetPrivateData(&hc->c);
    for (;;) {
        if (hc->running) {
            if (!runFinished(c)) {
                waitListAdd(hc);
                return;
            }
            hc->running = 0;
            waitListRemove(hc);
            host_ops->seq_done(hc->hconn, engine_idx, hc->run_end);
        }
        dlhostMsg *m = hc->run_head;
        if (!m) return;
        unsigned long long s = m->seqs[m->cur];
        if (!host_ops->seq_ready(hc->hconn, engine_idx, s)) return; /* a WAKE follows */
        int k = 1;
        while (m->cur + k < m->nparsed && m->seqs[m->cur + k] == s + k) k++;
        queueCommands(c, (parsedCommand *)m->parsed + m->cur, k);
        m->cur += k;
        if (m->cur == m->nparsed) {
            hc->run_head = m->next;
            if (!hc->run_head) hc->run_tail = NULL;
            zfree(m->parsed);
            m->parsed = NULL;
            host_ops->msg_done(m);
        }
        hc->running = 1;
        hc->run_end = s + k;
        if (runQueue(c) == C_ERR) return;
    }
}

static void handleMsg(dlhostMsg *m) {
    void *s = *m->engine_slot;
    if (m->type == DLHOST_MSG_CLOSE) {
        if (s && s != DLHOST_SLOT_CLOSED) {
            hostConn *hc = s;
            *hc->slot = DLHOST_SLOT_CLOSED;
            freeClient(connGetPrivateData(&hc->c));
        }
        host_ops->msg_done(m);
        return;
    }
    if (s == DLHOST_SLOT_CLOSED) {
        freeParsedMsg(m);
        return;
    }
    if (m->type == DLHOST_MSG_WAKE) {
        host_ops->msg_done(m);
        if (s) pumpRuns(s);
        return;
    }
    hostConn *hc = s ? s : hcCreate(m);
    if (m->type == DLHOST_MSG_PARSED && m->seqs) {
        m->next = NULL;
        if (hc->run_tail)
            hc->run_tail->next = m;
        else
            hc->run_head = m;
        hc->run_tail = m;
        pumpRuns(hc);
        return;
    }
    if (m->type == DLHOST_MSG_PARSED) {
        handleParsed(hc, m);
        return;
    }
    m->next = NULL;
    if (hc->in_tail)
        hc->in_tail->next = m;
    else
        hc->in_head = m;
    hc->in_tail = m;
    hcDrain(hc);
}

static void inboxHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);
    uint64_t v;
    if (read(fd, &v, sizeof(v)) < 0) { /* EAGAIN: spurious wake */
    }
    dlhostMsg *m = atomic_exchange_explicit(&inbox->head, NULL, memory_order_acquire);
    /* The inbox is a stack; reverse it to restore push order. */
    dlhostMsg *rev = NULL;
    while (m) {
        dlhostMsg *next = m->next;
        m->next = rev;
        rev = m;
        m = next;
    }
    while (rev) {
        dlhostMsg *next = rev->next;
        handleMsg(rev);
        rev = next;
    }
}

void dlhostEngineSetup(const dlhostOps *ops, int idx, dlhostInbox *ib) {
    host_ops = ops;
    engine_idx = idx;
    inbox = ib;
}

void dlhostEngineBeforeSleep(void) {
    if (!host_ops) return;
    /* Runs that blocked and have since finished (unblocked clients resumed
     * their queues earlier in beforeSleep). Advance their sequence; resume
     * any later run of theirs in the next iteration, not here, where its
     * replies would wait for the next wakeup. */
    hostConn *hc = wait_list;
    while (hc) {
        hostConn *next = hc->wnext;
        client *c = connGetPrivateData(&hc->c);
        if (runFinished(c)) {
            hc->running = 0;
            waitListRemove(hc);
            host_ops->seq_done(hc->hconn, engine_idx, hc->run_end);
            if (hc->run_head) host_ops->self_wake(hc->hconn, engine_idx);
        }
        hc = next;
    }
    host_ops->flush(engine_idx);
}

/* Called from main() right before aeMain(). */
void dlhostEngineReady(void) {
    if (!host_ops) return;
    if (aeCreateFileEvent(server.el, inbox->wake_fd, AE_READABLE, inboxHandler, NULL) == AE_ERR) {
        serverLog(LL_WARNING, "dlhost: cannot register inbox eventfd");
        exit(1);
    }
    serverLog(LL_NOTICE, "dlhost: engine %d ready", engine_idx);
    host_ops->ready(engine_idx);
}
#endif
