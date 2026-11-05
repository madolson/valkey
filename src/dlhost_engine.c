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

static const dlhostOps *host_ops;
static int engine_idx;
static dlhostInbox *inbox;

typedef struct {
    connection c;
    void *hconn;
    void **slot;
    dlhostMsg *in_head, *in_tail; /* input not yet read by the client */
} hostConn;

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
        host_ops->msg_done(m);
        return;
    }
    hostConn *hc = s ? s : hcCreate(m);
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
    if (host_ops) host_ops->flush(engine_idx);
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
