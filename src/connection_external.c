/*
 * connection_external.c - ConnectionType "external": a connection whose
 * socket IO is done by an external IO layer. See connection_external.h.
 */
#include "server.h"
#include "connhelpers.h"
#include "connection_external.h"

static ConnectionType CT_External;
static const externalConnOps *ext_ops;

static int connExternalGetType(void) {
    return CONN_TYPE_EXTERNAL;
}

static int connExternalAddr(connection *conn, char *ip, size_t ip_len, int *port, int remote) {
    if (anetFdToString(conn->fd, ip, ip_len, port, remote) == 0) return 0;
    conn->last_errno = errno;
    return -1;
}

static int connExternalIsLocal(connection *conn) {
    char cip[NET_IP_STR_LEN + 1] = {0};
    if (connExternalAddr(conn, cip, sizeof(cip) - 1, NULL, 1) == -1) return -1;
    return !strncmp(cip, "127.", 4) || !strcmp(cip, "::1");
}

static void connExternalShutdown(connection *conn) {
    ext_ops->shutdown(conn);
}

static void connExternalClose(connection *conn) {
    ext_ops->close(conn);
    if (connHasRefs(conn)) {
        conn->flags |= CONN_FLAG_CLOSE_SCHEDULED;
        return;
    }
    zfree(conn);
}

static int connExternalAccept(connection *conn, ConnectionCallbackFunc accept_handler) {
    if (conn->state != CONN_STATE_ACCEPTING) return C_ERR;
    conn->state = CONN_STATE_CONNECTED;
    if (!callHandler(conn, accept_handler)) return C_ERR;
    return C_OK;
}

static int connExternalWritev(connection *conn, const struct iovec *iov, int iovcnt) {
    ssize_t n = ext_ops->writev(conn, iov, iovcnt);
    if (n < 0 && errno != EAGAIN) {
        conn->last_errno = errno;
        if (conn->state == CONN_STATE_CONNECTED) conn->state = CONN_STATE_ERROR;
    }
    return (int)n;
}

static int connExternalWrite(connection *conn, const void *data, size_t data_len) {
    struct iovec iov = {.iov_base = (void *)data, .iov_len = data_len};
    return connExternalWritev(conn, &iov, 1);
}

/* Input arrives from the IO layer as parsed commands, never through read(). */
static int connExternalRead(connection *conn, void *buf, size_t buf_len) {
    UNUSED(conn);
    UNUSED(buf);
    UNUSED(buf_len);
    errno = EAGAIN;
    return -1;
}

static int connExternalSetWriteHandler(connection *conn, ConnectionCallbackFunc func, int barrier) {
    conn->write_handler = func;
    if (barrier)
        conn->flags |= CONN_FLAG_WRITE_BARRIER;
    else
        conn->flags &= ~CONN_FLAG_WRITE_BARRIER;
    ext_ops->want_writable(conn, func != NULL);
    return C_OK;
}

static int connExternalSetReadHandler(connection *conn, ConnectionCallbackFunc func) {
    conn->read_handler = func;
    if (func) ext_ops->read_resumed(conn);
    return C_OK;
}

static const char *connExternalGetLastError(connection *conn) {
    return strerror(conn->last_errno);
}

static ssize_t connExternalSyncWrite(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connExternalWrite(conn, ptr, size);
}

static ssize_t connExternalSyncRead(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connExternalRead(conn, ptr, size);
}

static ssize_t connExternalSyncReadLine(connection *conn, char *ptr, ssize_t size, long long timeout) {
    UNUSED(timeout);
    return connExternalRead(conn, ptr, size);
}

static int connExternalIsClosing(connection *conn) {
    return conn->state != CONN_STATE_CONNECTED;
}

static int connExternalHasPendingData(void) {
    return ext_ops->has_pending_data();
}

static int connExternalProcessPendingData(void) {
    return ext_ops->process_pending_data();
}

static ConnectionType CT_External = {
    .get_type = connExternalGetType,
    .init = NULL,
    .cleanup = NULL,
    .configure = NULL,
    .ae_handler = NULL,
    .accept_handler = NULL,
    .addr = connExternalAddr,
    .is_local = connExternalIsLocal,
    .listen = NULL,
    .closeListener = NULL,
    .conn_create = NULL,
    .conn_create_accepted = NULL,
    .shutdown = connExternalShutdown,
    .close = connExternalClose,
    .connect = NULL,
    .blocking_connect = NULL,
    .accept = connExternalAccept,
    .write = connExternalWrite,
    .writev = connExternalWritev,
    .read = connExternalRead,
    .set_write_handler = connExternalSetWriteHandler,
    .set_read_handler = connExternalSetReadHandler,
    .get_last_error = connExternalGetLastError,
    .sync_write = connExternalSyncWrite,
    .sync_read = connExternalSyncRead,
    .sync_readline = connExternalSyncReadLine,
    .has_pending_data = connExternalHasPendingData,
    .process_pending_data = connExternalProcessPendingData,
    .postpone_update_state = NULL,
    .update_state = NULL,
    .get_peer_cert = NULL,
    .get_peer_user = NULL,
    .connIntegrityChecked = NULL,
    .is_closing = connExternalIsClosing,
};

int connExternalAttach(const externalConnOps *ops) {
    serverAssert(ext_ops == NULL);
    ext_ops = ops;
    return connTypeRegister(&CT_External);
}

connection *connCreateExternal(size_t size, int fd) {
    serverAssert(ext_ops != NULL && size >= sizeof(connection));
    connection *conn = zcalloc(size);
    conn->type = &CT_External;
    conn->fd = fd;
    conn->iovcnt = IOV_MAX;
    conn->state = CONN_STATE_ACCEPTING;
    return conn;
}

int connIsExternal(connection *conn) {
    return conn && conn->type == &CT_External;
}
