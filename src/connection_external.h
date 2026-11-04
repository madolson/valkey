/*
 * connection_external.h - a connection whose socket IO is done by an
 * external IO layer (today the Rust IO layer, src/rust/valkey-io).
 *
 * The engine sees an ordinary ConnectionType ("external"). Reads never
 * happen through it: the IO layer delivers parsed commands itself. Writes,
 * close, shutdown and handler changes are forwarded to the attached IO
 * layer through externalConnOps. The transport knows nothing about the IO
 * layer behind it.
 */
#ifndef CONNECTION_EXTERNAL_H
#define CONNECTION_EXTERNAL_H

#include <stddef.h>
#include <sys/uio.h>

struct connection;

typedef struct externalConnOps {
    /* Hand reply bytes to the IO layer. Returns bytes accepted, or -1 with
     * errno EAGAIN (IO layer full: keep the rest, wait for the write
     * handler) or another errno (connection is gone). */
    ssize_t (*writev)(struct connection *conn, const struct iovec *iov, int iovcnt);
    /* The engine is done with the connection: write what was accepted, then
     * close. The connection struct is freed by the transport afterwards. */
    void (*close)(struct connection *conn);
    /* A fork child shares the fd: shut the socket down. */
    void (*shutdown)(struct connection *conn);
    /* The engine installed (on=1) or removed its write handler. */
    void (*want_writable)(struct connection *conn, int on);
    /* The engine installed a read handler: input held in the IO layer may
     * flow again (unprotectClient()). */
    void (*read_resumed)(struct connection *conn);
    /* Start of a direct-IO window (connBlock(), a fork child will use the
     * fd): write what was accepted, then leave the fd alone. Blocks.
     * Returns 0, or -1 if the pending output could not be written. */
    int (*detach)(struct connection *conn);
    /* End of the window (connNonBlock()): resume IO on the fd. */
    void (*attach)(struct connection *conn);
    /* connSetPriority(): serve this connection ahead of the others. */
    void (*set_priority)(struct connection *conn, int on);
    /* beforeSleep() hooks, see ConnectionType. */
    int (*has_pending_data)(void);
    int (*process_pending_data)(void);
} externalConnOps;

/* Register the "external" ConnectionType backed by ops. Once, at startup. */
int connExternalAttach(const externalConnOps *ops);
/* A new accepted connection of `size` bytes (>= sizeof(connection)); the
 * IO layer's own state may follow the embedded connection. fd is the IO
 * layer's socket, used for addresses only. */
struct connection *connCreateExternal(size_t size, int fd);
int connIsExternal(struct connection *conn);
/* connBlock()/connNonBlock() of an external connection. While blocked
 * (detached), the engine does IO on the fd itself, as a socket connection
 * would: in the parent (synchronous Lua debugger, the dual-channel RDB
 * channel's handoff) and in a fork child, which has no IO layer threads. */
int connExternalBlock(struct connection *conn);
/* connSetPriority() of an external connection (it has no ae event). */
void connExternalSetPriority(struct connection *conn, int on);
int connExternalNonBlock(struct connection *conn);
/* Stop the IO layer's IO on the fd without changing its blocking mode: a
 * fork child is about to take the connection over. */
void connExternalDetach(struct connection *conn);

#endif /* CONNECTION_EXTERNAL_H */
