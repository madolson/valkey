/*
 * valkey_io.h - C view of the symbols exported by the valkey-io Rust crate.
 *
 * Hand written; keep in sync with src/rust/valkey-io/src/lib.rs. The only
 * consumer is src/rust_io.c. See DESIGN.md next to this directory.
 */
#ifndef VALKEY_IO_H
#define VALKEY_IO_H

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/uio.h>

/* Opaque, reference counted connection handle owned by Rust. */
typedef struct VioConn VioConn;

/* Engine callbacks, invoked on Rust IO threads. */
typedef struct VioCallbacks {
    /* Build a robj** of argc string objects copied from ptrs[i]/lens[i]. */
    void **(*create_argv)(const unsigned char *const *ptrs, const size_t *lens, int argc);
    /* Free an argv from create_argv that never reached the engine. */
    void (*free_argv)(void **argv, int argc);
} VioCallbacks;

typedef struct VioConfig {
    const char *bindaddr;             /* NULL, "*" or an address; leading '-' ignored */
    int port;
    int threads;
    const long long *proto_max_bulk_len; /* read live by IO threads */
    const size_t *client_max_querybuf_len; /* read live by IO threads */
    VioCallbacks cbs;
} VioConfig;

/* Protocol error codes in VioCmd.err (mirrors READ_FLAGS_ERROR_*). */
enum {
    VIO_ERR_NONE = 0,
    VIO_ERR_BIG_INLINE = 1,
    VIO_ERR_BIG_MULTIBULK = 2,
    VIO_ERR_INVALID_MULTIBULK_LEN = 3,
    VIO_ERR_UNAUTH_MULTIBULK_LEN = 4,
    VIO_ERR_UNAUTH_BULK_LEN = 5,
    VIO_ERR_BIG_BULK_COUNT = 6,
    VIO_ERR_UNEXPECTED_CHAR = 7,
    VIO_ERR_INVALID_BULK_LEN = 8,
    VIO_ERR_UNBALANCED_QUOTES = 9,
    VIO_ERR_NUL_IN_INLINE = 10,
    VIO_ERR_INVALID_CRLF = 11,
};

typedef struct VioCmd {
    void **argv; /* robj** from create_argv; NULL when err != 0 */
    int argc;
    int err;      /* VIO_ERR_*; the error is always the last item */
    int err_byte; /* offending byte for VIO_ERR_UNEXPECTED_CHAR */
    size_t argv_len_sum;
    uint64_t input_bytes;
} VioCmd;

/* Event bits returned by vio_conn_take_events(). */
#define VIO_EV_NEW (1u << 0)      /* accepted; engine must adopt or close it */
#define VIO_EV_CLOSED (1u << 1)   /* peer closed or socket error */
#define VIO_EV_WRITABLE (1u << 2) /* outbound buffer drained (after want_writable) */
#define VIO_EV_QBLIMIT (1u << 3)  /* unparsed input exceeded client-query-buffer-limit */

/* Startup. Returns an eventfd to poll for readability, or -1 with errno. */
int vio_start(const VioConfig *cfg);

/* Main thread: consume the wakeup and iterate connections with work. Each
 * handle from vio_main_next_ready() carries one reference to release. */
void vio_main_begin_drain(void);
const VioConn *vio_main_next_ready(void);

/* Per connection, main thread only. */
void vio_conn_retain(const VioConn *h);
void vio_conn_release(const VioConn *h);
uint32_t vio_conn_take_events(const VioConn *h);
void *vio_conn_udata(const VioConn *h);
void vio_conn_set_udata(const VioConn *h, void *udata);
int vio_conn_fd(const VioConn *h);
size_t vio_conn_pop_cmds(const VioConn *h, VioCmd *out, size_t max);
size_t vio_conn_pending_cmds(const VioConn *h);
ssize_t vio_conn_writev(const VioConn *h, const struct iovec *iov, int iovcnt);
void vio_conn_want_writable(const VioConn *h, int on);
void vio_conn_set_auth_required(const VioConn *h, int on);
void vio_conn_close(const VioConn *h);
void vio_conn_shutdown(const VioConn *h);

#endif /* VALKEY_IO_H */
