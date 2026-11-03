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

typedef struct VioCmd VioCmd;

/* Per-connection engine state the precheck reads (cut point 2). Published
 * by the main thread with vio_conn_set_session(). */
#define VIO_SESS_AUTHENTICATED (1u << 0)
#define VIO_SESS_MULTI (1u << 1)
#define VIO_SESS_PUBSUB_RESP2 (1u << 2)
#define VIO_SESS_SPECIAL (1u << 3) /* replica, primary, slot migration, no user */
typedef struct VioSession {
    void *user;        /* engine user pointer, compared by identity only */
    uint64_t consumed; /* commands the engine popped from cmd_queue so far */
    uint32_t gen;      /* bumped by the engine when any other field changes */
    uint32_t flags;    /* VIO_SESS_* */
    int dbid;
} VioSession;

/* Engine callbacks, invoked on Rust IO threads. */
typedef struct VioCallbacks {
    /* Build a robj** of argc string objects copied from ptrs[i]/lens[i]. */
    void **(*create_argv)(const unsigned char *const *ptrs, const size_t *lens, int argc);
    /* Free an argv from create_argv that never reached the engine. */
    void (*free_argv)(void **argv, int argc);
    /* Zero-copy replies, on the IO thread: write the sealed chunk of job to
     * fd (can_write 0: only complete it). 1 written, 0 would block, -1 error. */
    int (*write_job)(void *job, int fd, int can_write);
    /* Precheck mode only (NULL otherwise). begin/end bracket one parsed
     * batch and pin the policy snapshot for this IO thread. precheck_cmd
     * fills cmd/slot/read_flags/token and returns 1 for a session barrier. */
    void *(*precheck_begin)(int tid);
    void (*precheck_end)(int tid);
    int (*precheck_cmd)(void *snap, const VioSession *sess, int barrier_active, VioCmd *cmd);
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

struct VioCmd {
    void **argv; /* robj** from create_argv; NULL when err != 0 */
    int argc;
    int err;      /* VIO_ERR_*; the error is always the last item */
    int err_byte; /* offending byte for VIO_ERR_UNEXPECTED_CHAR */
    size_t argv_len_sum;
    uint64_t input_bytes;
    /* Precheck mode: filled by precheck_cmd on the IO thread. */
    void *cmd; /* struct serverCommand *, NULL if not found */
    int slot;
    int read_flags;
    uint64_t token; /* 0 when not prechecked */
#ifdef RUST_IO_TIMING
    uint32_t trace; /* sampled stage-timing id, 0 when not sampled */
#endif
};

/* Event bits returned by vio_conn_take_events(). */
#define VIO_EV_NEW (1u << 0)      /* accepted; engine must adopt or close it */
#define VIO_EV_CLOSED (1u << 1)   /* peer closed or socket error */
#define VIO_EV_WRITABLE (1u << 2) /* outbound buffer drained (after want_writable) */
#define VIO_EV_QBLIMIT (1u << 3)  /* unparsed input exceeded client-query-buffer-limit */

/* Startup. Returns an eventfd to poll for readability, or -1 with errno. */
int vio_start(const VioConfig *cfg);

/* Main thread: consume the wakeup and take the current batch of ready
 * connections. */
void vio_main_begin_drain(void);
/* Hand the kicks collected during this iteration to the IO threads. */
void vio_main_flush_kicks(void);
/* Last call before the main thread polls; 1 = do not block. */
int vio_main_before_poll(void);
/* First call after the main thread polled; 1 = work was taken into the
 * current batch (drain it with vio_main_next_ready()). */
int vio_main_after_poll(void);
/* Connections left in the current batch (processEventsWhileBlocked()). */
int vio_main_has_ready(void);
/* Next ready connection, or NULL. Each handle carries one reference the
 * caller must vio_conn_release(). */
const VioConn *vio_main_next_ready(void);
/* Next completed zero-copy write job of the current batch, or NULL. */
void *vio_main_next_done(void);

/* Per connection, main thread only. */
/* Returns a new reference to h. */
const VioConn *vio_conn_retain(const VioConn *h);
void vio_conn_release(const VioConn *h);
uint32_t vio_conn_take_events(const VioConn *h);
void *vio_conn_udata(const VioConn *h);
void vio_conn_set_udata(const VioConn *h, void *udata);
int vio_conn_fd(const VioConn *h);
size_t vio_conn_pop_cmds(const VioConn *h, VioCmd *out, size_t max);
size_t vio_conn_pending_cmds(const VioConn *h);
/* Bytes accepted by vio_conn_writev() not yet written to the socket. */
size_t vio_conn_pending_out(const VioConn *h);
ssize_t vio_conn_writev(const VioConn *h, const struct iovec *iov, int iovcnt);
/* Zero-copy: write the sealed reply chunk of job from engine memory; job
 * comes back once through write_job and vio_main_next_done(). */
void vio_conn_submit_write(const VioConn *h, void *job, uint64_t release_offset);
/* Return an executed argv to the IO thread that built it (freed there with
 * free_argv, NULL entries skipped). Batched with the kicks. */
void vio_conn_return_argv(const VioConn *h, void **argv, int argc);
void vio_conn_want_writable(const VioConn *h, int on);
void vio_conn_set_auth_required(const VioConn *h, int on);
void vio_conn_set_session(const VioConn *h, const VioSession *s);
/* authRequired() for a new, unauthenticated connection. */
void vio_set_default_auth_required(int on);
void vio_conn_close(const VioConn *h);
void vio_conn_shutdown(const VioConn *h);

#ifdef RUST_IO_TIMING
/* Sampled stage timing (src/rust/valkey-io/src/timing.rs). */
typedef struct VioTimingStat {
    uint64_t count, p50_ns, p90_ns, p99_ns, max_ns;
} VioTimingStat;
void vio_timing_stamp(uint32_t id, uint32_t stage);
void vio_timing_drop(uint32_t id);
/* Tag the bytes of the next vio_conn_writev() on this thread with trace id. */
void vio_timing_next_write(uint32_t id);
void vio_timing_stat(uint32_t stage, VioTimingStat *out);
uint32_t vio_timing_sample_n(void);
uint64_t vio_timing_dropped(void);
void vio_timing_reset(void);
#endif

#endif /* VALKEY_IO_H */
