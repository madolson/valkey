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
};

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
void vio_conn_set_session(const VioConn *h, const VioSession *s);
/* authRequired() for a new, unauthenticated connection. */
void vio_set_default_auth_required(int on);
void vio_conn_close(const VioConn *h);
void vio_conn_shutdown(const VioConn *h);

/* ---------------------------------------------------------------------------
 * Exec mode (cut point 3), src/exec.rs. Engine side: src/rust_io_exec.c.
 * ------------------------------------------------------------------------- */

/* State call() keeps on its stack; owned by the Rust call(). */
typedef struct VioCallFrame {
    void *prev_client;
    void *real_cmd;
    int64_t dirty;
    int64_t old_repl_offset;
    uint32_t old_flags;
    int update_command_stats;
    int reprocessing;
} VioCallFrame;

/* prop_info() bits */
#define VIO_PI_PREVENT_PROP (1u << 0)
#define VIO_PI_IS_EXEC (1u << 1)
#define VIO_PI_IS_MODULE (1u << 2)
#define VIO_PI_FORCE_REPL (1u << 3)
#define VIO_PI_FORCE_AOF (1u << 4)
#define VIO_PI_PREVENT_REPL (1u << 5)
#define VIO_PI_PREVENT_AOF (1u << 6)

typedef void (*VioCmdProc)(void *c);

/* Engine helpers the Rust call() uses, one per side effect. */
typedef struct VioExecOps {
    int (*bg_block)(void *c);
    void (*enter)(void *c, VioCallFrame *f);
    void (*stats_err_reset)(void);
    void (*enter_execution_unit)(int update_cached_time, long long us);
    void (*begin_exec)(void *c);
    VioCmdProc (*proc_of)(void *c);
    void *(*debug_argv_begin)(void *c);
    void (*debug_argv_end)(void *c, void *state);
    void (*exit_execution_unit)(void);
    int (*after_proc)(void *c);
    int64_t (*add_duration)(void *c, int64_t us);
    int64_t (*server_dirty)(void);
    int (*count_failure)(void *c, void *real_cmd);
    void (*fire_result_event)(void *c, void *real_cmd, int failed, int64_t duration, int64_t dirty);
    void (*close_after_command)(void *c);
    void (*latency)(void *c, void *real_cmd, int64_t duration);
    void (*commandlog)(void *c, void *real_cmd);
    void (*feed_monitors)(void *c);
    void (*cmd_stats)(void *c, void *real_cmd);
    void (*reset_duration)(void *c);
    uint32_t (*prop_info)(void *c);
    void (*also_propagate)(void *c, int propagate_flags);
    void (*restore_flags)(void *c, uint32_t old_flags);
    void (*track_keys)(void *c);
    void (*count_command)(void);
    void (*peak_memory)(void);
    void (*after_command)(void *c);
    void (*set_woff)(void *c, int64_t old_repl_offset);
    void (*leave)(void *prev_client);
} VioExecOps;

/* exec-rtc hooks, called on Rust IO threads. */
typedef struct VioRtcOps {
    int (*try_lock)(void); /* 0 when acquired */
    int (*lock)(void); /* 0 when acquired, 1: holder is yielding, hand the batch over */
    void (*unlock)(void);
    void (*exec_ready)(const VioConn *h); /* engine side of one ready connection */
    void (*after_batch)(void);            /* lock held: AOF, replies, wake main */
} VioRtcOps;

typedef struct VioExecConfig {
    VioExecOps ops;
    int rtc;
    VioRtcOps rtc_ops;
} VioExecConfig;

/* Before vio_start(). Returns 0. */
int vio_exec_configure(const VioExecConfig *cfg);
/* The top-level call() for a client on a Rust connection. */
void vio_exec_call(void *c, int flags);
/* exec-rtc: hand this connection's pending work to the main thread. */
void vio_conn_defer_to_main(const VioConn *h);
/* Wake the main event loop. */
void vio_wake_main(void);
/* exec-rtc lock statistics as INFO lines. Returns length written. */
size_t vio_rtc_stats(char *buf, size_t len);

#endif /* VALKEY_IO_H */
