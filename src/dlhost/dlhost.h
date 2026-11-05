/*
 * dlhost.h - ABI between the dlmopen host (dlhost/host.c) and each
 * valkey-server instance loaded into its own link-map namespace
 * (dlhost_engine.c). Both sides are compiled against this header; nothing
 * else is shared. Memory crosses the boundary only as dlhostMsg, which the
 * host allocates and frees (each namespace has its own libc and malloc).
 */
#ifndef DLHOST_H
#define DLHOST_H

#include <stddef.h>
#include <stdatomic.h>
#include <sys/uio.h>

#define DLHOST_MSG_DATA 1  /* host -> engine: RESP bytes for one connection */
#define DLHOST_MSG_CLOSE 2 /* host -> engine: connection closed */
#define DLHOST_MSG_PARSED 5 /* host -> engine: commands already parsed into argv */

typedef struct dlhostMsg {
    struct dlhostMsg *next;
    int type;
    void *hconn;         /* opaque host connection */
    void **engine_slot;  /* per-engine slot in hconn, touched only by that engine */
    size_t len;
    size_t pos;          /* consumed by the engine */
    void *parsed;        /* DLHOST_MSG_PARSED: engine-allocated, from dlhostEngineParseFn */
    int nparsed;
    char data[];
} dlhostMsg;

/* Lock-free MPSC inbox: producers push chains, the consumer takes all. */
typedef struct dlhostInbox {
    _Atomic(dlhostMsg *) head;
    int wake_fd; /* eventfd, written when the inbox goes empty -> non-empty */
} dlhostInbox;

/* Host callbacks, called from the engine's main thread. */
typedef struct dlhostOps {
    /* Reply bytes produced by engine `idx` for `hconn`. Copies them and
     * queues them; nothing is visible to the IO thread until flush(). */
    void (*reply)(void *hconn, int idx, const struct iovec *iov, int iovcnt);
    /* Hand everything reply() queued for engine `idx` to the IO threads. */
    void (*flush)(int idx);
    /* Engine `idx` closed its client for `hconn` (QUIT, kill, error, or a
     * CLOSE message). Drops the engine's reference. */
    void (*engine_closed)(void *hconn, int idx, int by_engine);
    /* Engine `idx` took a reference on hconn (created a client). */
    void (*engine_ref)(void *hconn);
    /* Engine is done with a message from its inbox. */
    void (*msg_done)(dlhostMsg *m);
    /* Engine finished startup and is entering aeMain. */
    void (*ready)(int idx);
} dlhostOps;

/* Exported by the engine shared object. */
typedef void (*dlhostEngineSetupFn)(const dlhostOps *ops, int idx, dlhostInbox *inbox);
typedef int (*dlhostEngineMainFn)(int argc, char **argv);
/* Runs on a host IO thread, in the engine's namespace: turns n complete
 * multibulk requests into the engine's parsed-command structs, allocated
 * with the engine's allocator. */
typedef void *(*dlhostEngineParseFn)(const struct iovec *cmds, int n);

#endif
