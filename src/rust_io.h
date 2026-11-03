/*
 * rust_io.h - bridge between the engine and the valkey-io Rust IO layer.
 *
 * Everything here is compiled only with BUILD_RUST_IO=yes (USE_RUST_IO).
 * See src/rust/valkey-io/DESIGN.md.
 */
#ifndef RUST_IO_H
#define RUST_IO_H

#ifdef USE_RUST_IO

struct connection;

enum {
    RUST_IO_MODE_OFF = 0,
    RUST_IO_MODE_IO,       /* cut point 1: Rust does IO + RESP parsing */
    RUST_IO_MODE_PRECHECK, /* cut point 2: io + Rust-side processCommand() checks */
};

typedef struct rustIoConfig {
    int mode;
    int port;          /* 0 = disabled (unless takeover) */
    int threads;       /* Rust IO threads */
    int takeover;      /* Rust binds 'port'; the C TCP listener is not created */
    int zero_copy_min; /* replies of at least this many bytes are written by the IO thread from
                        * engine memory; -1 = always copy (DESIGN.md "Zero-copy replies") */
} rustIoConfig;

extern rustIoConfig rust_io_config;
extern int rust_io_precheck_active; /* precheck mode */

#include "connection_external.h"
/* True when the C TCP listener on 'port' must not be created. */
int rustIoTakesOverPort(void);
int rustIoListenerUnchanged(void);
/* Start the Rust layer if configured. Returns the number of listeners. */
int rustIoInitListeners(void);
void rustIoLogReady(void);
int rustIoBeforePoll(int sleeping);
void rustIoAfterPoll(void);
struct client;
/* Client memory held in the IO layer (DESIGN.md "Client memory"). */
size_t rustIoClientMem(struct client *c);
void rustIoClientQbuf(struct client *c, size_t *qbuf, size_t *qbuf_free);
size_t rustIoHeldMemory(void);
/* trySendWriteToIOThreads() for external connections. */
int rustIoTrySendWrite(struct client *c);
/* tryOffloadFreeArgvToIOThreads() for external connections. */
int rustIoTryOffloadFreeArgv(struct client *c, int argc, struct serverObject **argv);
/* waitForClientIO(): hand batched kicks to the IO threads first. */
void rustIoFlushKicks(void);
void rustIoPublishLimits(void);
sds rustIoCatInfo(sds info);
/* Wait up to timeout_ms for Rust to write what replicas were handed. */
void rustIoFlushReplicas(long long timeout_ms);

#include "rust_io_precheck.h"

#endif /* USE_RUST_IO */
#endif /* RUST_IO_H */
