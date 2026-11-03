/*
 * rust_io_precheck.h - cut point 2 ("precheck") of the valkey-io Rust IO layer.
 *
 * See src/rust/valkey-io/DESIGN.md, section "Stage 2: precheck".
 */
#ifndef RUST_IO_PRECHECK_H
#define RUST_IO_PRECHECK_H

#ifdef USE_RUST_IO

struct client;
struct connection;

/* Set in parsedCommand.read_flags (and so in c->read_flags while the
 * command runs) when c->rust_precheck carries a precheck token. */
#define READ_FLAGS_RUST_PRECHECKED (1 << 30)
/* VioCmd.read_flags only: the IO thread skipped prepareCommandGeneric(). */
#define READ_FLAGS_RUST_PREPARE_ON_MAIN (1 << 29)

/* Token layout: epoch (low 32 bits of acl_mutation_epoch) << 32 |
 * session generation (24 bits) << 8 | code. */
enum {
    PC_NONE = 0,
    PC_FAST = 1,   /* every class A-C check passed against the snapshot */
    PC_REJECT = 2, /* the snapshot predicts a rejection; the full path emits it */
    /* Fallback reasons decided on the IO thread. */
    PC_FB_FIRST = 10,
    PC_FB_NO_SNAPSHOT = 10,
    PC_FB_BARRIER = 11,
    PC_FB_MODULES = 12,
    PC_FB_INELIGIBLE = 13,
    PC_FB_MULTI = 14,
    PC_FB_PUBSUB = 15,
    PC_FB_SPECIAL_CLIENT = 16,
    PC_FB_ACL_USER = 17,
    PC_FB_NO_SESSION = 18, /* first read parsed before the engine adopted the connection */
    /* Fallback reasons decided on the main thread. */
    PC_FB_EPOCH = 20,
    PC_FB_SESSION = 21,
    PC_FB_MODULES_LIVE = 22,
    PC_FB_CLUSTER = 23,
    PC_CODE_MAX = 24,
};

void rustPrecheckInit(void);
/* Main thread, once per event loop iteration: republish the snapshot if the
 * ACL epoch moved and free retired snapshots no IO thread holds. */
void rustPrecheckPublish(void);
/* processCommand(): 1 when the command may skip auth, lookup/arity,
 * NO_MULTI, ACL and cluster routing checks. Counts the outcome. */
int rustPrecheckValidate(struct client *c);
/* Session fields the precheck reads, packed (VIO_SESS_*). */
unsigned int rustPrecheckSessionFlags(struct client *c);
sds rustPrecheckCatInfo(sds info);

/* IO thread callbacks (VioCallbacks). */
void *rustPrecheckBegin(int tid);
void rustPrecheckEnd(int tid);

#endif /* USE_RUST_IO */
#endif /* RUST_IO_PRECHECK_H */
