/* Sampled per-stage timing for the Rust IO layer.
 * Built only with `make BUILD_RUST_IO=yes RUST_IO_TIMING=yes`; otherwise
 * every hook expands to nothing. See src/rust/valkey-io/src/timing.rs. */
#ifndef RUST_IO_TIMING_H
#define RUST_IO_TIMING_H

#if defined(USE_RUST_IO) && defined(RUST_IO_TIMING)
#include <stdint.h>

/* Stage indices; keep in sync with timing.rs. */
#define RIT_DEQUEUED 5
#define RIT_PROCESS 6
#define RIT_CALL 7
#define RIT_CALL_EXIT 8
#define RIT_REPLY_COPIED 9

struct client;
struct connection;
extern int rit_nq, rit_nr;
extern struct client *rit_cur_c;
void ritDequeued(uint32_t id, void **argv);
void ritProcess(struct client *c);
void ritCall(struct client *c, int stage);
void ritWritev(struct connection *conn);
sds ritCatInfo(sds info);
void ritReset(void);

/* Each hook costs one load and compare when the command is not sampled. */
#define RUST_IO_TIMING_HOOK(c, stage)                                  \
    do {                                                               \
        if ((stage) == RIT_PROCESS) {                                  \
            if (rit_nq) ritProcess(c);                                 \
        } else if (rit_cur_c == (c)) {                                 \
            ritCall((c), (stage));                                     \
        }                                                              \
    } while (0)
#define RUST_IO_TIMING_WRITEV(conn)          \
    do {                                     \
        if (rit_nr) ritWritev((conn));       \
    } while (0)
#else
#define RUST_IO_TIMING_HOOK(c, stage) ((void)0)
#define RUST_IO_TIMING_WRITEV(conn) ((void)0)
#endif

#endif
