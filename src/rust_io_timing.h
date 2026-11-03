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
/* Main-thread time in freeClientArgv(), every call (not sampled). */
extern uint64_t rit_fa_ticks, rit_fa_n;
uint64_t ritTicks(void);
void ritReset(void);

/* Each hook costs one load and compare when the command is not sampled. */
#define RUST_IO_TIMING_HOOK(c, stage)  \
    do {                               \
        if ((stage) == RIT_PROCESS) {  \
            if (rit_nq) ritProcess(c); \
        } else if (rit_cur_c == (c)) { \
            ritCall((c), (stage));     \
        }                              \
    } while (0)
#define RUST_IO_TIMING_WRITEV(conn)    \
    do {                               \
        if (rit_nr) ritWritev((conn)); \
    } while (0)
#define RUST_IO_TIMING_FREE_ARGV_BEGIN() uint64_t rit_fa0 = ritTicks()
#define RUST_IO_TIMING_FREE_ARGV_END()        \
    do {                                      \
        rit_fa_ticks += ritTicks() - rit_fa0; \
        rit_fa_n++;                           \
    } while (0)
#else
#define RUST_IO_TIMING_FREE_ARGV_BEGIN() ((void)0)
#define RUST_IO_TIMING_FREE_ARGV_END() ((void)0)
#define RUST_IO_TIMING_HOOK(c, stage) ((void)0)
#define RUST_IO_TIMING_WRITEV(conn) ((void)0)
#endif

#endif
