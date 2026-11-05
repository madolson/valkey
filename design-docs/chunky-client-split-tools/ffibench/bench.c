#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
typedef struct cbuf { char *buf; size_t pos, cap; } cbuf;
typedef struct RsBuf RsBuf;
size_t c_append(cbuf *b, const char *p, size_t len);
size_t rs_append_cbuf(cbuf *b, const char *p, size_t len);
RsBuf *rs_buf_new(size_t cap);
void rs_buf_append(RsBuf *b, const char *p, size_t len);
size_t rs_buf_len(const RsBuf *b);
void rs_buf_clear(RsBuf *b);

static inline size_t c_append_inline(cbuf *b, const char *p, size_t len) {
    size_t avail = b->cap - b->pos;
    size_t n = len < avail ? len : avail;
    memcpy(b->buf + b->pos, p, n);
    b->pos += n;
    return n;
}

static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec / 1e9; }
#define CAP 16384
/* One GET-shaped reply is three appends: "$3\r\n", value, "\r\n" (addReplyBulk without copy avoidance). */
#define REPLY(APPEND, B)                         \
    do {                                         \
        APPEND(B, "$3\r\n", 4);                  \
        APPEND(B, val, 3);                       \
        APPEND(B, "\r\n", 2);                    \
    } while (0)

int main(int argc, char **argv) {
    long iters = argc > 1 ? atol(argv[1]) : 200000000L;
    volatile const char *vv = "bar"; const char *val = (const char *)vv;
    cbuf b = {malloc(CAP), 0, CAP};
    RsBuf *rb = rs_buf_new(CAP);
    double t; uint64_t sink = 0;

    t = now();
    for (long i = 0; i < iters; i++) { REPLY(c_append_inline, &b); if (b.pos > CAP - 64) { sink += b.pos; b.pos = 0; } __asm__ volatile("" ::: "memory"); }
    double t_inline = (now() - t) / iters * 1e9;

    t = now();
    for (long i = 0; i < iters; i++) { REPLY(c_append, &b); if (b.pos > CAP - 64) { sink += b.pos; b.pos = 0; } }
    double t_ccall = (now() - t) / iters * 1e9;

    t = now();
    for (long i = 0; i < iters; i++) { REPLY(rs_append_cbuf, &b); if (b.pos > CAP - 64) { sink += b.pos; b.pos = 0; } }
    double t_rscbuf = (now() - t) / iters * 1e9;

    t = now();
    for (long i = 0; i < iters; i++) { REPLY(rs_buf_append, rb); if (rs_buf_len(rb) > CAP - 64) { sink += rs_buf_len(rb); rs_buf_clear(rb); } }
    double t_rsbuf = (now() - t) / iters * 1e9;

    printf("ns_per_reply(3 appends) c_inline=%.2f c_call=%.2f rust_call_cbuf=%.2f rust_opaque_vec=%.2f sink=%llu\n",
           t_inline, t_ccall, t_rscbuf, t_rsbuf, (unsigned long long)sink);
    return 0;
}
