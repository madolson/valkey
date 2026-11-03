/* Engine side of an external IO layer (engine_io.h). */
#include "server.h"
#include "engine_io.h"
#ifdef RUST_IO_TIMING
#include "rust_io_timing.h"
#endif

#define COMMAND_QUEUE_MIN_CAPACITY 16

int engineCmdQueuePending(client *c) {
    return c->cmd_queue.len - c->cmd_queue.off;
}

size_t engineSubmit(client *c, const engineCmd *cmds, size_t n, int *err, int *err_byte, unsigned long long *bytes) {
    cmdQueue *q = &c->cmd_queue;
    size_t appended = 0;
    if (q->off > 0) {
        memmove(q->cmds, q->cmds + q->off, (q->len - q->off) * sizeof(parsedCommand));
        q->len -= q->off;
        q->off = 0;
    }
    if (q->len + n > q->cap) {
        size_t cap = q->cap ? q->cap : COMMAND_QUEUE_MIN_CAPACITY;
        while (cap < q->len + n) cap *= 2;
        q->cap = cap;
        q->cmds = zrealloc(q->cmds, q->cap * sizeof(parsedCommand));
    }
    for (size_t i = 0; i < n; i++) {
        const engineCmd *e = &cmds[i];
        if (e->err) {
            *err = e->err;
            *err_byte = e->err_byte;
            break;
        }
        parsedCommand *p = &q->cmds[q->len++];
        p->read_flags = READ_FLAGS_PARSING_COMPLETED;
        p->argc = e->argc;
        p->argv = (robj **)e->argv;
        p->argv_len = e->argc;
        p->argv_len_sum = e->argv_len_sum;
        p->input_bytes = e->input_bytes;
        p->slot = -1;
        p->cmd = NULL;
        p->rust_precheck = e->token;
#ifdef RUST_IO_TIMING
        if (e->trace) ritDequeued(e->trace, e->argv);
#endif
        if (e->token && !(e->read_flags & READ_FLAGS_RUST_PREPARE_ON_MAIN)) {
            /* Precheck mode: lookup, arity and slot ran on the IO thread. */
            p->cmd = e->cmd;
            p->slot = e->slot;
            p->read_flags = e->read_flags | READ_FLAGS_RUST_PRECHECKED;
        } else {
            if (e->token) p->read_flags |= READ_FLAGS_RUST_PRECHECKED;
            prepareCommandGeneric(p->argv, p->argc, &p->read_flags, &p->cmd, &p->slot);
        }
        *bytes += e->input_bytes;
        appended++;
    }
    return appended;
}

void engineAccountRead(client *c, unsigned long long bytes) {
    server.stat_total_reads_processed++;
    server.stat_net_input_bytes += bytes;
    c->net_input_bytes += bytes;
    c->last_interaction = server.unixtime;
}
