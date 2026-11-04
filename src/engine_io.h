/* Engine side of an external IO layer: what the engine offers a transport
 * that reads and parses outside the engine (src/rust_io.c today). Only this
 * file's implementation writes a client's cmd_queue for such a transport. */
#ifndef ENGINE_IO_H
#define ENGINE_IO_H

#include <stddef.h>
#include <stdint.h>

struct client;

/* One command parsed by the IO layer, or a protocol error when err != 0.
 * The IO layer's own type (valkey_io.h VioCmd) has this layout; rust_io.c
 * checks it at compile time. */
typedef struct engineCmd {
    void **argv; /* robj **, owned by the engine once submitted */
    int argc;
    int err; /* protocol error code (VIO_ERR_*), 0 for a command */
    int err_byte;
    uint32_t trace; /* stage-timing id (RUST_IO_TIMING), 0 otherwise */
    size_t argv_len_sum;
    uint64_t input_bytes;
    void *cmd; /* precheck: struct serverCommand *, filled off the engine thread */
    int slot;
    int read_flags;
    uint64_t token; /* precheck token, 0 when not prechecked */
} engineCmd;

/* Commands in c->cmd_queue not yet executed. */
int engineCmdQueuePending(struct client *c);

/* Append cmds[0..n] to c->cmd_queue and prepare them (lookup, arity, slot)
 * unless the IO layer prechecked them. Stops at the first protocol error and
 * stores its code in *err and *err_byte (the error is always the last
 * entry). Adds the commands' input bytes to *bytes. Returns the number of
 * commands appended. */
size_t engineSubmit(struct client *c, const engineCmd *cmds, size_t n, int *err, int *err_byte, unsigned long long *bytes);

/* A client that just became replicated (slot import): its querybuf is the
 * replication stream. Append the commands still in its cmd_queue, which
 * were submitted before, as engineSubmit() does for later ones. */
void engineReplicatedBackfill(struct client *c);

/* After a submit that appended commands: the read accounting
 * handleReadResult() does for a socket read. */
void engineAccountRead(struct client *c, unsigned long long bytes);

#endif /* ENGINE_IO_H */
