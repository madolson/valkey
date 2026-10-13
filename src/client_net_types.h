/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * client_net_types.h -- networking/client-I/O state owned by the networking
 * layer and embedded in `struct client` as `clientNet net`.
 *
 * This header is the type counterpart of client_net.h (which holds the
 * networking API prototypes). It exists so that the client's networking state
 * is DEFINED by the networking layer rather than inline in server.h. It is
 * included from server.h immediately before the `client` struct, after `sds`
 * and `connection` are available. Not standalone; include server.h.
 *
 * Option B extraction (incremental): fields are migrated here cluster by
 * cluster, each keeping the build green. See NETWORKING_MODULARIZATION.md.
 */
#ifndef VALKEY_CLIENT_NET_TYPES_H
#define VALKEY_CLIENT_NET_TYPES_H

/* Networking / client-I/O state. Embedded in `struct client` as `c->net`. */
typedef struct clientNet {
    sds peerid;    /* Cached peer ID (addr:port of the remote end). */
    sds sockname;  /* Cached connection target address (local bound addr). */
    /* Network byte-count statistics. */
    unsigned long long net_input_bytes;           /* Total network input bytes read from this client. */
    unsigned long long net_input_bytes_curr_cmd;  /* Total network input bytes read for the execution of this client's current command. */
    unsigned long long net_output_bytes;          /* Total network output bytes sent to this client. */
    unsigned long long net_output_bytes_curr_cmd; /* Total network output bytes sent to this client, by the current command. */
    _Atomic(size_t) io_tracked_reply_len;         /* Total size of BULK_STR_REF replies tracked by I/O threads. */
} clientNet;

#endif /* VALKEY_CLIENT_NET_TYPES_H */
