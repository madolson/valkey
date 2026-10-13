/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * valkey_net.h -- C ABI exported by the Rust networking layer (rust/valkey_net).
 *
 * These symbols are implemented in Rust (rust/valkey_net/src/lib.rs), compiled
 * to a static library (libvalkey_net.a) and linked into valkey-server. In
 * Phase 2 the surface is a stub used to prove the C <-> Rust link path.
 */
#ifndef VALKEY_NET_H
#define VALKEY_NET_H

#include <sys/uio.h> /* struct iovec */

#ifdef __cplusplus
extern "C" {
#endif

/* ABI version of the Rust networking layer; must match the C expectation. */
#define VALKEY_NET_EXPECTED_ABI_VERSION 1u

/* Return the ABI version compiled into the Rust layer. */
unsigned int valkey_net_abi_version(void);

/* Compute a trivial checksum of a NUL-terminated C string, in Rust.
 * Returns -1 if s is NULL. Used by the Phase 2 link smoke test. */
int valkey_net_cstr_checksum(const char *s);

/* Socket byte-I/O leaf operations, implemented in Rust (Phase 3).
 *
 * Each performs the raw syscall on `fd` and returns its result (bytes
 * transferred, 0 for read-EOF, or <0 on error). On a negative return the errno
 * is written through `out_errno` (set to 0 otherwise); `out_errno` may be NULL.
 * The C caller retains ownership of connection-struct state transitions. */
long valkey_net_write(int fd, const void *data, unsigned long data_len, int *out_errno);
long valkey_net_read(int fd, void *buf, unsigned long buf_len, int *out_errno);
long valkey_net_writev(int fd, const struct iovec *iov, int iovcnt, int *out_errno);

#ifdef __cplusplus
}
#endif

#endif /* VALKEY_NET_H */
