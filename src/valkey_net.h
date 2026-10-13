/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * valkey_net.h -- networking leaf operations, with two interchangeable
 * implementations selected at build time.
 *
 * When USE_RUST_NET is defined (the default for the normal server build), these
 * symbols are implemented in Rust (rust/valkey_net/src/lib.rs), compiled to a
 * static library (libvalkey_net.a) and linked in. When it is NOT defined -- for
 * build configurations where a matching-architecture Rust archive is not
 * available, e.g. the 32-bit build -- this header supplies equivalent
 * `static inline` C implementations, so no Rust toolchain or archive is needed
 * and callers (socket.c, server.c) compile unchanged.
 *
 * This keeps the Rust networking layer an opt-outable experiment: the C
 * fallback is behaviourally identical, so the server builds and runs either
 * way. See NETWORKING_MODULARIZATION.md.
 */
#ifndef VALKEY_NET_H
#define VALKEY_NET_H

#include <sys/uio.h>   /* struct iovec */
#include <sys/types.h> /* ssize_t, size_t */

/* ABI version shared by both implementations. */
#define VALKEY_NET_EXPECTED_ABI_VERSION 1u

#ifdef USE_RUST_NET

#ifdef __cplusplus
extern "C" {
#endif

/* Rust-provided symbols (linked from libvalkey_net.a). */
unsigned int valkey_net_abi_version(void);
int valkey_net_cstr_checksum(const char *s);
ssize_t valkey_net_write(int fd, const void *data, size_t data_len, int *out_errno);
ssize_t valkey_net_read(int fd, void *buf, size_t buf_len, int *out_errno);
ssize_t valkey_net_writev(int fd, const struct iovec *iov, int iovcnt, int *out_errno);

#ifdef __cplusplus
}
#endif

#else /* !USE_RUST_NET -- C fallback, behaviourally identical to the Rust path. */

#include <errno.h>
#include <string.h>
#include <unistd.h>

static inline unsigned int valkey_net_abi_version(void) {
    return VALKEY_NET_EXPECTED_ABI_VERSION;
}

static inline int valkey_net_cstr_checksum(const char *s) {
    if (s == NULL) return -1;
    unsigned int sum = 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) sum += *p;
    return (int)(sum & 0x7fffffffu);
}

static inline ssize_t valkey_net_write(int fd, const void *data, size_t data_len, int *out_errno) {
    ssize_t ret = write(fd, data, data_len);
    if (out_errno) *out_errno = (ret < 0) ? errno : 0;
    return ret;
}

static inline ssize_t valkey_net_read(int fd, void *buf, size_t buf_len, int *out_errno) {
    ssize_t ret = read(fd, buf, buf_len);
    if (out_errno) *out_errno = (ret < 0) ? errno : 0;
    return ret;
}

static inline ssize_t valkey_net_writev(int fd, const struct iovec *iov, int iovcnt, int *out_errno) {
    ssize_t ret = writev(fd, iov, iovcnt);
    if (out_errno) *out_errno = (ret < 0) ? errno : 0;
    return ret;
}

#endif /* USE_RUST_NET */

#endif /* VALKEY_NET_H */
