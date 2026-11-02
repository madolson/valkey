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
    RUST_IO_MODE_PRECHECK, /* cut point 2: not implemented, runs as io */
    RUST_IO_MODE_EXEC,     /* cut point 3: not implemented, runs as io */
};

typedef struct rustIoConfig {
    int mode;
    int port;     /* 0 = disabled (unless takeover) */
    int threads;  /* Rust IO threads */
    int takeover; /* Rust binds 'port'; the C TCP listener is not created */
} rustIoConfig;

extern rustIoConfig rust_io_config;

int RegisterConnectionTypeRust(void);
int connIsRust(struct connection *conn);
/* True when the C TCP listener on 'port' must not be created. */
int rustIoTakesOverPort(void);
/* Start the Rust layer if configured. Returns the number of listeners. */
int rustIoInitListeners(void);
void rustIoLogReady(void);

#endif /* USE_RUST_IO */
#endif /* RUST_IO_H */
