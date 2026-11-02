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
    RUST_IO_MODE_EXEC,     /* cut point 3: the IO layer owns call() */
};

enum {
    RUST_IO_EXEC_HANDOFF = 0, /* Rust call() on the main thread */
    RUST_IO_EXEC_RTC,         /* Rust IO thread executes under the engine lock */
};

typedef struct rustIoConfig {
    int mode;
    int port;     /* 0 = disabled (unless takeover) */
    int threads;  /* Rust IO threads */
    int takeover; /* Rust binds 'port'; the C TCP listener is not created */
    int exec_model; /* RUST_IO_EXEC_*, exec mode only */
} rustIoConfig;

extern rustIoConfig rust_io_config;

int RegisterConnectionTypeRust(void);
int connIsRust(struct connection *conn);
/* True when the C TCP listener on 'port' must not be created. */
int rustIoTakesOverPort(void);
/* Start the Rust layer if configured. Returns the number of listeners. */
int rustIoInitListeners(void);
void rustIoLogReady(void);

/* Exec mode (src/rust_io_exec.c). */
struct client;
extern int rust_io_exec_active; /* exec mode started: Rust conns use the Rust call() */
extern int rust_io_rtc_active;  /* exec-rtc: the engine lock is released around poll */
void rustIoExecCall(struct client *c, int flags);
int rustIoExecInit(void);
char *rustIoGenInfo(char *info);

#endif /* USE_RUST_IO */
#endif /* RUST_IO_H */
