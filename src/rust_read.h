/* C side of the Rust read path (src/rust/valkey-read), built with
 * BUILD_RUST_IO=yes. These networking.c functions are compiled out under
 * USE_RUST_IO and exported by the Rust crate instead. */
#ifndef VALKEY_RUST_READ_H
#define VALKEY_RUST_READ_H

#ifdef USE_RUST_IO
#include "server.h"

int handleReadResult(client *c);
void parseInlineBuffer(client *c);
void parseMultibulkBuffer(client *c);
void parseInputBuffer(client *c);
bool readToQueryBuf(client *c);
void resetSharedQueryBuf(client *c);
void handleQbLimitReached(client *c);
int postponeClientRead(client *c);
void trimCommandQueue(client *c);

/* Accessors for the _Thread_local shared query buffer, which stays a C
 * variable because freeClient() and trimClientQueryBuffer() compare it. */
sds rustReadGetThreadSharedQb(void);
void rustReadSetThreadSharedQb(sds qb);
#endif

#endif
