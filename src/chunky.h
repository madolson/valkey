#ifndef __CHUNKY_H
#define __CHUNKY_H

#include <stdint.h>
#include "sds.h"

/* Counters estimating how often traffic would cross instances if this node
 * were split into N logical instances with contiguous, equal slot ranges
 * (N = 2, 4, 8). Enabled with the chunky-stats config and reported in
 * INFO chunky. See chunky.c. */

struct client;

typedef struct chunkyBatch {
    struct client *c; /* Client whose read pass is in progress, NULL if none. */
    long long cmds;   /* Commands seen in this pass. */
    uint8_t mask;     /* Bit i set if a key mapped to instance i of 8. */
} chunkyBatch;

void chunkyTrackCommand(struct client *c);
int chunkyBatchBegin(struct client *c, chunkyBatch *saved);
void chunkyBatchEnd(chunkyBatch *saved);
void chunkyScriptBegin(struct client *caller, uint64_t script_flags);
void chunkyScriptTrackCall(struct client *c);
void chunkyScriptEnd(void);
void chunkyResetStats(void);
sds genChunkyInfoString(sds info);

#endif
