/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* A module that writes keys from a timer, through the key API, and replicates
 * each write with ValkeyModule_Replicate. It declares HANDLE_FORKLESS, so a
 * full sync with it loaded can be forkless. */

#include "valkeymodule.h"

static long long timer_set = 0;     /* keys written by the last timer */
static long long timer_skipped = 0; /* keys the key API refused (in use by the sync) */
static int timer_done = 0;
static int open_new_keys = 0; /* open every new key for write from its "new" notification */

typedef struct {
    ValkeyModuleString *prefix;
    ValkeyModuleString *field; /* NULL to set strings, else the hash field to set */
    ValkeyModuleString *value;
    long long count;
} setAllArgs;

static void setAllTimer(ValkeyModuleCtx *ctx, void *data) {
    setAllArgs *args = data;
    size_t plen;
    const char *prefix = ValkeyModule_StringPtrLen(args->prefix, &plen);
    for (long long i = 0; i < args->count; i++) {
        ValkeyModuleString *keyname =
            ValkeyModule_CreateStringPrintf(ctx, "%.*s%lld", (int)plen, prefix, i);
        ValkeyModuleKey *key =
            ValkeyModule_OpenKey(ctx, keyname, VALKEYMODULE_WRITE);
        if (key == NULL) {
            timer_skipped++;
        } else {
            if (args->field) {
                ValkeyModule_HashSet(key, VALKEYMODULE_HASH_NONE, args->field, args->value, NULL);
                ValkeyModule_CloseKey(key);
                ValkeyModule_Replicate(ctx, "HSET", "sss", keyname, args->field, args->value);
            } else {
                ValkeyModule_StringSet(key, args->value);
                ValkeyModule_CloseKey(key);
                ValkeyModule_Replicate(ctx, "SET", "ss", keyname, args->value);
            }
            timer_set++;
        }
        ValkeyModule_FreeString(ctx, keyname);
    }
    ValkeyModule_FreeString(NULL, args->prefix);
    if (args->field) ValkeyModule_FreeString(NULL, args->field);
    ValkeyModule_FreeString(NULL, args->value);
    ValkeyModule_Free(args);
    timer_done = 1;
}

/* FORKLESSTIMER.SETALL <prefix> <count> <value>
 * Schedules a timer that sets <prefix>0 .. <prefix><count-1> in DB 0 to
 * <value>. The command itself is not a write, so it does not touch the
 * replication decision that the server caches for the last client write. */
static int setAllCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    long long count;
    if (ValkeyModule_StringToLongLong(argv[2], &count) != VALKEYMODULE_OK ||
        count < 0)
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid count");
    setAllArgs *args = ValkeyModule_Alloc(sizeof(*args));
    args->prefix = ValkeyModule_CreateStringFromString(NULL, argv[1]);
    args->field = NULL;
    args->value = ValkeyModule_CreateStringFromString(NULL, argv[3]);
    args->count = count;
    timer_set = timer_skipped = 0;
    timer_done = 0;
    ValkeyModule_CreateTimer(ctx, 1, setAllTimer, args);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* FORKLESSTIMER.HSETALL <prefix> <count> <field> <value>
 * Like FORKLESSTIMER.SETALL, but sets <field> in the hashes <prefix>0 ..
 * <prefix><count-1>, which keeps their other fields. */
static int hsetAllCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 5)
        return ValkeyModule_WrongArity(ctx);
    long long count;
    if (ValkeyModule_StringToLongLong(argv[2], &count) != VALKEYMODULE_OK ||
        count < 0)
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid count");
    setAllArgs *args = ValkeyModule_Alloc(sizeof(*args));
    args->prefix = ValkeyModule_CreateStringFromString(NULL, argv[1]);
    args->field = ValkeyModule_CreateStringFromString(NULL, argv[3]);
    args->value = ValkeyModule_CreateStringFromString(NULL, argv[4]);
    args->count = count;
    timer_set = timer_skipped = 0;
    timer_done = 0;
    ValkeyModule_CreateTimer(ctx, 1, setAllTimer, args);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

typedef struct {
    ValkeyModuleString *keys[2];
    ValkeyModuleString *value;
} msetArgs;

static void msetTimer(ValkeyModuleCtx *ctx, void *data) {
    msetArgs *args = data;
    for (int i = 0; i < 2; i++) {
        ValkeyModuleKey *key =
            ValkeyModule_OpenKey(ctx, args->keys[i], VALKEYMODULE_WRITE);
        if (key == NULL) {
            timer_skipped++;
        } else {
            ValkeyModule_StringSet(key, args->value);
            ValkeyModule_CloseKey(key);
            timer_set++;
        }
    }
    /* One replicated write that touches both keys. */
    ValkeyModule_Replicate(ctx, "MSET", "ssss", args->keys[0], args->value,
                           args->keys[1], args->value);
    for (int i = 0; i < 2; i++)
        ValkeyModule_FreeString(NULL, args->keys[i]);
    ValkeyModule_FreeString(NULL, args->value);
    ValkeyModule_Free(args);
    timer_done = 1;
}

/* FORKLESSTIMER.MSET <key1> <key2> <value>
 * Schedules a timer that sets both keys in DB 0 to <value> and replicates that
 * as a single MSET. */
static int msetCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4)
        return ValkeyModule_WrongArity(ctx);
    msetArgs *args = ValkeyModule_Alloc(sizeof(*args));
    args->keys[0] = ValkeyModule_CreateStringFromString(NULL, argv[1]);
    args->keys[1] = ValkeyModule_CreateStringFromString(NULL, argv[2]);
    args->value = ValkeyModule_CreateStringFromString(NULL, argv[3]);
    timer_set = timer_skipped = 0;
    timer_done = 0;
    ValkeyModule_CreateTimer(ctx, 1, msetTimer, args);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* Opens a new key for write and closes it without changing it, like a module
 * that inspects every key it sees created. */
static int newKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *keyname) {
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(event);
    if (!open_new_keys) return VALKEYMODULE_OK;
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, keyname, VALKEYMODULE_WRITE);
    if (key) ValkeyModule_CloseKey(key);
    return VALKEYMODULE_OK;
}

/* FORKLESSTIMER.OPENNEW <0|1> */
static int openNewCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    long long enable;
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    if (ValkeyModule_StringToLongLong(argv[1], &enable) != VALKEYMODULE_OK)
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid value");
    open_new_keys = enable != 0;
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

/* FORKLESSTIMER.STATS -> [done, set, skipped] */
static int statsCommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    if (argc != 1)
        return ValkeyModule_WrongArity(ctx);
    ValkeyModule_ReplyWithArray(ctx, 3);
    ValkeyModule_ReplyWithLongLong(ctx, timer_done);
    ValkeyModule_ReplyWithLongLong(ctx, timer_set);
    ValkeyModule_ReplyWithLongLong(ctx, timer_skipped);
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    if (ValkeyModule_Init(ctx, "forklesstimer", 1, VALKEYMODULE_APIVER_1) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_FORKLESS);
    if (ValkeyModule_CreateCommand(ctx, "forklesstimer.setall", setAllCommand,
                                   "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "forklesstimer.hsetall", hsetAllCommand,
                                   "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "forklesstimer.mset", msetCommand,
                                   "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "forklesstimer.opennew", openNewCommand,
                                   "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_NEW, newKeyNotification) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "forklesstimer.stats", statsCommand,
                                   "readonly", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
