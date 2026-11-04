/* Test module to verify that module write operations are properly rejected
 * when the server is paused (CLIENT PAUSE WRITE or during CLUSTER FAILOVER).
 *
 * This module uses timer callbacks to execute writes during a pause,
 * reproducing the crash from issue #1921 where module-initiated writes
 * bypass the pause check in processCommand() and hit the assertion in
 * propagateNow().
 *
 * Commands:
 *   PAUSETEST.TIMER_CALL       - schedules a timer that does VM_Call("SET")
 *   PAUSETEST.TIMER_REPLICATE  - schedules a timer that does VM_Replicate
 *   PAUSETEST.TIMER_VERBATIM   - schedules a timer that does VM_ReplicateVerbatim
 *   PAUSETEST.GET_RESULT       - returns the result of the last timer operation
 *   PAUSETEST.SET_REPLICATE    - SET a key locally, then VM_Replicate it
 *   PAUSETEST.SET_VERBATIM     - SET a key locally, then VM_ReplicateVerbatim
 */

#include "valkeymodule.h"

#define UNUSED(V) ((void)V)

/* Store the result of the last timer operation for the test to query.
 * "NONE" = not run, "OK" = success, "NULL" = VM_Call returned NULL,
 * "ERROR" = error reply / VM_Replicate* returned an error.
 * Only one timer runs at a time, so a single variable is enough. */
static const char *last_result = "NONE";

/* Timer callback: VM_Call with replication. */
void timerCallHandler(ValkeyModuleCtx *ctx, void *data) {
    UNUSED(data);
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "SET", "cc!", "pause_timer_key", "from_call");
    /* Without "E" in the format, a rejected write is reported as a NULL reply,
     * not an error reply, so record the two cases distinctly. */
    if (reply == NULL) {
        last_result = "NULL";
        return;
    }
    if (ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        last_result = "ERROR";
    } else {
        last_result = "OK";
    }
    ValkeyModule_FreeCallReply(reply);
}

/* Timer callback: VM_Replicate. */
void timerReplicateHandler(ValkeyModuleCtx *ctx, void *data) {
    UNUSED(data);
    int ret = ValkeyModule_Replicate(ctx, "SET", "cc", "pause_timer_key", "from_replicate");
    last_result = (ret == VALKEYMODULE_OK) ? "OK" : "ERROR";
}

/* Timer callback: VM_ReplicateVerbatim.
 * Tests that ReplicateVerbatim is rejected when replica traffic is paused. */
void timerVerbatimHandler(ValkeyModuleCtx *ctx, void *data) {
    UNUSED(data);
    int ret = ValkeyModule_ReplicateVerbatim(ctx);
    last_result = (ret == VALKEYMODULE_OK) ? "OK" : "ERROR";
}

/* PAUSETEST.TIMER_CALL [delay_ms] - Schedule a timer that does VM_Call. */
int PauseTestTimerCall_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    long long delay = 100;
    if (argc > 1) ValkeyModule_StringToLongLong(argv[1], &delay);
    last_result = "NONE";
    ValkeyModule_CreateTimer(ctx, delay, timerCallHandler, NULL);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return VALKEYMODULE_OK;
}

/* PAUSETEST.TIMER_REPLICATE [delay_ms] - Schedule a timer that does VM_Replicate. */
int PauseTestTimerReplicate_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    long long delay = 100;
    if (argc > 1) ValkeyModule_StringToLongLong(argv[1], &delay);
    last_result = "NONE";
    ValkeyModule_CreateTimer(ctx, delay, timerReplicateHandler, NULL);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return VALKEYMODULE_OK;
}

/* PAUSETEST.TIMER_VERBATIM [delay_ms] - Schedule a timer that does VM_ReplicateVerbatim. */
int PauseTestTimerVerbatim_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    long long delay = 100;
    if (argc > 1) ValkeyModule_StringToLongLong(argv[1], &delay);
    last_result = "NONE";
    ValkeyModule_CreateTimer(ctx, delay, timerVerbatimHandler, NULL);
    ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    return VALKEYMODULE_OK;
}

/* PAUSETEST.GET_RESULT - Get the result of the last timer op. */
int PauseTestGetResult_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);
    ValkeyModule_ReplyWithSimpleString(ctx, last_result);
    return VALKEYMODULE_OK;
}

/* Write the key locally the way a module command would. */
static void setKey(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname, ValkeyModuleString *val) {
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, keyname, VALKEYMODULE_WRITE);
    ValkeyModule_StringSet(key, val);
    ValkeyModule_CloseKey(key);
}

/* PAUSETEST.SET_REPLICATE key value - SET locally, then VM_Replicate the SET. */
int PauseTestSetReplicate_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 3) return ValkeyModule_WrongArity(ctx);
    setKey(ctx, argv[1], argv[2]);
    if (ValkeyModule_Replicate(ctx, "SET", "ss", argv[1], argv[2]) == VALKEYMODULE_OK) {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        ValkeyModule_ReplyWithError(ctx, "ERR replicate failed");
    }
    return VALKEYMODULE_OK;
}

/* PAUSETEST.SET_VERBATIM key value - SET locally, then VM_ReplicateVerbatim. */
int PauseTestSetVerbatim_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 3) return ValkeyModule_WrongArity(ctx);
    setKey(ctx, argv[1], argv[2]);
    if (ValkeyModule_ReplicateVerbatim(ctx) == VALKEYMODULE_OK) {
        ValkeyModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        ValkeyModule_ReplyWithError(ctx, "ERR replicate failed");
    }
    return VALKEYMODULE_OK;
}

/* PAUSETEST.AVOIDTRAFFIC - Check VM_AvoidReplicaTraffic. */
int PauseTestAvoidTraffic_Command(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);
    ValkeyModule_ReplyWithLongLong(ctx, ValkeyModule_AvoidReplicaTraffic());
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);

    if (ValkeyModule_Init(ctx, "pausetest", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.TIMER_CALL", PauseTestTimerCall_Command, "", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.TIMER_REPLICATE", PauseTestTimerReplicate_Command, "", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.TIMER_VERBATIM", PauseTestTimerVerbatim_Command, "", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.SET_REPLICATE", PauseTestSetReplicate_Command, "write", 1, 1, 1) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.SET_VERBATIM", PauseTestSetVerbatim_Command, "write", 1, 1, 1) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.GET_RESULT", PauseTestGetResult_Command, "readonly", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "PAUSETEST.AVOIDTRAFFIC", PauseTestAvoidTraffic_Command, "readonly", 0, 0, 0) ==
        VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
