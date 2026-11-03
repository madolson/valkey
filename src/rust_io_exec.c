/*
 * rust_io_exec.c - engine side of rust-io-mode exec (cut point 3).
 *
 * The Rust layer owns the top-level call() for clients on Rust connections
 * (vio_exec_call(), src/rust/valkey-io/src/exec.rs). Rust cannot see the
 * client or server structs, so each engine side effect of call() is one
 * helper below, passed to Rust in a VioExecOps table. The helpers carry no
 * control flow of their own beyond what is needed to touch a field: the
 * order, the conditions on blocked/update_command_stats/reprocessing and the
 * propagation decision live in Rust.
 *
 * exec-rtc: the Rust IO thread that parsed a batch executes it itself while
 * holding the module GIL, which the main thread releases around aeApiPoll in
 * beforeSleep()/afterSleep() (see engineLockShared() in server.c).
 *
 * See src/rust/valkey-io/DESIGN.md, "Stage 3".
 */
#include "server.h"
#include "module.h"
#include "bgiteration.h"
#include "cluster_slot_stats.h"
#include "latency.h"
#include "cluster_legacy.h"
#include "rust_io.h"

#ifdef USE_RUST_IO

#include "rust/valkey-io/include/valkey_io.h"

static_assert(CMD_CALL_PROPAGATE_AOF == 1 && CMD_CALL_PROPAGATE_REPL == 2, "exec.rs constants");
static_assert(PROPAGATE_AOF == 1 && PROPAGATE_REPL == 2, "exec.rs constants");

/* In rust_io.c. */
void rustIoHandleReady(const VioConn *h);

int rust_io_exec_active = 0;
int rust_io_rtc_active = 0;

configEnum rust_io_exec_model_enum[] = {
    {"handoff", RUST_IO_EXEC_HANDOFF},
    {"rtc", RUST_IO_EXEC_RTC},
    {NULL, 0},
};

#define FORCE_AOF_BIT (1u << 0)
#define FORCE_REPL_BIT (1u << 1)
#define PREVENT_PROP_BIT (1u << 2)

/* ---------------------------------------------------------------------------
 * call() side effects, in the order the Rust call() uses them
 * ------------------------------------------------------------------------- */

static int exBgBlock(void *p) {
    return bgIteration_blockClientIfRequired((client *)p);
}

/* in_call, executing_client, the per-call snapshot and the flag reset at the
 * top of call(). */
static void exEnter(void *p, VioCallFrame *f) {
    client *c = p;
    server.in_call++;
    f->old_flags = (c->flag.force_aof ? FORCE_AOF_BIT : 0) | (c->flag.force_repl ? FORCE_REPL_BIT : 0) |
                   (c->flag.prevent_prop ? PREVENT_PROP_BIT : 0);
    f->real_cmd = c->realcmd;
    f->prev_client = server.executing_client;
    server.executing_client = c;
    f->update_command_stats = !(server.current_client && server.current_client->id == CLIENT_ID_AOF);
    f->reprocessing = c->flag.reexecuting_command ? 1 : 0;
    c->flag.force_aof = 0;
    c->flag.force_repl = 0;
    c->flag.prevent_prop = 0;
    c->redact_arg_bitmap = 0;
    f->dirty = server.dirty;
    f->old_repl_offset = server.primary_repl_offset;
}

static void exStatsErrReset(void) {
    incrCommandStatsOnError(NULL, 0);
}

static void exBeginExec(void *p) {
    client *c = p;
    c->flag.executing_command = 1;
    c->flag.buffered_reply = 0;
    c->flag.keyspace_notified = 0;
}

static VioCmdProc exProcOf(void *p) {
    return (VioCmdProc)((client *)p)->cmd->proc;
}

/* The enable-debug-assert check that a borrowed argv is not modified. */
typedef struct {
    robj **argv;
    int *refcount;
    int argc;
} debugArgv;

static void *exDebugArgvBegin(void *p) {
    client *c = p;
    if (!c->flag.argv_borrowed || !server.enable_debug_assert) return NULL;
    debugArgv *d = zmalloc(sizeof(*d));
    d->argc = c->original_argv ? c->original_argc : c->argc;
    d->argv = zmalloc(sizeof(robj *) * d->argc);
    d->refcount = zmalloc(sizeof(int) * d->argc);
    for (int i = 0; i < d->argc; i++) {
        d->argv[i] = c->original_argv ? c->original_argv[i] : c->argv[i];
        d->refcount[i] = d->argv[i]->refcount;
    }
    return d;
}

static void exDebugArgvEnd(void *p, void *state) {
    client *c = p;
    debugArgv *d = state;
    if (!d) return;
    robj **argv = c->original_argv ? c->original_argv : c->argv;
    int argc = c->original_argv ? c->original_argc : c->argc;
    serverAssert(argc == d->argc);
    for (int i = 0; i < d->argc; i++) {
        serverAssert(d->argv[i] == argv[i]);
        serverAssert(argv[i]->refcount >= d->refcount[i]);
    }
    zfree(d->argv);
    zfree(d->refcount);
    zfree(d);
}

static int exAfterProc(void *p) {
    client *c = p;
    if (!c->flag.blocked) c->flag.executing_command = 0;
    return c->flag.blocked;
}

static int64_t exAddDuration(void *p, int64_t us) {
    client *c = p;
    c->duration += us;
    return c->duration;
}

static int64_t exServerDirty(void) {
    return server.dirty;
}

static int exCountFailure(void *p, void *rc) {
    client *c = p;
    struct serverCommand *real_cmd = rc;
    int failed = incrCommandStatsOnError(real_cmd, ERROR_COMMAND_FAILED);
    if (!failed && c->deferred_reply_errors) {
        real_cmd->failed_calls++;
        failed = 1;
    }
    return failed;
}

static void exFireResultEvent(void *p, void *rc, int failed, int64_t duration, int64_t dirty) {
    client *c = p;
    struct serverCommand *real_cmd = rc;
    valkey_commands_trace(valkey_commands, command_call, connGetType(c->conn), getClientPeerId(c),
                          getClientSockname(c), real_cmd->declared_name, duration);
    moduleFireCommandResultEvent(c, real_cmd, failed, duration, dirty);
}

static void exCloseAfterCommand(void *p) {
    client *c = p;
    if (c->flag.close_after_command) {
        c->flag.close_after_command = 0;
        c->flag.close_after_reply = 1;
    }
}

static void exLatency(void *p, void *rc, int64_t duration) {
    client *c = p;
    struct serverCommand *real_cmd = rc;
    char *latency_event = (real_cmd->flags & CMD_FAST) ? "fast-command" : "command";
    latencyAddSampleIfNeeded(latency_event, duration);
    if (real_cmd->flags & CMD_FAST) {
        latencyTraceIfNeeded(server, fast_command, duration);
    } else {
        latencyTraceIfNeeded(server, command, duration);
    }
    if (server.execution_nesting == 0) {
        durationAddSample(EL_DURATION_TYPE_CMD, duration);
        if (connIsPriority(c->conn)) durationAddSample(EL_DURATION_TYPE_PRIORITY_CMD, duration);
    }
}

static void exCommandlog(void *p, void *rc) {
    commandlogPushCurrentCommand((client *)p, (struct serverCommand *)rc);
}

static void exFeedMonitors(void *p) {
    client *c = p;
    if (c->cmd->flags & (CMD_SKIP_MONITOR | CMD_ADMIN)) return;
    robj **argv = c->original_argv ? c->original_argv : c->argv;
    int argc = c->original_argv ? c->original_argc : c->argc;
    replicationFeedMonitors(c, server.monitors, c->db->id, argv, argc);
}

static void exCmdStats(void *p, void *rc) {
    client *c = p;
    struct serverCommand *real_cmd = rc;
    real_cmd->calls++;
    real_cmd->microseconds += c->duration;
    if (server.latency_tracking_enabled)
        updateCommandLatencyHistogram(&(real_cmd->latency_histogram), c->duration * 1000);
    clusterSlotStatsAddCpuDuration(c, c->duration);
}

static void exResetDuration(void *p) {
    ((client *)p)->duration = 0;
}

static uint32_t exPropInfo(void *p) {
    client *c = p;
    uint32_t b = 0;
    if (c->flag.prevent_prop) b |= VIO_PI_PREVENT_PROP;
    if (c->cmd->proc == execCommand) b |= VIO_PI_IS_EXEC;
    if (c->cmd->flags & CMD_MODULE) b |= VIO_PI_IS_MODULE;
    if (c->flag.force_repl) b |= VIO_PI_FORCE_REPL;
    if (c->flag.force_aof) b |= VIO_PI_FORCE_AOF;
    if (c->flag.prevent_repl_prop || c->flag.module_prevent_repl_prop) b |= VIO_PI_PREVENT_REPL;
    if (c->flag.prevent_aof_prop || c->flag.module_prevent_aof_prop) b |= VIO_PI_PREVENT_AOF;
    return b;
}

static void exAlsoPropagate(void *p, int pflags) {
    client *c = p;
    alsoPropagate(c->db->id, c->argv, c->argc, pflags, c->slot);
}

static void exRestoreFlags(void *p, uint32_t old) {
    client *c = p;
    c->flag.force_aof = !!(old & FORCE_AOF_BIT);
    c->flag.force_repl = !!(old & FORCE_REPL_BIT);
    c->flag.prevent_prop = !!(old & PREVENT_PROP_BIT);
}

static void exTrackKeys(void *p) {
    client *c = p;
    if ((c->cmd->flags & CMD_READONLY) && (c->cmd->proc != evalRoCommand) && (c->cmd->proc != evalShaRoCommand) &&
        (c->cmd->proc != fcallroCommand)) {
        if (server.current_client && (server.current_client->flag.tracking) &&
            !(server.current_client->flag.tracking_bcast)) {
            trackingRememberKeys(server.current_client, c);
        }
    }
}

static void exCountCommand(void) {
    if (server.current_client) server.current_client->commands_processed++;
    server.stat_numcommands++;
}

static void exPeakMemory(void) {
    size_t zmalloc_used = zmalloc_used_memory();
    if (zmalloc_used > server.stat_peak_memory) server.stat_peak_memory = zmalloc_used;
}

static void exAfterCommand(void *p) {
    afterCommand((client *)p);
}

static void exSetWoff(void *p, int64_t old_repl_offset) {
    if (old_repl_offset != server.primary_repl_offset) ((client *)p)->woff = server.primary_repl_offset;
}

static void exLeave(void *prev_client) {
    if (!server.in_exec && server.client_pause_in_transaction) server.client_pause_in_transaction = 0;
    server.executing_client = prev_client;
    server.in_call--;
}

void rustIoExecCall(client *c, int flags) {
    vio_exec_call(c, flags);
}

/* ---------------------------------------------------------------------------
 * exec-rtc
 * ------------------------------------------------------------------------- */

/* The real main thread. While a Rust IO thread holds the engine lock it
 * is "the main thread" for onServerMainThread() (server.h), which forkless
 * save, bgiteration and scripting assert on (forkless.c:309 fired on BGSAVE
 * from an IO thread). inMainThread() (io_threads.c) is already true on Rust
 * threads: its thread_id defaults to 0. */
static pthread_t rtc_real_main;

/* Taking the engine: become "the main thread" and refresh the cached
 * clock, which only the main loop (afterSleep, serverCron) updates.
 * Without it, clients accepted on an IO thread got a stale ctime after
 * the main thread ran DEBUG SLEEP ("CLIENT KILL maxAGE" killed both). */
static void rtcTakeEngine(void) {
    server.main_thread_id = pthread_self();
    updateCachedTime(0);
    /* As afterSleep() does: processCommand() checks expiry before call()
     * takes its own snapshot (getNodeByQuery() missed an expired key in a
     * migrating slot and served it as missing instead of -ASK). The lock is
     * never taken here while a command yields, so no call() is in flight. */
    server.cmd_time_snapshot = server.mstime;
}

static int rtcTryLock(void) {
    int r = moduleTryAcquireGIL();
    if (r == 0) rtcTakeEngine();
    return r;
}

int moduleTimedAcquireGIL(long usec); /* module.c */

/* Blocking acquire, except while the lock holder runs a long script or
 * module command that yields (processEventsWhileBlocked()): then return 1
 * and let Rust hand the batch to the yielding thread through the stage 1
 * ready list and eventfd, which processEventsWhileBlocked() polls, so
 * those clients get -BUSY and SCRIPT KILL / FUNCTION KILL get through.
 * isInsideYieldingLongCommand() is read without the lock (a flag and a
 * pointer the holder sets); a stale read costs one more 1 ms wait. */
static int rtcLock(void) {
    while (moduleTimedAcquireGIL(1000) != 0) {
        if (isInsideYieldingLongCommand()) return 1;
    }
    rtcTakeEngine();
    return 0;
}

static void rtcUnlock(void) {
    server.main_thread_id = rtc_real_main;
    moduleReleaseGIL();
}

/* networking.c. NULL on a thread that never ran the C read path, and
 * NULL == c->querybuf of a Rust client, so trimClientQueryBuffer() would take
 * the shared-buffer branch and crash in resetSharedQueryBuf(). */
extern _Thread_local sds thread_shared_qb;

/* server.c, not in server.h. */
void updateCachedTime(int update_daylight_info);

static void rtcExecReady(const VioConn *h) {
    /* Not initSharedQueryBuf(): it registers the buffer under getCurTid(),
     * which is the main thread's slot (0) on a Rust thread. */
    if (!thread_shared_qb) thread_shared_qb = sdsempty();
    rustIoHandleReady(h);
}

/* The part of beforeSleep() a batch needs before its replies may leave,
 * done here with the lock held, and a main-thread wakeup for the rest.
 * DESIGN.md "beforeSleep() under rtc" lists every beforeSleep() item. */
static void rtcAfterBatch(void) {
    /* The cluster state a command changed (CONFIG SET
     * cluster-require-full-coverage, CLUSTER ADDSLOTS ...) must be visible
     * to the next command, which may run on an IO thread before the main
     * thread wakes. The rest of clusterBeforeSleep() (failover, config
     * save, pong broadcast) uses the cluster bus and stays on the main
     * thread. */
    if (server.cluster_enabled && (server.cluster->todo_before_sleep & CLUSTER_TODO_UPDATE_STATE))
        clusterUpdateState();
    /* BCAST invalidations go out with the batch's replies, as in
     * beforeSleep(), instead of at the next main-thread iteration. */
    trackingBroadcastInvalidationMessages();
    /* Replies must not leave before the AOF write (beforeSleep() order). */
    if (server.aof_state == AOF_ON || server.aof_state == AOF_WAIT_REWRITE) flushAppendOnlyFile(0);
    handleClientsWithPendingWrites();
    rustIoPublishGlobals();
    if (listLength(server.unblocked_clients) || listLength(server.clients_pending_write) ||
        listLength(server.clients_to_close) || listLength(server.ready_keys) || server.get_ack_from_replicas ||
        listLength(server.postponed_clients) || connTypeHasPendingData() ||
        listLength(server.clients_waiting_acks) || server.failover_state == FAILOVER_WAIT_FOR_SYNC ||
        (server.cluster_enabled && server.cluster->todo_before_sleep) || bgIteration_iterationActive())
        vio_wake_main();
}

/* Commands that can run long and yield through processEventsWhileBlocked():
 * scripts, functions, module commands, DEBUG. An IO thread running one
 * cannot poll its own sockets, so a SCRIPT KILL arriving on the same IO
 * thread would never be read (the "MULTI and script timeout" test hung). */
static int rtcMayYield(struct serverCommand *cmd) {
    if (!cmd) return 0;
    serverCommandProc *p = cmd->proc;
    return p == evalCommand || p == evalRoCommand || p == evalShaCommand || p == evalShaRoCommand ||
           p == fcallCommand || p == fcallroCommand || p == debugCommand || (cmd->flags & CMD_MODULE);
}

/* On a Rust IO thread in exec-rtc: whether this client's queued commands
 * must run on the main thread instead. */
int rustIoRtcShouldDefer(client *c) {
    if (!rust_io_rtc_active || pthread_equal(pthread_self(), rtc_real_main)) return 0;
    for (int i = c->cmd_queue.off; i < c->cmd_queue.len; i++) {
        struct serverCommand *cmd = c->cmd_queue.cmds[i].cmd;
        if (rtcMayYield(cmd)) return 1;
        if (cmd && cmd->proc == execCommand && c->mstate) {
            for (int j = 0; j < c->mstate->count; j++)
                if (rtcMayYield(c->mstate->commands[j].cmd)) return 1;
        }
    }
    return 0;
}

/* Configure exec mode before vio_start(). */
int rustIoExecInit(void) {
    VioExecConfig cfg = {
        .ops =
            {
                .bg_block = exBgBlock,
                .enter = exEnter,
                .stats_err_reset = exStatsErrReset,
                .enter_execution_unit = enterExecutionUnit,
                .begin_exec = exBeginExec,
                .proc_of = exProcOf,
                .debug_argv_begin = exDebugArgvBegin,
                .debug_argv_end = exDebugArgvEnd,
                .exit_execution_unit = exitExecutionUnit,
                .after_proc = exAfterProc,
                .add_duration = exAddDuration,
                .server_dirty = exServerDirty,
                .count_failure = exCountFailure,
                .fire_result_event = exFireResultEvent,
                .close_after_command = exCloseAfterCommand,
                .latency = exLatency,
                .commandlog = exCommandlog,
                .feed_monitors = exFeedMonitors,
                .cmd_stats = exCmdStats,
                .reset_duration = exResetDuration,
                .prop_info = exPropInfo,
                .also_propagate = exAlsoPropagate,
                .restore_flags = exRestoreFlags,
                .track_keys = exTrackKeys,
                .count_command = exCountCommand,
                .peak_memory = exPeakMemory,
                .after_command = exAfterCommand,
                .set_woff = exSetWoff,
                .leave = exLeave,
            },
        .rtc = rust_io_config.exec_model == RUST_IO_EXEC_RTC,
        .rtc_ops =
            {
                .try_lock = rtcTryLock,
                .lock = rtcLock,
                .unlock = rtcUnlock,
                .exec_ready = rtcExecReady,
                .after_batch = rtcAfterBatch,
            },
    };
    if (vio_exec_configure(&cfg) != 0) return -1;
    rust_io_exec_active = 1;
    if (cfg.rtc) {
        /* The main thread owns the engine except while it polls. It already
         * holds the GIL: moduleInitModulesSystem() locks it at startup, so IO
         * threads that start before the dataset is loaded wait. */
        rtc_real_main = server.main_thread_id;
        rust_io_rtc_active = 1;
    }
    return 0;
}

char *rustIoGenInfo(char *info) {
    char buf[4096];
    info = sdscatprintf(info,
                        "# RustIO\r\n"
                        "rust_io_mode:%s\r\n"
                        "rust_io_exec_model:%s\r\n",
                        rust_io_config.mode == RUST_IO_MODE_EXEC ? "exec"
                        : rust_io_config.mode == RUST_IO_MODE_IO ? "io"
                                                                 : "other",
                        rust_io_rtc_active ? "rtc" : "handoff");
    if (rust_io_rtc_active) {
        vio_rtc_stats(buf, sizeof(buf));
        info = sdscat(info, buf);
    }
    return info;
}

#endif /* USE_RUST_IO */
