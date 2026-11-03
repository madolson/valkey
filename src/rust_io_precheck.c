/*
 * rust_io_precheck.c - cut point 2 ("precheck") of the valkey-io Rust IO layer.
 *
 * The Rust IO thread runs the class A-C checks of processCommand() right
 * after parsing, against an immutable policy snapshot and a per-connection
 * session mirror, and tags every command with a token. On the main thread
 * processCommand() calls rustPrecheckValidate(): when the token's ACL epoch
 * and session generation still match and the live cluster slot check passes,
 * processCommand() skips the auth, lookup/arity, protected, NO_MULTI, ACL and
 * getNodeByQuery() checks. Every other outcome runs the unchanged full path.
 *
 * What is validated how:
 *   command table      static after startup (modules force fallback)
 *   ACL users          copy-on-write snapshot, acl_mutation_epoch (acl.c)
 *   session (class B)  published per connection with a generation; the main
 *                      thread compares the current client fields directly
 *   cluster slot       recomputed live on the main thread (a few loads)
 *   other class C      not skipped: those checks stay in processCommand()
 *
 * See src/rust/valkey-io/DESIGN.md, section "Stage 2: precheck".
 */
#include "server.h"
#include "cluster.h"
#include "rust_io.h"

#ifdef USE_RUST_IO

#include <stdatomic.h>
#include "rust/valkey-io/include/valkey_io.h"
#include "rust_io_precheck.h"

extern unsigned long long acl_mutation_epoch;
void ACLCopyUser(user *dst, user *src);
void ACLFreeUser(user *u);
size_t moduleCommandFilterCount(void);
const VioSession *rustIoConnSession(connection *conn);

#define PC_MAX_USERS 64
#define PC_MAX_THREADS 128

typedef struct pcSnapshot {
    unsigned long long epoch; /* acl_mutation_epoch the copies reflect */
    int default_auth_required; /* authRequired() of an unauthenticated client */
    int modules;               /* module command filters registered at build time */
    int nusers;
    user *live[PC_MAX_USERS]; /* identity only, never dereferenced off-thread */
    user *copy[PC_MAX_USERS]; /* private deep copies, read-only once published */
    struct pcSnapshot *next;  /* retired list */
} pcSnapshot;

static _Atomic(pcSnapshot *) pc_current;
static _Atomic(pcSnapshot *) pc_hazard[PC_MAX_THREADS];
static pcSnapshot *pc_retired;

static struct {
    unsigned long long code[PC_CODE_MAX];
    unsigned long long snapshots_built;
    unsigned long long snapshots_freed;
} pc_stats;

/* Session barrier commands: they change state that later commands on the
 * same connection are prechecked against. Resolved once by name and compared
 * by pointer (container commands such as ACL and CLIENT have no proc of
 * their own, so a proc comparison would miss their subcommands). */
static const char *pc_barrier_names[] = {
    "auth", "hello", "select", "multi", "exec", "discard", "reset", "subscribe", "unsubscribe",
    "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe", "readonly", "readwrite", "client",
    "acl", "sync", "psync", "replconf", "quit", NULL,
};
#define PC_MAX_BARRIERS 32
static struct serverCommand *pc_barrier_cmds[PC_MAX_BARRIERS];

/* Commands whose checks read more than (cmd, argv, session, ACL, slot):
 * EXEC reads the MULTI queue, scripts read declared script flags. */
static serverCommandProc *pc_ineligible_procs[] = {
    execCommand, evalCommand, evalRoCommand, evalShaCommand, evalShaRoCommand, fcallCommand, fcallroCommand, NULL,
};

static int procIn(serverCommandProc *p, serverCommandProc **set) {
    for (int i = 0; set[i]; i++)
        if (set[i] == p) return 1;
    return 0;
}

/* ---------------------------------------------------------------------------
 * Snapshot build / publish / reclaim (main thread)
 * ------------------------------------------------------------------------- */

static pcSnapshot *pcBuild(void) {
    unsigned long long saved = acl_mutation_epoch; /* copies below bump it */
    pcSnapshot *s = zcalloc(sizeof(*s));
    s->default_auth_required =
        (!(DefaultUser->flags & USER_FLAG_NOPASS) || (DefaultUser->flags & USER_FLAG_DISABLED));
    s->modules = moduleCommandFilterCount() > 0;

    raxIterator ri;
    raxStart(&ri, Users);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
        user *u = ri.data;
        /* Users that hold roles read the roles' live selectors: not copied,
         * so they fall back (PC_FB_ACL_USER). */
        if ((u->flags & USER_FLAG_ROLE) || (u->roles && listLength(u->roles))) continue;
        if (s->nusers == PC_MAX_USERS) break;
        s->live[s->nusers++] = u;
    }
    raxStop(&ri);
    /* Copy after the walk: ACLCreateUnlinkedUser() inserts into and removes
     * from Users, which would invalidate the iterator. */
    for (int i = 0; i < s->nusers; i++) {
        s->copy[i] = ACLCreateUnlinkedUser();
        ACLCopyUser(s->copy[i], s->live[i]);
    }
    acl_mutation_epoch = saved;
    s->epoch = saved;
    pc_stats.snapshots_built++;
    return s;
}

static void pcFree(pcSnapshot *s) {
    unsigned long long saved = acl_mutation_epoch;
    for (int i = 0; i < s->nusers; i++) ACLFreeUser(s->copy[i]);
    acl_mutation_epoch = saved;
    zfree(s);
    pc_stats.snapshots_freed++;
}

static void pcReclaim(void) {
    pcSnapshot **pp = &pc_retired;
    while (*pp) {
        pcSnapshot *s = *pp;
        int held = 0;
        for (int i = 0; i < rust_io_config.threads && i < PC_MAX_THREADS; i++) {
            if (atomic_load(&pc_hazard[i]) == s) {
                held = 1;
                break;
            }
        }
        if (held) {
            pp = &s->next;
        } else {
            *pp = s->next;
            pcFree(s);
        }
    }
}

void rustPrecheckPublish(void) {
    if (rust_io_config.mode != RUST_IO_MODE_PRECHECK) return;
    pcSnapshot *cur = atomic_load_explicit(&pc_current, memory_order_relaxed);
    int modules = moduleCommandFilterCount() > 0;
    if (!cur || cur->epoch != acl_mutation_epoch || cur->modules != modules) {
        pcSnapshot *s = pcBuild();
        atomic_store(&pc_current, s);
        if (cur) {
            cur->next = pc_retired;
            pc_retired = cur;
        }
    }
    if (pc_retired) pcReclaim();
}

/* Evidence that a server served commands through the fast path: logged at
 * exit, so test harness server logs carry the counters. Skipped when nothing
 * was prechecked: a server that exits during startup (e.g. a bad RDB) must
 * keep its fatal error as the last log line. */
static void pcLogAtExit(void) {
    unsigned long long any = 0;
    for (int i = 0; i < PC_CODE_MAX; i++) any |= pc_stats.code[i];
    if (!any) return;
    serverLog(LL_NOTICE, "rust-io precheck: fast=%llu rejected=%llu barrier=%llu epoch=%llu session=%llu cluster=%llu",
              pc_stats.code[PC_FAST], pc_stats.code[PC_REJECT], pc_stats.code[PC_FB_BARRIER], pc_stats.code[PC_FB_EPOCH],
              pc_stats.code[PC_FB_SESSION], pc_stats.code[PC_FB_CLUSTER]);
}

void rustPrecheckInit(void) {
    atexit(pcLogAtExit);
    int n = 0;
    for (int i = 0; pc_barrier_names[i] && n < PC_MAX_BARRIERS - 1; i++) {
        struct serverCommand *cmd = lookupCommandByCString(pc_barrier_names[i]);
        if (cmd) pc_barrier_cmds[n++] = cmd;
    }
    pc_barrier_cmds[n] = NULL;
    for (int i = 0; i < PC_MAX_THREADS; i++) atomic_init(&pc_hazard[i], NULL);
    atomic_init(&pc_current, NULL);
}

/* ---------------------------------------------------------------------------
 * IO thread side
 * ------------------------------------------------------------------------- */

/* Pin the current snapshot for this IO thread (hazard pointer). The main
 * thread frees a retired snapshot only when no hazard slot holds it; the
 * re-check after the store closes the race with a concurrent swap+reclaim. */
void *rustPrecheckBegin(int tid) {
    if (tid < 0 || tid >= PC_MAX_THREADS) return NULL;
    pcSnapshot *s;
    do {
        s = atomic_load(&pc_current);
        atomic_store(&pc_hazard[tid], s);
    } while (s != atomic_load(&pc_current));
    return s;
}

void rustPrecheckEnd(int tid) {
    if (tid < 0 || tid >= PC_MAX_THREADS) return;
    atomic_store(&pc_hazard[tid], NULL);
}

static user *pcFindUser(pcSnapshot *s, void *live) {
    for (int i = 0; i < s->nusers; i++)
        if (s->live[i] == live) return s->copy[i];
    return NULL;
}

static int pcDecide(pcSnapshot *s, const VioSession *sess, int barrier_active, robj **argv, int argc,
                    struct serverCommand *cmd, int rf) {
    if (!s) return PC_FB_NO_SNAPSHOT;
    if (sess->gen == 0) return PC_FB_NO_SESSION;
    if (barrier_active) return PC_FB_BARRIER;
    if (s->modules) return PC_FB_MODULES;
    if (sess->flags & VIO_SESS_SPECIAL) return PC_FB_SPECIAL_CLIENT;
    if (sess->flags & VIO_SESS_MULTI) return PC_FB_MULTI;
    if (sess->flags & VIO_SESS_PUBSUB_RESP2) return PC_FB_PUBSUB;
    /* Unknown command and wrong arity are rejected by processCommand() before
     * anything else except the auth check, which rejects too. */
    if (!cmd || (rf & READ_FLAGS_BAD_ARITY)) return PC_REJECT;
    if (s->default_auth_required && !(sess->flags & VIO_SESS_AUTHENTICATED) && !(cmd->flags & CMD_NO_AUTH))
        return PC_REJECT;
    if ((cmd->flags & (CMD_MODULE | CMD_PROTECTED | CMD_PUBSUB)) || procIn(cmd->proc, pc_ineligible_procs))
        return PC_FB_INELIGIBLE;
    if (rf & READ_FLAGS_CROSSSLOT) return PC_REJECT;
    user *u = pcFindUser(s, sess->user);
    if (!u) return PC_FB_ACL_USER;
    int idx = 0;
    if (ACLCheckAllUserCommandPerm(u, cmd, argv, argc, sess->dbid, &idx) != ACL_OK) return PC_REJECT;
    return PC_FAST;
}

/* VioCallbacks.precheck_cmd. Runs on a Rust IO thread: reads only the
 * pinned snapshot, the session copy, the command table (immutable after
 * startup) and the private argv. */
static int rustPrecheckCmd(void *snap, const VioSession *sess, int barrier_active, VioCmd *v) {
    pcSnapshot *s = snap;
    robj **argv = (robj **)v->argv;
    struct serverCommand *cmd = NULL;
    int slot = -1;
    int rf = READ_FLAGS_PARSING_COMPLETED;
    /* prepareCommandGeneric(), except that module commands are left for the
     * main thread: their getkeys callback is module code (valkey#5054). */
    if (v->argc > 0) {
        cmd = lookupCommand(argv, v->argc);
        if (!cmd) {
            rf |= READ_FLAGS_COMMAND_NOT_FOUND;
        } else if (cmd->flags & CMD_MODULE) {
            v->cmd = NULL;
            v->slot = -1;
            v->read_flags = rf | READ_FLAGS_RUST_PREPARE_ON_MAIN;
            v->token = ((uint64_t)(sess->gen & 0xffffff) << 8) | PC_FB_INELIGIBLE;
            return 0;
        } else if (!commandCheckArity(cmd, v->argc, NULL)) {
            rf |= READ_FLAGS_BAD_ARITY;
        } else if (server.cluster_enabled) {
            slot = clusterSlotByCommand(cmd, argv, v->argc, &rf);
        }
    }
    v->cmd = cmd;
    v->slot = slot;
    v->read_flags = rf;
    int code = v->argc > 0 ? pcDecide(s, sess, barrier_active, argv, v->argc, cmd, rf) : PC_FB_INELIGIBLE;
    uint64_t epoch = s ? (uint32_t)s->epoch : 0;
    v->token = (epoch << 32) | ((uint64_t)(sess->gen & 0xffffff) << 8) | (uint64_t)code;
    if (!cmd) return 0;
    struct serverCommand *base = cmd->parent ? cmd->parent : cmd;
    for (int i = 0; pc_barrier_cmds[i]; i++)
        if (pc_barrier_cmds[i] == base) return 1;
    return 0;
}

/* VioCallbacks.precheck_batch: one parsed batch of one connection. A command
 * after a session barrier in the same batch is checked with the barrier
 * active. Returns the 1-based index of the last barrier, 0 if none. */
size_t rustPrecheckBatch(void *snap, const VioSession *sess, int barrier_active, VioCmd *cmds, size_t n) {
    size_t last = 0;
    for (size_t i = 0; i < n; i++) {
        if (cmds[i].err) continue;
        if (rustPrecheckCmd(snap, sess, barrier_active || last, &cmds[i])) last = i + 1;
    }
    return last;
}

/* ---------------------------------------------------------------------------
 * Main thread side
 * ------------------------------------------------------------------------- */

unsigned int rustPrecheckSessionFlags(client *c) {
    unsigned int f = 0;
    if (c->flag.authenticated) f |= VIO_SESS_AUTHENTICATED;
    if (c->flag.multi) f |= VIO_SESS_MULTI;
    if (c->flag.pubsub && c->resp == 2) f |= VIO_SESS_PUBSUB_RESP2;
    if (c->flag.replica || c->flag.primary || c->slot_migration_job || c->user == NULL || mustObeyClient(c))
        f |= VIO_SESS_SPECIAL;
    return f;
}

int rustPrecheckValidate(client *c) {
    if (!(c->read_flags & READ_FLAGS_RUST_PRECHECKED)) return 0;
    uint64_t t = c->rust_precheck;
    int code = (int)(t & 0xff);
    if (code >= PC_CODE_MAX) code = PC_FB_INELIGIBLE;
    if (code >= PC_FB_FIRST) goto fallback;

    if ((uint32_t)(t >> 32) != (uint32_t)acl_mutation_epoch) {
        code = PC_FB_EPOCH;
        goto fallback;
    }
    const VioSession *pub = rustIoConnSession(c->conn);
    if (!pub || ((t >> 8) & 0xffffff) != (pub->gen & 0xffffff) || pub->user != (void *)c->user ||
        pub->dbid != c->db->id || pub->flags != rustPrecheckSessionFlags(c)) {
        code = PC_FB_SESSION;
        goto fallback;
    }
    if (moduleCommandFilterCount() || (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)) {
        code = PC_FB_MODULES_LIVE;
        goto fallback;
    }
    if (code == PC_REJECT) goto fallback; /* the full path emits it in order */
    if (server.cluster_enabled && c->slot >= 0) {
        /* getNodeByQuery() returns myself without touching the keys when the
         * slot is ours, not migrating/importing, and the cluster is healthy. */
        if (getNodeBySlot(c->slot) != getMyClusterNode() || getMigratingSlotDest(c->slot) ||
            getImportingSlotSource(c->slot) || !isClusterHealthy()) {
            code = PC_FB_CLUSTER;
            goto fallback;
        }
    }
    pc_stats.code[PC_FAST]++;
    return 1;

fallback:
    pc_stats.code[code]++;
    return 0;
}

sds rustPrecheckCatInfo(sds info) {
    static const struct {
        int code;
        const char *name;
    } names[] = {
        {PC_FB_NO_SNAPSHOT, "no_snapshot"}, {PC_FB_BARRIER, "barrier"}, {PC_FB_MODULES, "modules"},
        {PC_FB_INELIGIBLE, "ineligible"}, {PC_FB_MULTI, "multi"}, {PC_FB_PUBSUB, "pubsub_resp2"},
        {PC_FB_SPECIAL_CLIENT, "special_client"}, {PC_FB_ACL_USER, "acl_user"}, {PC_FB_NO_SESSION, "no_session"}, {PC_FB_EPOCH, "epoch"},
        {PC_FB_SESSION, "session"}, {PC_FB_MODULES_LIVE, "modules_live"}, {PC_FB_CLUSTER, "cluster"},
    };
    unsigned long long fb = 0;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) fb += pc_stats.code[names[i].code];
    info = sdscatprintf(info,
                        "# Rust_io\r\n"
                        "rust_io_mode:%s\r\n"
                        "precheck_fast:%llu\r\n"
                        "precheck_rejected:%llu\r\n"
                        "precheck_fallback:%llu\r\n",
                        rust_io_config.mode == RUST_IO_MODE_PRECHECK ? "precheck"
                        : rust_io_config.mode == RUST_IO_MODE_IO     ? "io"
                                                                     : "other",
                        pc_stats.code[PC_FAST], pc_stats.code[PC_REJECT], fb);
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
        info = sdscatprintf(info, "precheck_fallback_%s:%llu\r\n", names[i].name, pc_stats.code[names[i].code]);
    info = sdscatprintf(info, "precheck_snapshots_built:%llu\r\nprecheck_snapshots_freed:%llu\r\nacl_mutation_epoch:%llu\r\n",
                        pc_stats.snapshots_built, pc_stats.snapshots_freed, acl_mutation_epoch);
    return info;
}

#endif /* USE_RUST_IO */
