/* Chunky shard traffic counters.
 *
 * A chunky shard splits one node into N logical instances, each owning a
 * contiguous range of slots, behind shared IO threads that route each command
 * to its owning instance. These counters measure, on today's single-instance
 * server, how often real traffic would cross instances for N = 2, 4 and 8:
 *
 * - Per connection, keyed commands whose instance differs from the previous
 *   keyed command on the same connection. Keyless commands are counted apart
 *   and do not change the connection's instance.
 * - Read batches (one processInputBuffer() pass) whose commands span more
 *   than one instance, and MULTI/EXEC transactions that do.
 * - Commands whose keys span more than one slot, per command.
 * - Scripts and functions that touch keys not declared in KEYS, or whose keys
 *   span more than one slot or instance, and EVALs in compat mode.
 *
 * Instance i of N owns slots [i * 16384 / N, (i + 1) * 16384 / N). Everything
 * is tracked in terms of the instance of 8 (slot >> 11). The instance of
 * N = 8 >> shift is that value >> shift. */

#include "server.h"
#include "cluster.h"
#include "script.h"
#include "chunky.h"

#define CHUNKY_NUM_N 3 /* N = 2, 4, 8 */
#define CHUNKY_SHIFT(idx) (2 - (idx))

static const int chunky_n[CHUNKY_NUM_N] = {2, 4, 8};

static struct {
    long long cmds;
    long long keyless;
    long long keyed;
    long long multislot;
    long long switches[CHUNKY_NUM_N];
    long long multi_instance[CHUNKY_NUM_N];
    long long batches;
    long long batch_cmds;
    long long batches_multi_cmd;
    long long batches_mixed[CHUNKY_NUM_N];
    long long batch_cmds_mixed[CHUNKY_NUM_N];
    long long exec;
    long long exec_mixed[CHUNKY_NUM_N];
    long long script_runs;
    long long script_eval_compat;
    long long script_flag_cross_slot;
    long long script_undeclared_runs;
    long long script_undeclared_keys;
    long long script_nodecl_runs;
    long long script_multislot_runs;
    long long script_mixed_runs[CHUNKY_NUM_N];
} cs;

static chunkyBatch batch;

static struct {
    client *caller; /* NULL when no tracked script is running. */
    getKeysResult declared;
    int first_slot;
    int multislot;
    int undeclared;
    uint8_t mask;
} script;

/* Returns true if the instance-of-8 mask touches more than one instance of
 * N = 8 >> shift. */
static int maskSpans(unsigned m8, int shift) {
    unsigned m = 0;
    for (int i = 0; i < 8; i++) {
        if (m8 & (1u << i)) m |= 1u << (i >> shift);
    }
    return (m & (m - 1)) != 0;
}

/* Called from processCommand() for each new command from a regular client,
 * after lookup and arity checks. */
void chunkyTrackCommand(client *c) {
    cs.cmds++;
    if (batch.c == c) batch.cmds++;

    if (c->cmd->proc == multiCommand) c->chunky_multi_mask = 0;
    if (c->cmd->proc == execCommand && c->flag.multi) {
        cs.exec++;
        for (int i = 0; i < CHUNKY_NUM_N; i++) {
            if (maskSpans(c->chunky_multi_mask, CHUNKY_SHIFT(i))) cs.exec_mixed[i]++;
        }
    }

    getKeysResult result;
    initGetKeysResult(&result);
    int numkeys = getKeysFromCommand(c->cmd, c->argv, c->argc, &result);
    if (numkeys == 0) {
        getKeysFreeResult(&result);
        cs.keyless++;
        return;
    }
    int first_slot = -1, multislot = 0;
    unsigned m8 = 0;
    for (int j = 0; j < numkeys; j++) {
        sds key = objectGetVal(c->argv[result.keys[j].pos]);
        int slot = keyHashSlot(key, sdslen(key));
        if (first_slot == -1)
            first_slot = slot;
        else if (slot != first_slot)
            multislot = 1;
        m8 |= 1u << (slot >> 11);
    }
    getKeysFreeResult(&result);

    cs.keyed++;
    if (multislot) {
        cs.multislot++;
        c->cmd->chunky_multislot_calls++;
    }
    int inst8 = first_slot >> 11;
    for (int i = 0; i < CHUNKY_NUM_N; i++) {
        int shift = CHUNKY_SHIFT(i);
        if (maskSpans(m8, shift)) cs.multi_instance[i]++;
        if (c->chunky_last_inst >= 0 && (c->chunky_last_inst >> shift) != (inst8 >> shift)) cs.switches[i]++;
    }
    c->chunky_last_inst = inst8;
    if (batch.c == c) batch.mask |= m8;
    if (c->flag.multi) c->chunky_multi_mask |= m8;
}

/* Start a read batch for 'c'. Returns 0 if a batch for 'c' is already open
 * (nested call within the same pass), otherwise saves the open batch, if any,
 * into 'saved' and returns 1. A batch for another client can be open when we
 * get here from processEventsWhileBlocked(). */
int chunkyBatchBegin(client *c, chunkyBatch *saved) {
    if (batch.c == c) return 0;
    *saved = batch;
    batch.c = c;
    batch.cmds = 0;
    batch.mask = 0;
    return 1;
}

/* Close the batch opened by chunkyBatchBegin(). Does not dereference the
 * client, which may have been freed. */
void chunkyBatchEnd(chunkyBatch *saved) {
    if (batch.cmds) {
        cs.batches++;
        cs.batch_cmds += batch.cmds;
        if (batch.cmds > 1) cs.batches_multi_cmd++;
        for (int i = 0; i < CHUNKY_NUM_N; i++) {
            if (maskSpans(batch.mask, CHUNKY_SHIFT(i))) {
                cs.batches_mixed[i]++;
                cs.batch_cmds_mixed[i] += batch.cmds;
            }
        }
    }
    batch = *saved;
}

/* Called when a script or function starts running for 'caller'. The declared
 * keys are the keys of the caller's EVAL/FCALL, as the router would see them. */
void chunkyScriptBegin(client *caller, uint64_t script_flags) {
    if (mustObeyClient(caller)) return;
    cs.script_runs++;
    if (script_flags & SCRIPT_FLAG_EVAL_COMPAT_MODE) cs.script_eval_compat++;
    if (script_flags & SCRIPT_FLAG_ALLOW_CROSS_SLOT) cs.script_flag_cross_slot++;

    script.caller = caller;
    script.first_slot = -1;
    script.multislot = 0;
    script.undeclared = 0;
    script.mask = 0;
    initGetKeysResult(&script.declared);
    int numkeys = getKeysFromCommand(caller->cmd, caller->argv, caller->argc, &script.declared);
    for (int j = 0; j < numkeys; j++) {
        sds key = objectGetVal(caller->argv[script.declared.keys[j].pos]);
        int slot = keyHashSlot(key, sdslen(key));
        if (script.first_slot == -1)
            script.first_slot = slot;
        else if (slot != script.first_slot)
            script.multislot = 1;
        script.mask |= 1u << (slot >> 11);
    }
}

static int isDeclaredKey(sds key) {
    client *caller = script.caller;
    for (int j = 0; j < script.declared.numkeys; j++) {
        sds d = objectGetVal(caller->argv[script.declared.keys[j].pos]);
        if (sdslen(d) == sdslen(key) && memcmp(d, key, sdslen(key)) == 0) return 1;
    }
    return 0;
}

/* Called from VM_Call() for each command a running script issues, after
 * lookup and arity checks. */
void chunkyScriptTrackCall(client *c) {
    if (!script.caller) return;
    getKeysResult result;
    initGetKeysResult(&result);
    int numkeys = getKeysFromCommand(c->cmd, c->argv, c->argc, &result);
    for (int j = 0; j < numkeys; j++) {
        sds key = objectGetVal(c->argv[result.keys[j].pos]);
        int slot = keyHashSlot(key, sdslen(key));
        if (!isDeclaredKey(key)) {
            cs.script_undeclared_keys++;
            script.undeclared = 1;
        }
        if (script.first_slot == -1)
            script.first_slot = slot;
        else if (slot != script.first_slot)
            script.multislot = 1;
        script.mask |= 1u << (slot >> 11);
    }
    getKeysFreeResult(&result);
}

void chunkyScriptEnd(void) {
    if (!script.caller) return;
    if (script.undeclared) {
        cs.script_undeclared_runs++;
        if (script.declared.numkeys == 0) cs.script_nodecl_runs++;
    }
    if (script.multislot) cs.script_multislot_runs++;
    for (int i = 0; i < CHUNKY_NUM_N; i++) {
        if (maskSpans(script.mask, CHUNKY_SHIFT(i))) cs.script_mixed_runs[i]++;
    }
    getKeysFreeResult(&script.declared);
    script.caller = NULL;
}

static void resetCommandMultislot(hashtable *commands) {
    hashtableIterator iter;
    void *next;
    hashtableInitIterator(&iter, commands, HASHTABLE_ITER_SAFE);
    while (hashtableNext(&iter, &next)) {
        struct serverCommand *cmd = next;
        cmd->chunky_multislot_calls = 0;
        if (cmd->subcommands_ht) resetCommandMultislot(cmd->subcommands_ht);
    }
    hashtableCleanupIterator(&iter);
}

void chunkyResetStats(void) {
    memset(&cs, 0, sizeof(cs));
    resetCommandMultislot(server.commands);
}

static sds catPerN(sds info, const char *name, long long *v) {
    info = sdscatfmt(info, "%s:", name);
    for (int i = 0; i < CHUNKY_NUM_N; i++) {
        info = sdscatfmt(info, "%sn%i=%I", i ? "," : "", chunky_n[i], v[i]);
    }
    return sdscat(info, "\r\n");
}

static sds catCommandMultislot(sds info, hashtable *commands) {
    hashtableIterator iter;
    void *next;
    hashtableInitIterator(&iter, commands, HASHTABLE_ITER_SAFE);
    while (hashtableNext(&iter, &next)) {
        struct serverCommand *cmd = next;
        if (cmd->chunky_multislot_calls) {
            char *tmpsafe;
            info = sdscatprintf(info, "chunky_multislot_cmd_%s:calls=%lld\r\n",
                                getSafeInfoString(cmd->fullname, sdslen(cmd->fullname), &tmpsafe),
                                cmd->chunky_multislot_calls);
            if (tmpsafe != NULL) zfree(tmpsafe);
        }
        if (cmd->subcommands_ht) info = catCommandMultislot(info, cmd->subcommands_ht);
    }
    hashtableCleanupIterator(&iter);
    return info;
}

sds genChunkyInfoString(sds info) {
    info = sdscatfmt(info,
                     "chunky_stats_enabled:%i\r\n"
                     "chunky_cmds:%I\r\n"
                     "chunky_cmds_keyless:%I\r\n"
                     "chunky_cmds_keyed:%I\r\n"
                     "chunky_cmds_multislot:%I\r\n",
                     server.chunky_stats, cs.cmds, cs.keyless, cs.keyed, cs.multislot);
    info = catPerN(info, "chunky_switches", cs.switches);
    info = catPerN(info, "chunky_cmds_multi_instance", cs.multi_instance);
    info = sdscatfmt(info,
                     "chunky_batches:%I\r\n"
                     "chunky_batch_cmds:%I\r\n"
                     "chunky_batches_multi_cmd:%I\r\n",
                     cs.batches, cs.batch_cmds, cs.batches_multi_cmd);
    info = catPerN(info, "chunky_batches_mixed", cs.batches_mixed);
    info = catPerN(info, "chunky_batch_cmds_mixed", cs.batch_cmds_mixed);
    info = sdscatfmt(info, "chunky_exec:%I\r\n", cs.exec);
    info = catPerN(info, "chunky_exec_mixed", cs.exec_mixed);
    info = sdscatfmt(info,
                     "chunky_script_runs:%I\r\n"
                     "chunky_script_eval_compat:%I\r\n"
                     "chunky_script_flag_cross_slot:%I\r\n"
                     "chunky_script_undeclared_runs:%I\r\n"
                     "chunky_script_undeclared_keys:%I\r\n"
                     "chunky_script_nodecl_runs:%I\r\n"
                     "chunky_script_multislot_runs:%I\r\n",
                     cs.script_runs, cs.script_eval_compat, cs.script_flag_cross_slot, cs.script_undeclared_runs,
                     cs.script_undeclared_keys, cs.script_nodecl_runs, cs.script_multislot_runs);
    info = catPerN(info, "chunky_script_mixed_runs", cs.script_mixed_runs);
    return catCommandMultislot(info, server.commands);
}
