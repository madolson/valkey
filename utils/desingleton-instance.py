#!/usr/bin/env python3
# One-off refactoring script, kept so other branches can rerun it. Not part of
# the build. It moves the per-instance fields of struct valkeyServer
# (src/server.h) into struct valkeyInstance and rewrites every `server.<field>`
# reference for those fields to `inst-><field>`, where `inst` is the
# thread-local pointer to the current instance.
#
# Field list: the "per-engine" (E) and "per-engine, summed for INFO" (E*)
# classes of the de-singleton research report. Run from the repo root on a
# tree that has not been rewritten yet:
#
#     python3 utils/desingleton-instance.py

import os
import re
import sys

FIELDS = """
db el cronloops in_exec in_call busy_module_yield_flags busy_module_yield_reply
client_pause_in_transaction server_del_keys_in_slot module_pipe current_client
executing_client clients_timeout_table execution_nesting postponed_clients
migrate_cached_sockets events_processed_while_blocked
stat_expired_keys_stale_perc stat_expired_keys_with_vola_stale_perc
stat_last_eviction_exceeded_time stat_last_active_defrag_time el_start
stat_active_time el_iteration_active el_cmd_cnt_start el_cmd_cnt_max
priority_el_cmd_cnt_prev priority_el_cmd_cnt_max el_cron_duration
duration_stats lazy_expire_disabled also_propagate replication_allowed
blocked_clients blocked_clients_by_type unblocked_clients ready_keys
tracking_pending_keys pending_push_messages sort_desc sort_alpha
sort_bypattern sort_store mstime ustime cmd_time_snapshot blocking_op_nesting
blocked_last_cron pubsubshard_channels watching_clients pre_command_oom_state
errors stat_numcommands stat_expiredkeys stat_expiredfields
stat_expired_time_cap_reached_count stat_expire_cycle_time_used
stat_evictedkeys stat_evictedscripts stat_total_eviction_exceeded_time
stat_keyspace_hits stat_keyspace_misses stat_active_defrag_hits
stat_active_defrag_misses stat_active_defrag_key_hits
stat_active_defrag_key_misses stat_active_defrag_scanned
stat_total_active_defrag_time commandlog stat_unexpected_error_replies
stat_total_error_replies stat_dump_payload_sanitizations
stat_total_prefetch_entries stat_total_prefetch_batches inst_metric dirty
tracking_clients hotkeys_manager
""".split()

# Client-side programs that do not use struct valkeyServer.
SKIP = re.compile(r"^(valkey-cli|valkey-benchmark|cli_|fuzzer_).*")

HEADER = "src/server.h"


def move_struct_fields():
    src = open(HEADER).read().split("\n")
    start = next(i for i, l in enumerate(src) if l.startswith("struct valkeyServer {"))
    end = next(i for i in range(start, len(src)) if src[i] == "};")
    taken = set()
    moved = []
    i = start + 1
    while i < end:
        code = re.sub(r"/\*.*?\*/", "", src[i]).split("//")[0]
        hit = [f for f in FIELDS if re.search(r"[\s*,]" + f + r"\s*(\[[^\]]*\])?\s*[;,]", code)]
        if not hit:
            i += 1
            continue
        first, last = i, i
        # A declarator split over two lines: "long long\n    name; /* ... */".
        if re.match(r"^\s+\w+;", src[i]) and not re.search(r"[;{}]\s*(/\*.*)?$", src[i - 1]):
            first = i - 1
        # An anonymous struct ending in "} name[...];".
        if src[i].strip().startswith("}"):
            first = i
            while not src[first].strip().startswith("struct {"):
                first -= 1
        # Comment continuation lines.
        if "/*" in src[last] and "*/" not in src[last].split("/*", 1)[1]:
            while "*/" not in src[last + 1]:
                last += 1
            last += 1
        moved.append((first, src[first : last + 1]))
        taken.update(hit)
        for j in range(first, last + 1):
            src[j] = None
        i = last + 1
    # A comment heading a run of declarations that all moved goes with them.
    groups = dict(moved)
    i = start + 1
    while i < end:
        if src[i] is None or not src[i].strip().startswith("/*"):
            i += 1
            continue
        c = i
        while "*/" not in src[c]:
            c += 1
        j = c + 1
        if j in groups:
            while j < end and src[j] is None:
                j += 1
            nxt = src[j].strip()
            heads_struct = groups[c + 1][0].strip() == "struct {"
            if heads_struct or nxt == "" or nxt.startswith("/*") or nxt.startswith("#") or nxt == "};":
                groups[c + 1] = src[i : c + 1] + groups[c + 1]
                for k in range(i, c + 1):
                    src[k] = None
        i = c + 1
    moved = ["\n".join(groups[k]) for k in sorted(groups)]
    missing = set(FIELDS) - taken
    if missing:
        sys.exit("fields not found in struct valkeyServer: %s" % sorted(missing))
    src = [l for l in src if l is not None]
    start = next(i for i, l in enumerate(src) if l.startswith("struct valkeyServer {"))
    block = [
        "/* State owned by one logical server instance. Today there is exactly one",
        " * instance; `inst` points at it on every thread that runs instance code.",
        " * Fields that are process-global (configuration, replication, persistence,",
        " * cluster bus, networking) stay in struct valkeyServer. */",
        "struct valkeyInstance {",
    ] + moved + ["};", ""]
    src[start:start] = block
    text = "\n".join(src)
    text = text.replace(
        "extern struct valkeyServer server;\n",
        "extern struct valkeyServer server;\n"
        "extern struct valkeyInstance server_instance;\n"
        "extern __thread struct valkeyInstance *inst;\n",
    )
    open(HEADER, "w").write(text)


# Matches string/char literals (left alone) or a server.<field> reference.
REF = re.compile(
    r'("(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\')|\bserver\.(' + "|".join(FIELDS) + r")\b"
)


def rewrite(path):
    text = open(path).read()
    count = [0]

    def sub(m):
        if m.group(1):
            return m.group(0)
        count[0] += 1
        return "inst->" + m.group(2)

    new = REF.sub(sub, text)
    if count[0]:
        open(path, "w").write(new)
    return count[0]


def main():
    move_struct_fields()
    total, files = 0, 0
    for root in ("src", "src/unit", "src/modules"):
        for name in sorted(os.listdir(root)):
            if not re.search(r"\.(c|h|cpp)$", name) or SKIP.match(name):
                continue
            n = rewrite(os.path.join(root, name))
            if n:
                total += n
                files += 1
    print("rewrote %d references in %d files" % (total, files))


if __name__ == "__main__":
    main()
