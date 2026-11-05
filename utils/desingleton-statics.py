#!/usr/bin/env python3
# One-off refactoring script, kept so other branches can rerun it. Not part of
# the build. Run after utils/desingleton-instance.py. It moves the file-scope
# per-instance globals and statics listed below into struct valkeyInstance
# (src/server.h) and rewrites their uses in the defining file to inst-><name>.
#
# The list is the file-scope part of the "E" statics of the de-singleton
# research report. Function-local statics, the PRNG state in mt19937-64.c (a
# library file without server.h), the Lua module's engine_ctx (it reaches the
# server only through the module API) and scripting_engine.c's debugger state
# stay where they are.
#
#     python3 utils/desingleton-statics.py

import re
import sys

# (file, name, field declaration, initializer or None)
STATICS = [
    ("evict.c", "EvictionPoolLRU", "struct evictionPoolEntry *EvictionPoolLRU;", None),
    ("evict.c", "isEvictionProcRunning", "int isEvictionProcRunning;", None),
    ("networking.c", "ProcessingEventsWhileBlocked", "int ProcessingEventsWhileBlocked; /* See processEventsWhileBlocked(). */", None),
    ("t_hash.c", "listpack_ttl_ignored", "bool listpack_ttl_ignored;", None),
    ("bgiteration.c", "freeItemStackHead", "struct itemListNode *freeItemStackHead;", None),
    ("bgiteration.c", "freeItemStackCount", "int freeItemStackCount;", None),
    ("bgiteration.c", "allIterators", "list *allIterators; /* list of bgIterator */", None),
    ("bgiteration.c", "nameToIterator", "dict *nameToIterator; /* bgIterator->name -> bgIterator */", None),
    ("bgiteration.c", "inUseEntries", "dict *inUseEntries; /* dbEntry -> ref count, across all iterators */", None),
    ("bgiteration.c", "curCmdMissingKeys", "list *curCmdMissingKeys; /* Keys of the current command missing from the DB */", None),
    ("bgiteration.c", "bufferedReplicationBytes", "ssize_t bufferedReplicationBytes; /* Buffered replication data, excluded from eviction */", None),
    ("bgiteration.c", "bgiteration_current_clone_memory_pool_size", "ssize_t bgiteration_current_clone_memory_pool_size; /* Bytes of cloned items */", None),
    ("bgiteration.c", "last_item_count_target", "int last_item_count_target; /* Last queue size, seeds the next queue */", None),
    ("bgiteration.c", "bgIterator_timeproc_id", "long long bgIterator_timeproc_id; /* Timer proc ID or AE_DELETED_EVENT_ID */", None),
    ("bgiteration.c", "bgIteration_epoch", "uint32_t bgIteration_epoch; /* Incremented on each new iteration */", "1"),
    ("bgiteration.c", "iteratorReplicationFlagsWereUpdated", "bool iteratorReplicationFlagsWereUpdated;", None),
    ("blocked.c", "inuse_key_to_clients", "hashtable *inuse_key_to_clients; /* Maps keys to keyToClientsEntry. */", None),
    ("eval.c", "isScriptsEvictionProcRunning", "int isScriptsEvictionProcRunning;", None),
    ("expire.c", "replicaKeysWithExpire", "dict *replicaKeysWithExpire; /* Keys with an expire set directly on a writable replica */", None),
    ("forkless.c", "currentForklessSave", "struct forklessSaveInfo *currentForklessSave;", None),
    ("functions.c", "curr_functions_lib_ctx", "functionsLibCtx *curr_functions_lib_ctx;", None),
    ("module.c", "moduleTempClients", "client **moduleTempClients;", None),
    ("module.c", "moduleTempClientCap", "size_t moduleTempClientCap;", None),
    ("module.c", "moduleTempClientCount", "size_t moduleTempClientCount; /* Client count in pool */", None),
    ("module.c", "moduleTempClientMinCount", "size_t moduleTempClientMinCount; /* Min client count in pool since the last cron. */", None),
    ("module.c", "modulePostExecUnitJobs", "list *modulePostExecUnitJobs;", None),
    ("script.c", "curr_run_ctx", "struct scriptRunCtx *curr_run_ctx;", None),
    ("tracking.c", "TrackingTable", "rax *TrackingTable;", None),
    ("tracking.c", "TrackingTableTotalItems", "uint64_t TrackingTableTotalItems; /* Total number of IDs stored across the tracking table. */", None),
    ("throttle.c", "metrics_table", "hashtable *metrics_table; /* Maps throttler type name to metricsEntry*. */", None),
    ("throttle.c", "total_throttled_commands", "long long total_throttled_commands; /* Cumulative throttled-command counter */", None),
]

LITERAL = r'"(?:\\.|[^"\\\n])*"|\'(?:\\.|[^\'\\\n])*\''


def drop_declaration(text, name):
    lines = text.split("\n")
    decl = re.compile(r"^(?:static )?[A-Za-z_][\w *]*?[ *]" + name + r"\s*(?:=[^;]*)?;")
    i = next((i for i, l in enumerate(lines) if decl.match(l)), None)
    if i is None:
        sys.exit("declaration of %s not found" % name)
    j = i
    # The declaration's comment may continue on following lines.
    if "/*" in lines[i] and "*/" not in lines[i].split("/*", 1)[1]:
        while "*/" not in lines[j]:
            j += 1
    del lines[i : j + 1]
    return "\n".join(lines)


def rewrite(text, names):
    ref = re.compile("(" + LITERAL + r")|(?<![\w.>])(" + "|".join(names) + r")\b")
    return ref.sub(lambda m: m.group(0) if m.group(1) else "inst->" + m.group(2), text)


def main():
    by_file = {}
    for f, name, _, _ in STATICS:
        by_file.setdefault(f, []).append(name)
    for f, names in by_file.items():
        path = "src/" + f
        text = open(path).read()
        for name in names:
            text = drop_declaration(text, name)
        text = rewrite(text, names)
        open(path, "w").write(text)

    # Extern declarations of the non-static globals in other files.
    for f in ("src/server.c",):
        text = open(f).read()
        for _, name, _, _ in STATICS:
            text = re.sub(r"^extern [^\n;]*\b" + name + r";\n", "", text, flags=re.M)
        text = rewrite(text, [n for _, n, _, _ in STATICS if n == "ProcessingEventsWhileBlocked"])
        open(f, "w").write(text)

    h = open("src/server.h").read()
    start = h.index("struct valkeyInstance {")
    end = h.index("\n};\n", start)
    fields = "\n    /* Former file-scope statics, by defining file. */\n" + "\n".join(
        "    " + decl + " /* " + f + " */" if "/*" not in decl else "    " + decl.replace("/*", "/* " + f + ":", 1)
        for f, _, decl, _ in STATICS
    )
    h = h[:end] + fields + h[end:]
    open("src/server.h", "w").write(h)

    inits = ", ".join("." + n + " = " + v for _, n, _, v in STATICS if v)
    c = open("src/server.c").read()
    old = "struct valkeyInstance server_instance;"
    if old not in c:
        sys.exit("server_instance definition not found in src/server.c")
    c = c.replace(old, "struct valkeyInstance server_instance = {" + inits + "};", 1)
    open("src/server.c", "w").write(c)
    print("moved %d statics from %d files" % (len(STATICS), len(by_file)))


if __name__ == "__main__":
    main()
