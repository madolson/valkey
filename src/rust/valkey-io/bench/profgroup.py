#!/usr/bin/env python3
"""Group perf-script samples of one thread into pipeline components.

  profgroup.py <perf script output>

'self' assigns each sample to exactly one group (sums to 100%): first the
inclusive rules (a syscall, the reply copy into Rust, the handoff drain:
anything under those frames belongs to them), then the leaf-most frame that
matches a leaf rule. 'incl' is the share of samples with any frame in the
group (overlaps). Rules are regexes over symbol names."""
import re, sys, collections

# (group, regex) — inclusive rules, checked over the whole stack in this order.
INCL = [
    ("syscall:epoll_wait", r"(__x64_sys_epoll_wait|do_epoll_wait|epoll_wait)$"),
    ("syscall:eventfd", r"(eventfd_read|eventfd_write)$"),
    ("syscall:read", r"(__x64_sys_read|ksys_read)$"),
    ("syscall:write", r"(__x64_sys_write|ksys_write|__x64_sys_writev|do_writev)$"),
    ("syscall:other", r"^(entry_SYSCALL_64|do_syscall_64)"),
    ("reply memcpy into Rust (connRustWritev)", r"(connRustWritev|vio_conn_writev)"),
    ("handoff: pop/drain/kick", r"(vio_main_|vio_conn_pop_cmds|rustIoPull|flush_kicks|wake_main|valkey_io::kick|"
                                r"on_kick|flush_notify|vio_conn_take_events|vio_conn_pending_cmds)"),
    ("key prefetch (prefetchCommandQueueKeys)", r"(prefetchCommandQueueKeys|addKeysToIncrFindBatch|"
                                                r"hashtableIncrementalFind)"),
]
# leaf rules: the leaf-most frame matching any of these wins.
LEAF = [
    ("clock reads (getMonotonicUs, ustime)", r"(getMonotonicUs|ustime|mstime|clock_gettime|__vdso|elapsedUs|"
                                             r"updateCachedTime)"),
    ("allocator: glibc malloc (Rust Vec/VecDeque/Arc)", r"^(malloc|free|cfree|calloc|realloc|_int_malloc|_int_free|"
                                                       r"_int_realloc|malloc_consolidate|tcache_get|tcache_put|"
                                                       r"__libc_malloc|__libc_free|__rust_alloc|__rust_dealloc|"
                                                       r"__rust_realloc)$|^(alloc::raw_vec|<alloc::raw_vec)"),
    ("allocator: jemalloc (zmalloc)", r"^(je_|_rjem_|zmalloc|zfree|zcalloc|zrealloc|ztrymalloc|valkey_malloc|"
                                      r"extent_|arena_|imalloc|ifree|sdsalloc)"),
    ("argv build (create_argv: createStringObject)", r"(rustIoCreateArgv|createEmbeddedStringObject|"
                                                     r"createStringObject|createRawStringObject)"),
    ("freeing argv", r"(freeClientArgv|rustIoFreeArgv|decrRefCount|freeStringObject|resetClient|commandProcessed)"),
    ("reply building", r"(addReply|_addReplyToBuffer|prepareClientToWrite|_addReplyProtoToList|setDeferred|"
                       r"writeToClient|_writeToClient|handleClientsWithPendingWrites|writevToClient)"),
    ("precheck (IO-thread checks; main-thread token validate)", r"(valkey_io::precheck|rustPrecheck|pcDecide|pcFindUser|precheck_cmd)"),
    ("processCommand checks", r"(processCommand|ACLCheck|checkKeyAccess|isPausedActions|"
                              r"blockPostponeClient|processInputBuffer|processCommandAndResetClient|"
                              r"getNodeByQuery|clusterRedirect)"),
    ("prepareCommand and lookup", r"(prepareCommand|lookupCommand|commandCheckArity|dictFetch|dictSdsCase|"
                                  r"siphash|getKeysFromCommand)"),
    ("call() and command procs", r"(^call$|Command$|lookupKey|dbAdd|setKey|dbFind|hashtable|kvstore|"
                                 r"objectSetKey|createObject|propagate|afterCommand|"
                                 r"updateCommandLatencyHistogram|hdr_record|notifyKeyspaceEvent|signalModified)"),
    ("RESP parse / IO-thread logic", r"(valkey_io::|resp::|Parser|parse_buffered|on_readable|Worker|"
                                     r"processMultibulk|processInline|readQueryFromClient)"),
    ("handoff: bridge dispatch (rustIoEventHandler, Deliver)", r"(rustIoEventHandler|rustIoDrainReady|"
                                                          r"rustIoHandleReady|rustIoDeliver|rustIoCanProgress)"),
    ("event loop", r"(aeProcessEvents|aeMain|beforeSleep|afterSleep|aeApiPoll|serverCron|whileBlocked)"),
    ("memcpy/libc", r"(memcpy|memmove|memset|memcmp|strlen|__memcpy|__memmove)"),
]
INCL = [(g, re.compile(r)) for g, r in INCL]
LEAF = [(g, re.compile(r)) for g, r in LEAF]


def samples(path):
    frames = []
    with open(path, errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                if frames:
                    yield frames
                frames = []
                continue
            if line[0] in " \t":
                parts = line.split(None, 1)
                if len(parts) == 2:
                    sym = parts[1].rsplit(" (", 1)[0]
                    sym = re.sub(r"\+0x[0-9a-f]+$", "", sym)
                    frames.append(sym)
    if frames:
        yield frames


def main():
    selfc, inclc, total = collections.Counter(), collections.Counter(), 0
    kernel_leaf = re.compile(r"^(native_|_raw_spin|__softirq|irq_|do_softirq|finish_task_switch|schedule)")
    for fr in samples(sys.argv[1]):
        total += 1
        g = None
        for name, rx in INCL:
            if any(rx.search(s) for s in fr):
                g = name
                break
        seen = set()
        for name, rx in INCL + LEAF:
            if any(rx.search(s) for s in fr):
                seen.add(name)
        for s in seen:
            inclc[s] += 1
        if g is None:
            for s in fr:
                for name, rx in LEAF:
                    if rx.search(s):
                        g = name
                        break
                if g:
                    break
        if g is None:
            g = "kernel (irq/sched, no syscall)" if kernel_leaf.search(fr[0]) else "other"
        selfc[g] += 1
    print("samples: %d" % total)
    print("%-44s %7s %7s" % ("group", "self%", "incl%"))
    for g, n in selfc.most_common():
        print("%-44s %7.1f %7.1f" % (g, 100.0 * n / total, 100.0 * inclc.get(g, n) / total))


main()
