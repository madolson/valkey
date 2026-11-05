# Chunky shard presented as one cluster node

A chunky shard is one valkey-server process that runs N instances. Each instance owns a subset of the node's slots and runs its own main thread. All of them sit behind shared IO threads that own every connection, compute each command's slot and hand the command to the owning instance. The process appears to the cluster and to clients as **one cluster node**: one node id, one nodes.conf, one cluster bus link set, one replication stream. madolson fixed that presentation after round one. N nodes per host would multiply client connections by N (`~/chunky/reports/topology.md`, Q1 and "Strongest evidence against").

This doc settles how that works, question by question, and splits the remaining work into stages. Scope is cluster mode only. Standalone keeps one instance (report 2 recommendation 1; section 4 below explains why cluster mode is the tractable case).

## Sources and conventions

- Code citations are `file:line` at `686abfe75` (`fork/epic/chunky-shards`). Upstream `origin/unstable` is `7836c5afe`, one commit ahead; that commit only touches `src/t_stream.c` and a stream test, so every citation here holds at both.
- `207:` prefixes cite `fork/rust-io/iter4` (PR https://github.com/madolson/valkey/pull/207).
- Round one PRs: #209 client split, #210 mixed-pipeline counters, #211 fork N nodes, #212 Rust read path, #213 dlmopen host, #214 instance struct, #215 precheck on C IO threads. Reports: `~/chunky/reports/<job>.md`.
- Research reports: `~/engine-research/<n>-*.md`, written against `d2466b90a`. I re-checked every citation reused here at `686abfe75`; line numbers that moved are given at the new position.
- Behavior probes for this doc ran against a one-node cluster owning all 16384 slots, built from `686abfe75`. Script, output and binary: `~/chunky2/data/single-node-spec/probe.sh`, `probe.out`, `probe_watch.py`, `probe_watch.out`.
- Tags: **[V]** read in code or measured; **[I]** inferred.

Vocabulary:

| Term | Meaning |
|---|---|
| node | The process as the cluster sees it. One id, one bus identity. |
| instance | One shared-nothing executor inside the node: a main thread, a `struct valkeyInstance` (#214), its own kvstores for the slots it owns. |
| IO layer | The shared IO threads (Rust per #207/#212, or C io_threads during transition). Owns sockets, parsing, sessions, routing. |
| control plane | The thread that owns node-global state: cluster bus, config, ACL, replication log readers, persistence scheduling. |
| session | Per-connection state the IO layer owns (identity, db, protocol, flags). |
| context | Per-(session, instance) execution state: today's `client` minus session and IO fields (report 1 §0). |
| slot map | slot → (node, instance). Node part comes from the bus; the instance part is node-local and never leaves the process. |
| G | All-instance barrier: every instance parks at a command boundary, the operation runs, all resume. |
| P | Park-and-borrow on a subset of instances (report 2 §4). |
| A | Fan out to each instance, aggregate, no barrier. |

## 1. Threads or processes

One node means one listener and one IO layer that sees every connection. Two ways to put N executors behind it.

**Threads (recommended).** N instance threads in one address space, each with its own `struct valkeyInstance` reached through a thread-local pointer. #214 built this at N=1: 78 fields and 31 statics moved, `__thread struct valkeyInstance *inst` at `src/server.c:114-118@e0b5cbc69`, +0.5% to +1.8% user instructions per request, throughput inside noise (`~/chunky/reports/engine-struct.md`, results table). The IO layer pushes a parsed command into the owning instance's inbox. #213 measured exactly this shape (lock-free per-instance inbox, shared IO threads, in-order reply write-back) with dlmopen standing in for de-singletoning: 4 instances matched 4 processes with a slot-aware client (GET -P 16 3.48M vs 3.47M) and lost 15-18% with plain clients whose pipelines mix all 4 instances (`~/chunky/reports/dlmopen.md`, summary).

**Processes.** A front IO process owns the sockets and sessions and feeds N instance processes over shared-memory rings. #211 shows that forking N children right after config load is about 350 lines (`~/chunky/reports/fork.md`), but #211's children are N cluster nodes, each with its own listener. Under one-node presentation the children may not own sockets, so the front process must build argv in shared memory and the instances must read requests from and write replies to it.

| Axis | Threads | Front process + instance processes |
|---|---|---|
| IPC per command | One inbox push, one reply chunk, both in-process. Measured in #213: +63% engine cycles per GET on fully mixed pipelines (1728 to 2813), 15-18% throughput loss, nothing extra when unmixed. | Same ring handoff, plus every argv and reply lives in a shared segment. A value the keyspace keeps must be copied out of the segment into the instance heap, because SET keeps a large value's sds by pointer (`src/object.c:507-512` [V]). That is the copy report 5 §3 wants to avoid, made mandatory [I]. |
| Session state and cross-session ops | IO layer reads instance-published stats directly (report 1 §2). | Same registry, but every CLIENT LIST/KILL, pub/sub fan-out, tracking push and MONITOR line crosses processes [I]. |
| Crash isolation | An instance crash is a process crash; the node fails over as a unit. | An instance can die alone. That leaves a node that serves some of its slots and not others, which the cluster protocol cannot express: the bus advertises one bitmap per node and one health. Either the front kills everything (no better than threads) or it reports those slots unserved (`-CLUSTERDOWN Hash slot not served`, `src/cluster.c:1361`) until a restart that has to resync that instance's slots from a replica that runs a merged stream (section 6). [I] |
| Fork | Needs every instance parked for the duration of `fork()` (report 4 §3), because the child image is torn otherwise. Forkless avoids it. | Each instance forks itself; only its own RSS and page tables. Clearly better [I]. |
| Memory accounting | Shared heap; needs per-instance counters and ownership rules at hand-offs (report 5). | Exact per process for free. A global maxmemory needs a shared counter in the segment [I]. |
| Modules | One module instance; needs the opt-in API (section 9). | Each instance loads its own copy (#211 shows `loadmodule` runs in every child, no "Module name is busy"). But the front process cannot run module `getkeys-api` or command filters, so module commands cannot be routed without a module protocol across processes [I]. |
| Replication, AOF | One merged log written by N threads with one fetch-add per batch (report 4 option A). | Merged log in shared memory written by N processes: same design, with cross-process futexes and crash recovery of a half-written reservation [I]. |
| Existing work | #214, #209, #212, #215 all assume one address space. | #211 for startup and supervision only. |

Recommendation: **threads**. Every item the one-node presentation needs (session registry, reply ordering, merged log, node-level fan-out) is cheaper in one address space, and the round-one path (#214 instance struct, #209 client split, #212 Rust read path, #215 precheck) already builds it. Processes win on fork and on accounting, but forkless save is upstream (`src/forkless.c`, `resolveBgsaveType` at `src/rdb.c:1859`) and fixes fork for threads too. The crash-isolation argument does not survive one-node presentation, because a partly failed node cannot be advertised. Keep #213's dlmopen host as a measurement harness only; its own report lists fork, modules, signals, cwd and crash logging as broken by design.

## 2. What the node shows the cluster

The cluster subsystem stays one per node. `server.cluster`, `myself`, the bus links, nodes.conf (`clusterLoadConfig`, `src/cluster_legacy.c:702`) and the epoch state move to the control plane, not into `struct valkeyInstance`. #214 already left them global (`engine-struct.md`, "Left global: all 473 G-sink, G-rt, G-cfg and IO fields").

| Surface | Code | Under one-node presentation |
|---|---|---|
| `CLUSTER MYID`, `CLUSTER NODES` | `src/cluster.c:810`, `clusterGenNodesDescription` `src/cluster_legacy.c:7845` | Unchanged. One line, one id, one config epoch. |
| `CLUSTER SLOTS` | `clusterCommandSlots`, `src/cluster.c:1589` | Unchanged. The slot bitmap is the union of instance ranges; instance boundaries are invisible. Contiguous instance ranges keep the reply as short as today. |
| `CLUSTER SHARDS` | `clusterCommandShards`, `src/cluster_legacy.c:8084`; `replication-offset` `:8054`, `health` `:8058` | Unchanged shape. `replication-offset` is the merged-log offset (section 6). `health` is the node's. |
| `INFO cluster`, `CLUSTER INFO` | | Unchanged. |
| Bus PING/PONG | scalar `offset` `src/cluster_legacy.h:369`, asserted at byte 32 `:403` | Unchanged; the scalar is the merged-log offset. |
| Failover rank | `clusterGetReplicaRank`, `src/cluster_legacy.c:6127`, compares `repl_offset` | Unchanged, because the offset stays scalar. |
| MOVED/ASK | `getNodeByQuery` `src/cluster.c:1066`; `n != myself` gives MOVED at `:1337`; text at `:1366` | Unchanged between nodes. The IO layer calls the same routing and gets `myself` for every slot a local instance owns; the instance part of the slot map then picks the inbox. |
| Keyless commands | `if (c->slot == -1) return myself;` `src/cluster.c:1115` | Routed by section 5's table, never redirected. |

**Rule: MOVED and ASK only ever name another node.** An intra-node slot move (section 7) holds the slot's commands in the IO layer for the handoff and then dispatches them to the new instance. It never returns MOVED or ASK pointing at the node itself, and never TRYAGAIN. The topology report found that glide treats MOVED-to-self as circular and replaces the connection, iovalkey recreates it, Jedis refreshes per MOVED and Lettuce fans a refresh out to every node (`topology.md`, Q1, last bullet).

The IO layer needs the slot map on its own threads. `clusterSlotByCommand` (`src/cluster.c:996`) is already side-effect free (report 2, "Ground truth"); `getNodeByQuery` reads `server.cluster` state that the bus mutates. Publish an immutable slot map (node part from the bus, instance part from the local map) through the same epoch-plus-snapshot scheme as the ACL snapshot (section 3), and let the control plane swap it.

## 3. Routing and sessions

**Session ownership.** The IO layer owns the session: client id, user (as an immutable ACL snapshot reference), auth state, selected db id, RESP version, name and lib info, CLIENT REPLY mode, tracking configuration, connection flags, the reply chunk queue and the registry entry. This is report 1's recommendation (§5 items 1-4) and #209's direction (shapes 1 and 2 for the field split; `client-split.md`). Session-mutating commands (AUTH, HELLO, SELECT, CLIENT SETNAME/REPLY/TRACKING, RESET) keep running in C on an instance as session barriers and return a session delta (report 1 §5 item 2).

**What each instance keeps per connection** (a context, created lazily on first dispatch):
- `serverDb*` for the session's dbid in this instance (report 1 §1.4; `createDatabaseIfNeeded` is per instance after #214);
- MULTI queue and WATCH keys whose slots this instance owns;
- blocking state (`bstate`) for keys it owns; BLPOP keys are single-slot (section 4);
- sharded pub/sub subscriptions for its slots (`pubsubshard_channels` is per instance in #214, `engine-struct.md` judgment calls);
- tracking entries for its keys (`TrackingTable` moved into the instance in #214);
- per-context stats published to the registry for CLIENT LIST (report 1 §2 first row).

**ACL.** Decision 3 binds: port 207's copy-on-write snapshot (`207:src/rust_io_precheck.c:41-53` for `pcSnapshot`, `pc_current`, the hazard array; `:115` `pcBuild`) instead of #215's drain-on-change. That is also the fix for the cross-instance use-after-free: `ACLFreeUserAndKillClients` rewrites `c->user` for every client and frees the user immediately (`src/acl.c:577-597` [V]), which is unsafe once instances run in parallel. Users become immutable, refcounted versions; contexts hold a version and re-read on epoch change. 207's snapshot is limited to 64 users, no roles, no module users (report 1 §1.4); the port must lift those limits or fall back to the slow path for them.

**Dispatch ordering.** Today a connection's commands run strictly in order, and a blocked command stops the rest of its pipeline (report 1 §2, consequence 1). Under one node, pipelines mix instances almost always: with random keys at -P 16, 100% of batches span instances for N = 2, 4, 8, and (N-1)/N of keyed commands switch instance (`~/chunky/reports/mixed-pipelines.md`, results table). Three modes:

| Mode | Keeps | Cost |
|---|---|---|
| Drain on switch (one instance in flight per session; report 1 §5 item 3) | Everything: execution order, blocked-command barrier, MULTI, pushes | Every switch is a cross-thread round trip, (N-1)/N of commands on random keys. Unmeasured at N>1 on real instances. |
| Chained dispatch (report 2 §1 option C) | Execution order across instances, without a full drain: instance B runs a session's next command only after the previous one completed on A | One completion signal per switch; no prior art (report 2) |
| Eager dispatch, replies reordered (#213's host, 207's api-analysis) | Reply order only | Measured in #213: 15-18% loss vs unmixed. Other clients can observe a pipeline's writes out of order; commands behind a blocked BLPOP run early. |

This doc does not pick one. The sibling job `single-node-ordering` measures them; its report (`~/chunky2/reports/single-node-ordering.md`) did not exist when this doc was written. The design below assumes only that the chosen mode keeps execution order for a session when it matters for correctness (blocked commands, MULTI, session barriers, pushes). Eager dispatch as a per-connection opt-in is compatible with everything here.

## 4. Commands that span instances, in cluster mode

Cluster mode already rejects most cross-slot work, so most commands are single-instance by construction. The probe confirms which are not (`probe.out`, `probe_watch.out`, node owning all slots; key slots a=15495, b=3300).

| Command class | Today in cluster mode | Under N instances |
|---|---|---|
| Multi-key commands (MSET, DEL, SUNIONSTORE, RENAME, ...) | `-CROSSSLOT` unless one slot (`src/cluster.c:1108-1110`, reply `:1350`). Probe: `MSET a 1 b 2` and `DEL a b` give CROSSSLOT. | Single instance by construction. |
| MULTI/EXEC queued commands | EXEC slot is the common slot of the queue, CROSSSLOT otherwise (`src/cluster.c:1089-1105`). Probe: MULTI SET a, SET b, EXEC gives CROSSSLOT. | Single instance by construction. A transaction of only keyless commands has slot -1 and runs on the session's home instance. |
| **WATCH on a slot other than the transaction's** | Accepted. `watchCommand` (`src/multi.c:682`) has no slot coupling to EXEC; `multi.c` records only queued commands' slots (`:115`). Probe: c1 `WATCH a`, c2 `SET a`, c1 `MULTI; SET b z; EXEC` returns `*-1`, so a watch on slot 15495 aborts a transaction on slot 3300. | **Cross-instance.** The watched key's instance sets the dirty flag; the EXEC instance must read it atomically with running the queue. Options: P on {watched instances} ∪ {EXEC instance}; or fail EXEC when any watched key is on another instance (a new error that depends on N and the slot map). Recommend P, since it is rare and the cost is one park. |
| Blocking commands (BLPOP, BLMOVE, BZPOPMIN, XREAD BLOCK) | All keys one slot. Probe: `BLPOP a b` gives CROSSSLOT. | Single instance by construction. Two cross-instance effects remain: a pipeline behind a blocked command (handled by the dispatch mode, section 3), and a slot moving between instances while a client blocks on it (section 7). |
| EVAL with a shebang, no flags; FCALL without flags | Every key one slot; checked per call (`src/module.c:7062-7072`). Probe: shebang script touching a and b fails with "Script attempted to access keys that do not hash to the same slot". | Single instance by construction. |
| **Compat-mode EVAL (no shebang)** | `SCRIPT_ALLOW_CROSS_SLOT` (`src/script.c:239-241`). Only "non local key" is checked (`src/module.c:6964-6995`). Probe: compat EVAL writes a and b, returns OK. | **Cross-instance.** Any slot the node owns. |
| **Scripts and functions with `allow-cross-slot-keys`** | Same flag (`src/script.c:239-241`). Probe: shebang EVAL with the flag and an FCALL of a flagged function both write a and b. | **Cross-instance.** |
| Module commands, `VM_Call` | Locality check against the node (`src/module.c:6964-6995`); module getkeys runs on IO threads today (valkey-io/valkey#5054). | Cross-instance for modules that call keys outside their declared slot. Module commands require the opt-in (section 9). |
| `CLUSTER COUNTKEYSINSLOT`, `GETKEYSINSLOT` | | Route by the slot argument. |

**Scripts.** Running every compat EVAL under G would cost about 0.29x of one instance at N=8 in report 2's model, and Dragonfly measured 0.10x on BullMQ under its global-lock fallback (report 2 §2.3). But in cluster mode, cluster-aware callers already keep a script's keys in one slot or the script fails on multi-node clusters; cross-slot access works today only by the accident of co-location. Recommend report 2's **P plus escalation**:

1. Dispatch the script to the instance of its first declared key (or the home instance if none).
2. At the per-call check (`src/module.c:7062-7072`), compare the key's instance with the running one, whether or not `scriptAllowsCrossSlot()` (`src/script.c:342`).
3. On the first access to another instance: if the script has not written yet (the existing write-dirty flag), abort, re-run under G, and remember the SHA (or function name) so later runs start under G. If it has written, fail with the existing cross-slot error text, which cluster mode already returns mid-script.

That keeps every script that works on a one-instance node working; the cost lands only on scripts that actually cross instances. Strict mode (always error) is a config, not the default.

**Measuring demand.** #210's `chunky_script_*` counters (hooks at `src/script.c:247` and `src/module.c:6855` on that branch) count undeclared and cross-slot script keys. Add a WATCH-across-slots counter to the same section before building P for WATCH.

## 5. Node-level commands and state

Treatments: **IO** handled in the IO layer from the session registry; **C** control plane, node-global state; **R** route to one instance; **A** fan out, aggregate, no barrier; **G** all-instance barrier; **B** broadcast to every instance with acks before replying. The command table already has request and response tips for most of these (`src/commands/*.json`; listed per row), which the IO layer can use as its routing table for A rows.

| Command / state | Tip | Treatment | Semantics and evidence |
|---|---|---|---|
| `INFO` | ALL_SHARDS, SPECIAL | A + C | `genValkeyInfoString` `src/server.c:6487`. server, cluster, persistence, replication, cpu from C; clients from IO; memory, stats, commandstats, errorstats, keyspace summed per instance. New `INFO instances` section with one line per instance (slots, keys, used memory, ops/sec). |
| `DBSIZE` | ALL_SHARDS, AGG_SUM | A | Sum of instance counts. Not a point-in-time cut; today's value is also stale by the time the client reads it. |
| `KEYS` | ALL_SHARDS | A | `keysCommand` `src/db.c:977`. In cluster mode no command moves a key between slots, so per-instance results have no duplicates or gaps except during an intra-node slot move, which holds that slot (section 7). Instances scan in parallel; faster than today. |
| `SCAN` | SPECIAL | R by cursor | The cursor already carries the hashtable index in the low bits: `(ht_cursor << num_hashtables_bits) \| didx` (`src/kvstore.c:142`, decode `:147-148`). Probe: cursors 86, 156, 187 are slot indexes (`probe.out` §8). The IO layer routes by `cursor & 16383` to that slot's instance. An instance that reaches the end of its last owned slot returns a cursor for the next slot, owned elsewhere. Same guarantees as cluster SCAN today. CLUSTERSCAN's fingerprint (`src/cluster.c:1703`) is node-level and unchanged. |
| `RANDOMKEY` | ALL_SHARDS, SPECIAL | A | Pick an instance weighted by published key count, then its local RANDOMKEY (`src/db.c:965`). Today's choice is already approximate (report 2 §2.4). |
| `FLUSHALL`, `FLUSHDB` | ALL_SHARDS, ALL_SUCCEEDED | G | `src/db.c:897`, `:872`. One barrier record in the merged log (section 6); dirties watchers and invalidates tracking per instance; only one instance sends the RESP3 "invalidate all" (report 1 §2, tracking row). ASYNC frees per instance after the barrier. |
| `SWAPDB` | | n/a | Already refused in cluster mode (`src/db.c:1929-1933`). |
| `CONFIG GET` | | C | Config stays in `server` (#214 kept G-cfg global). |
| `CONFIG SET` | ALL_NODES | C + B | `src/config.c:901`. Apply on C, then broadcast apply callbacks that touch instance state (maxmemory budgets, hz, commandlog thresholds live in `server` after #214). Reply after acks so the next command on any instance sees the value. |
| `CONFIG REWRITE` | ALL_NODES | C | One config file (`src/config.c:4003`). |
| `ACL SETUSER/DELUSER/LOAD`, `ACL LOG` | ALL_NODES | C | New snapshot version, epoch bump (section 3). DELUSER kills sessions through the IO registry. ACL LOG appends must be thread-safe (report 8 §6 item 5). |
| `CLIENT LIST`, `CLIENT INFO` | | IO | `getAllClientsInfoString` `src/networking.c:5141`. Session fields from the registry, context fields (multi, watch, sub counts, omem, flags) from published per-instance stats; may be one publish stale (report 1 §2). |
| `CLIENT KILL` | | IO + B | `src/networking.c:5956`. Filter in the registry, close, reply after context-holding instances ack, so a killed BLPOP client cannot consume a later push (report 1 §2). |
| `CLIENT PAUSE`, `UNPAUSE` | | IO + B | `pauseActions` `src/networking.c:7042`, checked in `processCommand` `src/server.c:5034`. The IO layer stops admitting; each instance applies the pause at its next command boundary and stops its expiry and eviction; OK after all acks. Failover and slot export use the same primitive (section 6, 7). |
| `CLIENT NO-EVICT`, client eviction | | IO | Session flag; the IO layer owns eviction buckets. Eviction becomes one IO loop late (report 1 §6 decision 6). |
| `CLIENT UNBLOCK` | | R | To the instance the IO layer recorded as holding the blocked context. |
| `CLIENT TRACKING` | | IO + per instance | Session config in IO. Default mode: each instance tracks its own keys (TrackingTable per instance after #214), so a read and the later invalidating write are on one instance. BCAST registers on every instance. Pushes go through the session's reply queue, ordered after the reply they follow. |
| `MONITOR` | | IO | Monitor sessions in the registry; each instance formats its lines (`replicationFeedMonitors` `src/replication.c:1002`) and pushes them to each monitor. Lines from different instances interleave in arrival order; timestamps may go backwards across instances. |
| Global pub/sub | | R (pub/sub home) | One registry for the node (`server.pubsub_channels` `src/server.c:3188`, global after #214). SUBSCRIBE/PSUBSCRIBE/PUBLISH route to a designated home instance. Inbound PUBLISH from the bus (`clusterPropagatePublish` `src/cluster_legacy.c:5771`, called `src/pubsub.c:664`) is delivered there. One home keeps "subscribers receive messages in publish order" for one channel. Throughput is one thread's, as today. |
| Sharded pub/sub | | R by slot | `pubsubshard_channels` is a per-slot kvstore in cluster mode (`src/server.c:3190`) and per instance after #214. |
| Keyspace notifications | | per instance → home | `notifyKeyspaceEvent` `src/notify.c:107` runs on the key's instance. The pub/sub part is forwarded to the pub/sub home asynchronously: arrives later, never earlier. Module subscribers run synchronously on the instance thread (needs the opt-in, section 9). |
| `WAIT`, `WAITAOF` | ALL_SHARDS, AGG_MIN | C | `src/replication.c:5706`, `:5743`. With the merged log, `c->woff` (`src/server.c:4443`) stays one scalar, taken from the reservation. The IO layer or control plane parks the session until acks pass it. No per-instance fan-out. |
| `SCRIPT LOAD`, `SCRIPT FLUSH`, `SCRIPT EXISTS` | ALL_NODES | C + B | Script bodies in a node-global cache; each instance compiles lazily into its own Lua state. The Lua module's `engine_ctx` (`src/modules/lua/engine_lua.c:480`, left global by #214) must become per instance. FLUSH is G. |
| `SCRIPT KILL`, `FUNCTION KILL` | ALL_SHARDS | R | To the instance running the busy script. Only clients routed to that instance see BUSY (report 2 §2.3), which is a visible improvement. |
| `FUNCTION LOAD/DELETE/FLUSH/RESTORE` | ALL_SHARDS | C + G | Library registry node-global; one barrier record in the merged log; each instance compiles. |
| `COMMANDLOG`, `SLOWLOG` | ALL_NODES | A | `commandlogCommand` `src/commandlog.c:223`. #214 split thresholds (global) from entries (per instance). GET merges by time; LEN sums; RESET is B. Entry ids come from one node-wide atomic counter so ids stay unique and monotonic. |
| `LATENCY` | ALL_NODES, SPECIAL | A | `src/latency.c:682`. Per-instance samples merged, max per event. |
| `MEMORY USAGE` | | R by key | |
| `MEMORY STATS`, `DOCTOR`, `MALLOC-STATS`, `PURGE` | ALL_SHARDS | A / C | Sums plus per-instance budgets (section 8); allocator stats and PURGE are process-wide. |
| `DEBUG` | | R / G | Keyed subcommands (OBJECT, DIGEST-VALUE) route by key. RELOAD, RESTART, JMAP, SLEEP and DIGEST are G, because tests use DEBUG SLEEP to stall the whole server. |
| `SHUTDOWN` | | C + G | `prepareForShutdown` `src/server.c:5126`: pause, final merged save, wait replicas, exit the process. |
| `SAVE` | | G | One merged RDB (section 6). |
| `BGSAVE` | | C | Forkless: per-instance iterators at a logical cut, no stall (report 4 §3). Fork: G for the duration of `fork()` only. |
| `BGREWRITEAOF` | | C + G | Fork barrier until a forkless AOF rewrite exists (report 4 §3, fallbacks list). |
| `PING`, `ECHO`, `TIME`, `HELLO`, `AUTH`, `SELECT`, `RESET` | | IO / home | Session commands; HELLO/RESET/SELECT are session barriers (section 3). |

## 6. Replication and failover as a unit

**One merged stream per node** (report 4 option A, recommended there for exactly this presentation). Each instance serializes its propagation into a local batch and at batch end reserves a range in one shared log with one atomic fetch-add, copies the batch in and publishes. Offsets stay scalar and are known synchronously, so `c->woff` (`src/server.c:4443`), WAIT/WAITAOF, PSYNC `(replid, offset)`, the backlog, `REPLCONF ACK`, the bus offset (`src/cluster_legacy.h:369`) and failover rank (`src/cluster_legacy.c:6127`) keep today's shape.

Why not N streams on one link: every scalar above becomes a vector, including the fixed-layout bus field asserted at byte 32 (`src/cluster_legacy.h:403`); replicas lose cross-slot prefix consistency within a node; a replica with a different N must re-route N streams; intra-node rebalancing becomes a cross-stream handoff every replica follows (report 4 §1, option B). Its gains (no shared counter, N parsers) can be had inside the merged design with a framed wire format behind a `REPLCONF capa` (report 4 recommendation 1).

**What gets a barrier record.** FLUSHALL, FLUSHDB, FUNCTION *, SCRIPT FLUSH and scripts that escalated to G (section 4). Everything else is per-instance and slot-disjoint, so any interleaving that keeps each instance's own order is valid (report 4 §1).

**Replicas.** A replica is any node that can route by slot: a chunky node with the same N, a different M, or a plain one-instance node. It parses the stream, computes each command's slot and hands it to its own instance for that slot, and reports a contiguous applied watermark (report 4 option A, replica side). Today's invariant "a replica proxies exactly the bytes it applied" (`src/replication.c:848-856` [V]) holds, so sub-replicas and the replica's backlog are unchanged. Recommend: allow any M, since it costs nothing extra in the merged design and lets operators replace a chunky node with a plain one. The ceiling is one parser per primary link; the framed format removes it later.

**Failover.** Whole node, unchanged protocol. An instance thread crash is a process crash. The bus sees one node fail and one election runs. Measured in round one: 7.42 s median to recover 1 failed primary against 8.40 s for 8 co-hosted primaries at a 5 s timeout, 23.57 s vs 24.88 s at 15 s (`topology.md`, Q3). So one-node presentation saves 1.0-1.3 s per host loss. Manual failover pauses writes (`pauseActions(PAUSE_DURING_FAILOVER, ...)`, `src/cluster_legacy.c:4842`), which becomes the IO-layer pause plus B (section 5).

**AOF.** One merged AOF written from the same log, one fsync per group commit, one `fsynced_reploff` for WAITAOF (report 4 recommendation 2). Format and manifest unchanged.

**RDB.** One file, one `repl-offset` aux field. Forkless first: each instance runs its own START iterator at a logical cut (report 4 §3, "A logical cut instead of a stop-the-world"); #214 already made the bgiteration state per instance (12 statics, `engine-struct.md`). Fork remains for what forkless cannot do: modules without `HANDLE_FORKLESS`, active slot import (`src/rdb.c:1866-1876`), AOF rewrite, dual-channel, compression. A fork parks every instance for the duration of `fork()` only, with a timeout-and-retry so one instance in a long script defers the save instead of stalling the rest.

## 7. Slot migration

**Between chunky nodes (atomic slot migration).** The wire protocol is unchanged. Inside the node, a migration job binds to instances:

- **Export.** The source instance is the one owning the requested ranges; a request spanning instances becomes one export job per instance, one reported job id per job. Today the export snapshot forks (`serverFork(CHILD_TYPE_SLOT_MIGRATION)`, `src/cluster_migrateslots.c:1736`) and the finalize step pauses **all** client writes on the node (`pauseActions(PAUSE_DURING_SLOT_MIGRATION, ..., PAUSE_ACTIONS_CLIENT_WRITE_SET)`, `src/cluster_migrateslots.c:1564`). Under N instances, scope the pause to the exporting instance (the IO layer holds writes only for that instance's slots), and replace the fork with an instance-scoped forkless iterator. Until that iterator exists, an export fork parks every instance (section 6).
- **Import.** The target node picks the importing instance: the one whose range is adjacent, else the one with the most headroom (section 8). The import client is a context on that instance. `clusterIsAnySlotImporting` currently forces fork-based saves (`src/rdb.c:1873`); that stays per node.

**Between a node's own instances (rebalancing).** New, invisible to clients and replicas. Recommend a **hashtable handoff**, not internal ASM:

1. The IO layer starts holding new commands for the slot (they queue, they do not fail).
2. P parks the source and destination instances at a command boundary.
3. Move each db's per-slot hashtables for keys and expires (cluster mode keeps one hashtable per slot, `src/server.c:3046-3055`) from the source kvstore to the destination: pointer moves, no key copies. Re-home the slot's side state: sharded pub/sub channels, tracking entries, blocked clients on its keys (re-dispatch the blocked command to the destination with its remaining timeout instead of the MOVED that `clusterRedirectBlockedClientIfNeeded`, `src/cluster.c:1384`, sends today), watched keys, slot stats, and the per-(db, slot) memory counter (section 8).
4. Swap the instance part of the slot map, resume both instances, release the held commands to the destination.

No log record is needed: the merged stream carries no instance ids, and each replica routes by its own map. The cost is proportional to the number of side-state entries, not keys. Internal ASM (serialize, replay) would reuse tested code but copies every key and runs a migration through the merged log for no visible effect. A forkless iteration in progress on either instance must finish or cancel first (the iterator owns its kvstore's position).

## 8. Memory

**Per instance under one maxmemory** (report 5 §2.5, recommended there): maxmemory stays a node-wide limit. Each instance checks its own counter sum against a local budget per command, with no cross-instance reads; the control plane re-derives budgets at cron rate from usage share, and an instance over budget borrows from global headroom before it evicts. Eviction is per instance, since an instance can only evict its own keys (`evictionPoolPopulate` takes a db and samples its kvstore, `src/evict.c:115-122`). Memory no instance owns (client buffers in the IO layer, replication backlog, the merged log, Lua, modules) is counted globally and spread over budgets by share, as `getMaxmemoryState` (`src/evict.c:271`) already counts it against keys today. Accounting is per-(thread, instance) counters selected by the thread-local instance (report 5 TL;DR layer 2), plus ownership rules at the IO-to-instance hand-off: large argv values are re-homed into the destination instance's domain after the slot is known (report 5 §3, `src/object.c:507-512`).

**Per tenant** (report 6 §6): data bytes per (db, slot) in kvstore hashtable metadata (open valkey-io/valkey#3966), summed per tenant by the control plane; client buffer bytes charged per principal in the IO layer. Because the counter lives in the per-slot hashtable's metadata, it moves with the hashtable in section 7's intra-node handoff for free. The tenant unit itself (database, prefix or slot range) is report 6's open decision, not this doc's.

## 9. Modules

Add `VALKEYMODULE_OPTIONS_HANDLE_MULTI_INSTANCE`, gated like `moduleAllModulesHandleForkless` (`src/module.c:7909`) and its siblings `HANDLE_ATOMIC_SLOT_MIGRATION` (`src/module.c:7892`). Fallback: if any module loaded at startup lacks it, start with one instance and log which module forced it, the way forkless falls back (`src/config.c:2500`). A runtime `MODULE LOAD` of a module without the flag fails when N>1. This is report 8's option D over B with C1 as fallback (report 8 §6).

Under one-node presentation, report 8's option A (one module copy per instance) is worse than under N nodes: a second init in one process fails with "Module name is busy" (`src/module.c:13899`), and a module that fans out across shards (valkey-search) would see one node yet hold only one instance's keys. So the engine-aware API (report 8 §6 items 2-4: instance on every context, defined semantics for the 17 cross-engine functions, `RunOnInstance`/`RunOnAllInstances`) is the only path. None of the four bundle modules sets the forkless or ASM opt-ins today (report 8 §4), so bundle users stay at N=1 until they are ported; Lua (`engine_lua.c`) is the first port, since EVAL needs it.

## 10. Stages

Each stage merges into `epic/chunky-shards` on its own and keeps the full suite passing at N=1. N>1 is refused until stage 4.

| # | Stage | Depends on | Proves | Exit criteria |
|---|---|---|---|---|
| 1 | Land #214 (instance struct) after the defrag-latency rerun on a quiet box; hoist the function-local statics it left (`databasesCron` cursors, expire state; `engine-struct.md` decision 3), `module_pipe`, Lua `engine_ctx` | | All per-instance state is reachable from `inst` | Full suite at N=1; `--io-threads` suite (only 30/177 files ran in round one); instructions per request within #214's +1.8% |
| 2 | ACL copy-on-write snapshot ported from 207 onto #215; immutable refcounted users, no 64-user cap | | ACL changes are safe with parallel readers, without draining | ACL DELUSER/SETUSER under GET/SET load with precheck on IO threads, no drain call on the ACL path, ASan clean; #215's negative controls still fail without the epoch check |
| 3 | Control plane split at N=1: cluster bus, config, ACL, replication readers, persistence scheduling on their own thread; published slot map for the IO layer | 1 | The node-global/instance boundary is real, not nominal | Full suite and `runtest-cluster` at N=1; slot map read on IO threads with TSan clean |
| 4 | N instances in cluster mode, slot routing in the IO layer, ordering mode from `single-node-ordering`, single-instance commands only; every row of section 5 as G; modules and persistence refused when N>1 | 1-3 | The central bet: one node, N instances, shared IO threads | GET/SET -P 1/16 at N=1, 2, 4 against a K-process cluster on the same cores in the same session; mixed-pipeline cost compared to #213's 15-18%; new `tests/unit/cluster/chunky-*.tcl` at N=4; MOVED never names self |
| 5 | Node-level commands per section 5 (A, IO, B rows replace G); IO-layer session registry, pub/sub home, MONITOR, tracking pushes | 4 | Clients cannot tell N | Existing introspection, pubsub, tracking, client-eviction suites pass at N=4 |
| 6 | Merged replication log; replica routes by its own map (any M); WAIT/WAITAOF; failover as a unit | 4 | One stream per node with scalar offsets | Replication and failover suites at N=4 primary with M=1 and M=4 replicas; PSYNC partial resync after restart; failover time vs N=1 |
| 7 | Persistence: merged AOF, forkless per-instance RDB at a logical cut, fork barrier fallback | 6 | Durability without stalls | AOF and RDB suites at N=4; `DEBUG RELOAD` digest equal; BGSAVE latency with one instance in a long command |
| 8 | Cross-instance scripts (P plus escalation) and WATCH across instances (P) | 4 | Compat EVAL and `allow-cross-slot-keys` keep working | Probes 2, 4, 5, 6 in `probe.sh` pass at N=4 with keys on different instances; escalation counted in INFO |
| 9 | Atomic slot migration with instance-scoped export and pause | 6, 7 | Migration between chunky nodes | ASM suite with chunky source and target; writes to other instances not paused during finalize |
| 10 | Intra-node rebalancing by hashtable handoff | 4, 5 | Hot-slot moves the cluster never sees | Move a slot under load: no MOVED, ASK or TRYAGAIN; blocked clients served; replica digest equal |
| 11 | Per-instance memory budgets under one maxmemory; per-(db, slot) counters | 4 | Eviction with skew | maxmemory suite at N=4; skewed load evicts from the hot instance without idle instances evicting |
| 12 | Module opt-in flag; Lua first, then json and bloom | 4 | Modules at N>1 | Module API suite at N=1; opted-in test module at N=4; non-opted module forces N=1 with a log line |

Stages 5, 6, 8, 11 and 12 can run in parallel after 4. Stage 4 is the go/no-go: if N instances behind one IO layer do not beat a K-process cluster on the same cores by enough to pay for stages 5-12, stop there.

## Decisions for madolson

1. **Dispatch ordering default** (section 3). Waits on `single-node-ordering`. Proposal: whichever mode keeps execution order with the lowest measured cost; eager dispatch only as a per-connection opt-in.
2. **Cross-instance compat EVAL** (section 4). Proposal: P plus escalation to G, remembered per SHA; strict error as a config.
3. **WATCH on another instance** (section 4). Proposal: P at EXEC. Alternative: fail EXEC, which makes behavior depend on N.
4. **Replica shape** (section 6). Proposal: any M, including plain nodes.
5. **Fork at N>1** (sections 6, 7). Proposal: allow behind a timed barrier, prefer forkless; revisit once forkless covers AOF rewrite and slot export.
6. **Intra-node rebalancing** (section 7). Proposal: hashtable handoff, not internal ASM.
7. **Global pub/sub owner** (section 5). Proposal: one home instance in C; the IO layer would scale PUBLISH but moves subscribe semantics into Rust.
8. **Instance crash** (section 1). Proposal: whole process, node fails over. Restarting one instance in place needs a partial-node state the protocol lacks.
9. **KEYS and DEBUG SLEEP** (section 5). Proposal: KEYS fans out without a barrier; DEBUG SLEEP is G.
10. **Name of the module flag** (section 9). Proposal: `HANDLE_MULTI_INSTANCE`.

## Strongest arguments against one-node presentation

Round one's topology report recommended N ordinary nodes. Its case, stated fairly:

- **Every mechanism in sections 3-7 exists only to hide instances.** With N nodes, each connection binds to one instance and the session split, registry, ordering mode, node-level fan-out, merged log, script escalation and hidden rebalancing are all unnecessary; every protocol stays per node (`topology.md`, Q4 last paragraph; report 1 §7).
- **Mixed pipelines cost throughput.** #213 lost 15-18% on fully mixed pipelines and raised engine cycles per GET by 63%; Garnet's in-process shards behind one network layer got 9 Mops/s against 17.1 for 64 client-routed processes (report 3, via the synthesis). The only measured multi-instance scaling, 4.2x at 4x4 cores, was client-routed processes (`207:design-docs/rust-io-layer.md:260-261`). #213 also found no host configuration that beat the best process layout on the same cores (4.70M vs 3.63-3.80M GET).
- **One stream per node has one parser per replica link** and one log writer path (report 4 §1, throughput row). N nodes scale replication linearly.
- **Failover concentrates load.** A one-node chunky shard's replica must host all N instances, so a host loss moves the whole host's load onto one other host; N nodes can spread replicas across hosts (`topology.md`, Q3, inferred).
- **Fork stalls every instance** (report 4 §3), and one process cannot stagger BGSAVE across instances the way K processes can.
- **Modules stay at N=1** until ported; with N nodes, option A (one copy per node process) works today (#211).

What tips it the other way: client connections and topology refresh multiply by N under N nodes (glide holds 2 eager connections per node, Jedis pools up to 8, go-redis 5xGOMAXPROCS, Lettuce refreshes from every node; `topology.md`, Q1), which undercuts the shared-IO-thread argument for chunky shards; one-node keeps iovalkey pipelines working across the host's slots; and host-loss recovery is 1.0-1.3 s faster with one election.

## Verified vs inferred

- [V] Every `file:line` above at `686abfe75`, re-read for this doc; the probe results in `~/chunky2/data/single-node-spec/`.
- [V, from round one] #213, #214, #210 and topology measurements, taken from their reports and raw data paths cited there; not re-measured here.
- [I] All costs of mechanisms not built yet: chained dispatch, P, G, the merged log, hashtable handoff, the process design's shared-segment copy.
- Not tested: anything at N>1 on real instances. No such build exists yet; stage 4 is where it starts.
