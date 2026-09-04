# Regression tests for defects in the forkless snapshot work (valkey-io/valkey#4460,
# merged to unstable as acba5ddd0).  Each test starts its own server, and the two
# scenarios whose failure depends on allocator or scheduling luck run as a loop of
# rounds, each round on a freshly started server so heap state does not carry over.
#
#   ./runtest --single unit/forkless-bugs
#
# Each block states the invariant it protects, the code that violates it, and the
# observed failure mode.  Tests that crash the server leave a crash report in the
# server log, which the harness reports separately from the failed assertion.

# Fresh-server rounds for the one scenario that is still a soak.  Raise when
# hunting; the deterministic tests are unaffected.
set ::forkless_quicklist_rounds 5

# Turn a dead connection into an assertion failure instead of an exception, so
# one crashing round does not abort the rest of the file.
proc assert_alive {r ctx} {
    if {[catch {$r ping} res]} {
        fail "server stopped responding after $ctx: $res"
    }
    assert_equal {PONG} $res
}

# Best-effort cleanup: never let teardown mask the assertion we care about.
proc forkless_cleanup {r} {
    catch {$r config set rdb-key-save-delay 0}
    catch {$r bgsave cancel}
    catch {wait_for_condition 100 100 {[status $r rdb_bgsave_in_progress] == 0} else {}}
}

# Start a forkless BGSAVE and wait for it to be running.  rdb-key-save-delay
# keeps the background serializer slow so the iterator stays mid-scan with
# entries checked out to the background thread.
proc start_slow_forkless_save {r delay_us} {
    $r config set rdb-key-save-delay $delay_us
    $r config set bgsave-default-method forkless
    $r bgsave
    wait_for_condition 50 100 {
        [status $r rdb_bgsave_in_progress] == 1 &&
        [status $r rdb_current_bgsave_type] eq "forkless"
    } else {
        fail "forkless bgsave did not start"
    }
}

proc wait_forkless_save_done {r} {
    $r config set rdb-key-save-delay 0
    wait_for_condition 300 100 {
        [status $r rdb_bgsave_in_progress] == 0
    } else {
        fail "forkless bgsave did not finish"
    }
    assert_equal "ok" [status $r rdb_last_bgsave_status]
}

proc populate_uncloneable_keys {r count} {
    # >BGITER_MAX_CLONE_ITEM_BYTES (512) so tryCloneDbEntry() refuses to clone
    # and the entry is handed to the background thread by reference, which is
    # what makes a concurrent writer block.
    set big [string repeat x 2000]
    for {set i 0} {$i < $count} {incr i} {
        $r set key$i $big
    }
}

#-------------------------------------------------------------------------------
# 1. EXEC without MULTI dereferences a NULL c->mstate
#
# call() runs bgIteration_blockClientIfRequired() (src/server.c:4056) before
# execCommand() gets to reject "EXEC without MULTI" (src/multi.c:202).
# isWriteCmd() counts execCommand as a write, so bgiteration.c:2345 dispatches to
# expediteKeysForMultiExec(), which reads c->mstate->count
# (src/bgiteration.c:1935).  c->mstate is lazily allocated (src/server.h:1373)
# and NULL on a client that never sent MULTI.  src/server.c:4600 already guards
# this exact pair.
#
# Observed: SIGSEGV in expediteKeysForMultiExec, argc:1 argv[0]:"exec".
#-------------------------------------------------------------------------------
start_server {tags {"forkless external:skip"} overrides {forkless-infrastructure-enabled yes save {}}} {
    test "EXEC without MULTI during a forkless bgsave does not crash the server" {
        populate_uncloneable_keys r 20
        start_slow_forkless_save r 10000000

        # Separate connection: on a buggy build this kills the server, and we
        # want the liveness assertion below to be the reported failure.
        set rd [valkey_client]
        catch {$rd exec} exec_err
        catch {$rd close}

        assert_alive r "EXEC without MULTI"
        forkless_cleanup r
        assert_match "*without MULTI*" $exec_err
        set _ {}
    } {} {needs:debug}
}

#-------------------------------------------------------------------------------
# 2. RENAME over an existing key leaves a dangling pointer in early_iterate_entries
#
# Pointers are inserted into early_iterate_entries without a refcount
# (src/bgiteration.c:1090) and removed only when the scan reaches them (:1049),
# on a propagated DEL/UNLINK (:2613), or on reallocation (:2688).  RENAME does
# the free and a same-sized reallocation inside one command:
#
#   1. expediteKeysForWrite expedites src and dst, so both pointers are in the
#      table.  The client blocks and resumes once both entries come back; the
#      pointers stay in the table.
#   2. renameGenericCommand dbDelete()s dst.  Its dbEntry is freed, and RENAME
#      propagates as RENAME, so :2613 never cleans the pointer up.
#   3. dbAdd(dst, src_robj) -> objectSetKeyAndExpire() allocates a replacement of
#      exactly the same size, and the allocator hands back dst's just-freed
#      address.
#   4. bgIteration_updateDbEntryPtr(src_robj, new) deletes src's pointer, then
#
#          bool wasAdded = hashtableAdd(it->early_iterate_entries, new);
#          serverAssert(wasAdded);          /* src/bgiteration.c:2692-2693 */
#
#      hashtableAdd returns false only for a duplicate key (hashtable.c:1631) and
#      insert() cannot fail, so the collision is with dst's dangling pointer.
#
# Equal-length key names and no TTLs make the replacement the same size class as
# the entry just freed, which is what makes this deterministic rather than a
# heap-layout lottery.
#
# Observed on upstream/unstable bb741667d, on the first RENAME:
#   ==> bgiteration.c:2693 'wasAdded' is not true
#   0 bgIteration_updateDbEntryPtr  1 objectSetKeyAndExpire  2 dbAddInternal
#   3 renameGenericCommand          4 call
#
# The same dangling pointer also makes iteratorHasPassedKey() lie.  A defragged
# dbEntry landing on such an address keeps its epoch (objectCopyMetadata copies
# it and defrag does not signal a modification), so feedIterator skips a key that
# was never modified and it goes missing from the snapshot.
#-------------------------------------------------------------------------------
start_server {tags {"forkless external:skip"} keep_persistence true overrides {
    forkless-infrastructure-enabled yes
    save {}
    lazyfree-lazy-server-del no
    lazyfree-lazy-user-del no
    lazyfree-lazy-expire no
}} {
    test "RENAME over an existing key during a forkless bgsave does not leave a dangling early-iterate pointer" {
        set n 600
        for {set i 0} {$i < $n} {incr i} {
            set suffix [format %05d $i]
            r set aa$suffix v$suffix
            r set bb$suffix w$suffix
        }

        # The point-in-time contract: whatever is in the keyspace now is exactly
        # what the snapshot must contain, whatever the churn does.
        set expected_keys [lsort [r keys *]]

        start_slow_forkless_save r 2000

        set failed_at ""
        for {set i 0} {$i < $n} {incr i} {
            set suffix [format %05d $i]
            if {[catch {r rename aa$suffix bb$suffix} err]} {
                set failed_at "rename #$i: $err"
                break
            }
            if {$i % 50 == 0 && [status r rdb_bgsave_in_progress] == 0} break
        }
        if {$failed_at ne ""} { fail "server stopped responding at $failed_at" }
        assert_alive r "RENAME churn"

        # The snapshot is a point in time before any of the renames, so it must
        # still hold every key that existed when BGSAVE started.
        wait_forkless_save_done r
        restart_server 0 true false
        assert_equal $expected_keys [lsort [r keys *]]
        set _ {}
    } {} {needs:debug}
}

#-------------------------------------------------------------------------------
# 3. Quicklist compression state races the background serializer
#
# bgIteration blocks writes only (src/bgiteration.c:2317) and pauseRehashing()
# covers only OBJ_ENCODING_HASHTABLE / OBJ_ENCODING_BTREE (:741).  Read commands
# mutate quicklist nodes: __quicklistDecompressNode zfree()s the quicklistLZF and
# sets encoding=RAW (src/quicklist.c:261-263); __quicklistCompressNode zfree()s
# node->entry and sets encoding=LZF (:234-236).  The background thread reads
# exactly those fields at src/rdb.c:917-925.
#
# Two failure modes: read a freed quicklistLZF, or take the RAW branch after the
# main thread recompressed and have rdbSaveRawString() over-read node->sz bytes
# from an (8 + lzf->sz) buffer straight into the RDB.
#
# The window is narrow - a single-key save finishes fast and rdb-key-save-delay
# only delays *between* keys - so this is a soak: 40 saves per server across
# several fresh servers.  Confirmed on upstream/unstable at bb741667d, roughly
# once per 100 saves:
#
#   crashed by signal: 11, Accessing address: 0x3e10ba55c2cefa38
#   0 crcspeed64little        1 rioGenericUpdateChecksum
#   3 rdbWriteRaw             4 rdbSaveObject
#   5 rdbSaveKeyValuePair     6 forklessSaveProcessor   <- background thread
#
# A wild read in the background serializer, which is the node->entry / node->sz
# mismatch above.  A pass is inconclusive, not evidence the race is absent; run
# it under ASAN when verifying a fix.
#-------------------------------------------------------------------------------
for {set round 1} {$round <= $::forkless_quicklist_rounds} {incr round} {
    start_server {tags {"forkless external:skip"} keep_persistence true overrides {
        forkless-infrastructure-enabled yes
        save {}
        list-compress-depth 1
        list-max-listpack-size 128
    }} {
        test "concurrent list reads during a forkless save do not corrupt quicklist nodes (round $round/$::forkless_quicklist_rounds)" {
            # Many nodes so all but the head and tail stay compressed.
            set val [string repeat z 120]
            set count 300000
            for {set i 0} {$i < $count} {incr i 1000} {
                set batch {}
                for {set j 0} {$j < 1000} {incr j} { lappend batch $val[expr {$i + $j}] }
                r rpush biglist {*}$batch
            }
            assert_equal $count [r llen biglist]
            assert_equal "quicklist" [r object encoding biglist]
            set before [debug_digest]

            set readers {}
            for {set i 0} {$i < 8} {incr i} {
                lappend readers [valkey_deferring_client]
            }

            r config set bgsave-default-method forkless
            for {set save 0} {$save < 40} {incr save} {
                if {[catch {
                    r bgsave
                    # Hammer interior (compressed) nodes while the background
                    # thread walks the same node list.  Pipeline a burst before
                    # each progress poll: the INFO round trip is expensive
                    # relative to the window we are trying to hit.
                    while {[status r rdb_bgsave_in_progress] == 1} {
                        foreach rd $readers {
                            for {set b 0} {$b < 25} {incr b} {
                                set idx [expr {int(rand() * ($count - 40))}]
                                $rd lrange biglist $idx [expr {$idx + 30}]
                                $rd lindex biglist [expr {int(rand() * $count)}]
                            }
                        }
                        foreach rd $readers {
                            for {set b 0} {$b < 50} {incr b} { catch {$rd read} }
                        }
                    }
                    assert_equal "ok" [status r rdb_last_bgsave_status]
                } err]} {
                    foreach rd $readers { catch {$rd close} }
                    fail "server stopped responding during save $save: $err"
                }
            }

            foreach rd $readers { catch {$rd close} }
            assert_alive r "concurrent LRANGE during forkless save"

            # Reload the RDB the forkless saves produced.  A node written with
            # the wrong length, or from a freed quicklistLZF, surfaces as a
            # digest mismatch or a load failure here.
            restart_server 0 true false
            assert_equal $count [r llen biglist]
            assert_equal $before [debug_digest]
            set _ {}
        } {} {needs:debug}
    }
}

#-------------------------------------------------------------------------------
# 4. A primary write to a key held by the replica's forkless save panics the
#    replica
#
# bgIteration_blockClientIfRequired() has no caller-type filter
# (src/bgiteration.c:2313).  blockClientInUseOnKeys() asserts !c->flag.replica
# (src/blocked.c:921), but the primary link client has flag.primary, so it
# reaches blockClient(c, BLOCKED_INUSE) and trips
#
#     serverAssert(!(isReplicatedClient(c) && btype != BLOCKED_MODULE &&
#                    btype != BLOCKED_POSTPONE));   /* src/blocked.c:109 */
#
# isReplicatedClient() (src/server.c:3704) is true for flag.primary and
# BLOCKED_INUSE is neither exempt btype.  The same assert is reachable for a
# slot-migration import client.
#
# Even with the assert removed, blockClientInUseOnKeys() clears the read handler
# and leaves pending_command set, so the replica's reploff stops advancing for
# the duration of the block.
#-------------------------------------------------------------------------------
start_server {tags {"forkless repl external:skip"} overrides {save {}}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {forkless-infrastructure-enabled yes save {}}} {
        set replica [srv 0 client]

        test "primary write to a key held by the replica's forkless save does not panic the replica" {
            populate_uncloneable_keys $primary 20

            $replica replicaof $primary_host $primary_port
            wait_for_sync $replica
            assert_equal 20 [$replica dbsize]

            start_slow_forkless_save $replica 10000000

            # Overwrite the same keys.  They are >512 bytes, so the iterator
            # holds them by reference and the applying client must block.
            set big [string repeat y 2000]
            for {set i 0} {$i < 20} {incr i} {
                $primary set key$i $big
            }
            after 500

            assert_alive $replica "primary write to a held key"

            # Replication must still be making progress.
            $primary set canary 1
            if {[catch {wait_for_ofs_sync $primary $replica} err]} {
                forkless_cleanup $replica
                fail "replication offset stopped advancing during forkless save: $err"
            }
            assert_equal 1 [$replica get canary]

            forkless_cleanup $replica
            catch {$replica replicaof no one}
            set _ {}
        } {} {needs:debug}
    }
}

#-------------------------------------------------------------------------------
# 5. repl-diskless-load swapdb frees the kvstore the forkless iterator is
#    scanning (use-after-free)
#
# replicaBeforeLoadPrimaryRDB() (src/replication.c:2385) kills only
# CHILD_TYPE_RDB; it never calls forklessSaveCancel().  Every other RDB-load path
# is covered (RM_RdbLoad, FLUSHALL, shutdown, and the non-swapdb sync paths via
# RDBFLAGS_EMPTY_DATA -> emptyData(-1,...) -> bgIteration_flushall()).  The
# swapdb path deliberately skips RDBFLAGS_EMPTY_DATA (src/replication.c:2528), so
# fullScanIteratorFlushDb() - the only place it->kvs is cleared - never runs.
# swapMainDbWithTempDb() plus disklessLoadDiscardTempDb() then hand that kvstore
# to lazyfree while it->kvs, captured at src/bgiteration.c:551, still points at
# it.
#
# Observed: SIGSEGV in fullScanIteratorGetEntries (accessing 0x0) 29ms after
# "Discarding old DB in background".
#-------------------------------------------------------------------------------
start_server {tags {"forkless repl external:skip"} overrides {save {} repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {overrides {
        forkless-infrastructure-enabled yes
        save {}
        repl-diskless-load swapdb
    }} {
        set replica [srv 0 client]

        test "full sync with repl-diskless-load swapdb during a forkless save does not use freed memory" {
            $primary set onlykey v1

            # Enough local keys that the iterator is still mid-scan (it->kvs
            # non-NULL) when the swap happens.
            $replica debug populate 300000
            assert_equal 300000 [$replica dbsize]

            # A small per-key delay, not a large one: the crash is inside
            # fullScanIteratorGetEntries, which the feed timer only reaches while
            # the background thread keeps draining the queue.  Parking the
            # serializer on one key fills the queue and hides the defect.
            start_slow_forkless_save $replica 200

            $replica replicaof $primary_host $primary_port
            wait_for_log_messages 0 {"*Discarding old DB in background*"} 0 100 100

            # The bgIteration timer proc fires every 2ms and dereferences
            # it->kvs; the crash lands within ~30ms of the discard.
            after 1000
            assert_alive $replica "swapdb full sync during forkless save"

            wait_for_sync $replica
            assert_equal 1 [$replica dbsize]
            assert_equal {v1} [$replica get onlykey]

            forkless_cleanup $replica
            catch {$replica replicaof no one}
            set _ {}
        } {} {needs:debug}
    }
}
