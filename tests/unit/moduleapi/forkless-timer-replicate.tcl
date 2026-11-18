set testmodule [file normalize tests/modules/forklesstimer.so]

# A module timer writes keys through the key API and replicates each write with
# RM_Replicate while a forkless full sync is running. The last client write
# before the timer is either to an existing key the scan has not reached (the
# server caches "do not replicate") or to a new key (it caches "replicate").
foreach last_write {unreached-key new-key} {
start_server {tags {"modules repl external:skip"} overrides {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 repl-timeout 60}} {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    # DB 0 is scanned first. DB 9 is not reached until DB 0 is done.
    $primary select 0
    $primary debug populate 1000 key 1000
    $primary select 9
    $primary set other old
    $primary select 0

    start_server {overrides {save {}}} {
        set replica [srv 0 client]

        test "Module timer RM_Replicate during a forkless full sync keeps the replica in sync (last client write: $last_write)" {
            # Keep the forkless iteration active (about 5s over the 1000 keys of DB 0).
            test_config_set $primary rdb-key-save-delay 5000
            set loglines [count_log_lines -1]
            $replica replicaof $primary_host $primary_port
            wait_for_condition 500 10 {
                [s -1 rdb_bgsave_in_progress] == 1
            } else {
                fail "full sync did not start"
            }
            assert_equal {forkless} [s -1 rdb_current_bgsave_type]
            # Let the scan pass some keys of DB 0.
            wait_for_condition 500 10 {
                [s -1 current_save_keys_processed] >= 100
            } else {
                fail "the sync did not progress"
            }

            if {$last_write eq "unreached-key"} {
                $primary select 9
                $primary set other new
                $primary select 0
            } else {
                $primary set brandnew x
            }

            # The timer runs outside any client command, so no command decision
            # covers these writes, whatever the last client write was.
            assert_equal {OK} [$primary forklesstimer.setall key: 1000 new]
            wait_for_condition 500 10 {
                [lindex [$primary forklesstimer.stats] 0] == 1
            } else {
                fail "the timer did not run"
            }
            lassign [$primary forklesstimer.stats] done set skipped
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            $primary config set rdb-key-save-delay 0
            wait_for_sync $replica
            wait_for_ofs_sync $primary $replica

            $replica select 0
            set diverged {}
            for {set i 0} {$i < 1000} {incr i} {
                if {[$primary get key:$i] ne [$replica get key:$i]} { lappend diverged key:$i }
            }
            assert_equal 0 [llength $diverged] \
                "timer set=$set skipped=$skipped; replica differs on [llength $diverged] keys, first: [lrange $diverged 0 4]"
            # Single-key writes are decided by key state, so the sync is not restarted.
            verify_no_log_message -1 {*Terminating*} $loglines
        }
    }
}
}

# A module timer replicates one write that touches two keys during a forkless
# full sync. The iterator can't decide it by key state, so it ends the sync and
# the replica resyncs.
start_server {tags {"modules repl external:skip"} overrides {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 repl-timeout 60}} {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    # DB 0 holds the two keys and is scanned first. DB 9 keeps the sync busy.
    $primary select 0
    $primary set k1 old
    $primary set k2 old
    $primary select 9
    $primary debug populate 1000 key 1000
    $primary select 0

    start_server {overrides {save {}}} {
        set replica [srv 0 client]

        test "Module timer RM_Replicate of a multi-key write ends a forkless full sync and the replica resyncs" {
            # Keep the forkless iteration active (about 5s over the 1000 keys of DB 9).
            test_config_set $primary rdb-key-save-delay 5000
            set loglines [count_log_lines -1]
            $replica replicaof $primary_host $primary_port
            wait_for_condition 500 10 {
                [s -1 rdb_bgsave_in_progress] == 1
            } else {
                fail "full sync did not start"
            }
            assert_equal {forkless} [s -1 rdb_current_bgsave_type]
            # Wait until the scan is in DB 9, so it has passed both keys of DB 0.
            wait_for_condition 500 10 {
                [s -1 current_save_keys_processed] >= 100
            } else {
                fail "the sync did not progress"
            }

            assert_equal {OK} [$primary forklesstimer.mset k1 k2 new]
            wait_for_condition 500 10 {
                [lindex [$primary forklesstimer.stats] 0] == 1
            } else {
                fail "the timer did not run"
            }
            assert_equal {1 2 0} [$primary forklesstimer.stats]

            $primary config set rdb-key-save-delay 0
            wait_for_sync $replica
            wait_for_ofs_sync $primary $replica

            $replica select 0
            assert_equal {new new} [list [$replica get k1] [$replica get k2]]
            assert_equal [$primary debug digest] [$replica debug digest]
            verify_log_message -1 {*Terminating*mset was propagated outside of a write command*} $loglines
            verify_log_message -1 {*forkless save cancelled*} $loglines
        }
    }
}
