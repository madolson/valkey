set testmodule [file normalize tests/modules/pausetest.so]

# Each test gets its own server pair since the crash kills the server.

# Map each API to its command suffix (differs from the VM_ name for verbatim)
# and expected result: VM_Call rejects with a NULL reply, VM_Replicate* error.
foreach {apiname cmdsuffix expected} {
    Call CALL NULL
    Replicate REPLICATE ERROR
    ReplicateVerbatim VERBATIM ERROR
} {
    start_server {tags {"modules needs:repl"}} {
        r module load $testmodule
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        start_server {} {
            set replica [srv 0 client]
            $replica replicaof $primary_host $primary_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "Replica did not connect"
            }

            test "VM_$apiname write command returns error during CLIENT PAUSE WRITE with replica" {
                $primary PAUSETEST.TIMER_$cmdsuffix 100
                $primary CLIENT PAUSE 60000 WRITE
                # Without fix: server crashes (assertion in propagateNow)
                # With fix: the write is rejected (result != OK)
                wait_for_condition 50 20 {
                    [$primary PAUSETEST.GET_RESULT] ne "NONE"
                } else {
                    fail "Timer result not ready"
                }
                set result [$primary PAUSETEST.GET_RESULT]
                $primary CLIENT UNPAUSE
                assert_equal $expected $result
            }
        }
    }
}

start_server {tags {"modules needs:repl"}} {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {} {
        set replica [srv 0 client]
        $replica replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [string match {*master_link_status:up*} [$replica info replication]]
        } else {
            fail "Replica did not connect"
        }

        test {Module timer VM_Call succeeds when not paused} {
            $primary PAUSETEST.TIMER_CALL 100
            wait_for_condition 50 20 {
                [$primary PAUSETEST.GET_RESULT] ne "NONE"
            } else {
                fail "Timer result not ready"
            }
            set result [$primary PAUSETEST.GET_RESULT]
            assert_equal "OK" $result
        }
    }
}

# A CLIENT PAUSE issued inside MULTI only takes effect after EXEC, so writes
# queued after it must still be applied and propagated.
start_server {tags {"modules needs:repl"}} {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]

    start_server {} {
        set replica [srv 0 client]
        # ReplicateVerbatim propagates the module command itself.
        $replica module load $testmodule
        $replica replicaof $primary_host $primary_port
        wait_for_condition 50 100 {
            [string match {*master_link_status:up*} [$replica info replication]]
        } else {
            fail "Replica did not connect"
        }

        foreach {desc cmd} {
            VM_Replicate {PAUSETEST.SET_REPLICATE}
            VM_ReplicateVerbatim {PAUSETEST.SET_VERBATIM}
            VM_Call {EVAL {return server.call('set', KEYS[1], ARGV[1])} 1}
        } {
            test "$desc write after CLIENT PAUSE WRITE in the same MULTI reaches the replica" {
                # Per-test key and marker, so state left by an earlier
                # iteration cannot satisfy the checks.
                set k "key:$desc"
                $primary multi
                $primary client pause 60000 write
                $primary {*}$cmd $k v
                $primary set marker $desc
                catch {$primary exec} res
                $primary client unpause
                wait_for_condition 50 100 {
                    [$replica get marker] eq $desc
                } else {
                    fail "Replica did not receive the transaction"
                }
                assert_equal v [$primary get $k]
                assert_equal v [$replica get $k]
                assert_equal {OK OK OK} $res
            }
        }
    }
}
