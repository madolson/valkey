# chunky-instances N: one valkey-server forks N cluster nodes on port..port+N-1.

proc chunky_wait_ping {port} {
    wait_for_condition 100 50 {
        ![catch {set r [valkey 127.0.0.1 $port]; $r ping; $r close}]
    } else {
        fail "chunky instance on port $port did not come up"
    }
}

proc chunky_is_down {port} {
    return [catch {set r [valkey 127.0.0.1 $port]; $r ping; $r close}]
}

# start_server waits for the spawned pid's banner, which the supervising
# parent never prints, so this test spawns the server itself.
tags {external:skip cluster} {
    set dir [file normalize [tmpdir chunky]]
    set port0 [find_available_port $::baseport $::portcount]
    set port1 [expr {$port0 + 1}]
    set conf [file join $dir valkey.conf]
    set fp [open $conf w]
    puts $fp "port $port0\nbind 127.0.0.1\ncluster-enabled yes\nchunky-instances 2\ncluster-node-timeout 3000\ndir $dir\nsave \"\"\nenable-debug-command yes"
    close $fp
    set stdout [file join $dir stdout]
    set parent [exec src/valkey-server $conf >> $stdout 2>> $stdout &]
    chunky_wait_ping $port0
    chunky_wait_ping $port1
    set r0 [valkey 127.0.0.1 $port0]
    set r1 [valkey 127.0.0.1 $port1]

    test "Each chunky instance reports its index" {
        assert_equal 2 [getInfoProperty [$r0 info server] chunky_instances]
        assert_equal 0 [getInfoProperty [$r0 info server] chunky_instance_index]
        assert_equal 1 [getInfoProperty [$r1 info server] chunky_instance_index]
        assert {[getInfoProperty [$r0 info server] process_id] != [getInfoProperty [$r1 info server] process_id]}
        assert {[getInfoProperty [$r0 info server] process_id] != $parent}
        assert {[$r0 cluster myid] ne [$r1 cluster myid]}
        assert {[getInfoProperty [$r0 info server] run_id] ne [getInfoProperty [$r1 info server] run_id]}
    }

    test "Chunky instances form a cluster and serve their own slots" {
        $r0 cluster addslotsrange 0 8191
        $r1 cluster addslotsrange 8192 16383
        $r1 cluster meet 127.0.0.1 $port0
        wait_for_condition 100 100 {
            [string match {*cluster_state:ok*} [$r0 cluster info]] &&
            [string match {*cluster_state:ok*} [$r1 cluster info]]
        } else {
            fail "cluster did not become ok"
        }
        # foo is slot 12182, bar is slot 5061.
        assert_equal OK [$r1 set foo 1]
        assert_equal OK [$r0 set bar 2]
        assert_error "*MOVED 12182*:$port1*" {$r0 get foo}
        assert_error "*MOVED 5061*:$port0*" {$r1 get bar}
        assert_equal 1 [$r1 get foo]
        assert_equal 2 [$r0 get bar]
    }

    test "A chunky instance can't replicate its own group" {
        assert_error "*own chunky group*" {$r1 cluster replicate [$r0 cluster myid]}
    }

    test "DEBUG RESTART of one instance keeps its port and index" {
        set pid1 [getInfoProperty [$r1 info server] process_id]
        catch {$r1 debug restart}
        $r1 close
        chunky_wait_ping $port1
        set r1 [valkey 127.0.0.1 $port1]
        assert_equal $pid1 [getInfoProperty [$r1 info server] process_id]
        assert_equal 1 [getInfoProperty [$r1 info server] chunky_instance_index]
        wait_for_condition 100 100 {
            [string match {*cluster_state:ok*} [$r1 cluster info]]
        } else {
            fail "cluster did not recover after DEBUG RESTART"
        }
        assert_equal $port0 [lindex [split [exec grep ^port $conf]] 1]
    }

    test "CONFIG REWRITE is refused on a chunky instance" {
        assert_error "*chunky*" {$r0 config rewrite}
    }

    test "SIGTERM to the parent shuts down every instance" {
        $r0 close
        $r1 close
        exec kill -TERM $parent
        wait_for_condition 100 100 {
            ![is_alive $parent] && [chunky_is_down $port0] && [chunky_is_down $port1]
        } else {
            fail "chunky group did not shut down"
        }
        set log [exec cat $stdout]
        assert_match "*chunky: all instances exited*" $log
    }
}
