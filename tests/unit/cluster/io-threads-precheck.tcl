# Port of hazards.py scenario 4 (rust-io/iter4): slot ownership changes while
# a command prechecked in an IO thread waits behind BLPOP. The slot check is
# never skipped, so the queued command must see the new owner.
start_cluster 2 0 {tags {external:skip cluster valgrind:skip} overrides {io-threads 2 io-threads-always-active yes}} {
    proc send_pipeline_in_one_read {rd cmds} {
        set buf {}
        foreach cmd $cmds { append buf [format_command {*}$cmd] }
        set server_pid [srv 0 pid]
        pause_process $server_pid
        $rd write $buf
        $rd flush
        resume_process $server_pid
    }

    # Two hash tags owned by node 0.
    set tags {}
    foreach t {a b c d e f g h i j k l m n o p} {
        if {[R 0 cluster keyslot "{$t}"] < 8192} { lappend tags $t }
    }
    set t [lindex $tags 0]
    set x [lindex $tags 1]
    set slot [R 0 cluster keyslot "{$t}"]
    set id0 [R 0 cluster myid]
    set id1 [R 1 cluster myid]
    set port1 [srv -1 port]

    test {Queued command sees a slot start migrating} {
        R 0 set "{$t}present" here
        set rd [valkey_deferring_client 0]
        $rd client id
        set rd_id [$rd read]
        send_pipeline_in_one_read $rd [list [list blpop "{$t}blk" 0] [list get "{$t}present"] [list get "{$t}missing"]]
        wait_for_blocked_clients_count 1 50 100 0
        assert_equal OK [R 0 cluster setslot $slot migrating $id1]
        R 0 client unblock $rd_id
        assert_equal {} [$rd read]
        assert_equal {here} [$rd read]
        assert_error "ASK $slot 127.0.0.1:$port1" {$rd read}
        $rd close
    }

    test {Queued command sees a slot move away} {
        assert_equal OK [R 1 cluster setslot $slot importing $id0]
        assert_equal OK [R 0 migrate 127.0.0.1 $port1 "" 0 5000 keys "{$t}present"]
        set rd [valkey_deferring_client 0]
        send_pipeline_in_one_read $rd [list [list blpop "{$x}blk" 0] [list get "{$t}present"]]
        wait_for_blocked_clients_count 1 50 100 0
        assert_equal OK [R 1 cluster setslot $slot node $id1]
        assert_equal OK [R 0 cluster setslot $slot node $id1]
        R 0 lpush "{$x}blk" x
        assert_equal [list "{$x}blk" x] [$rd read]
        assert_error "MOVED $slot 127.0.0.1:$port1" {$rd read}
        $rd close
    }

    test {Owned-slot pipeline is served} {
        set rd [valkey_deferring_client 0]
        set cmds {}
        for {set j 0} {$j < 100} {incr j} { lappend cmds [list set "{$x}k" $j] }
        send_pipeline_in_one_read $rd $cmds
        for {set j 0} {$j < 100} {incr j} { assert_equal OK [$rd read] }
        assert_equal 99 [R 0 get "{$x}k"]
        $rd close
    }
}
