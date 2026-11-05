# INFO chunky counters. Keys and their slot, instance of 8, 4 and 2:
#   aa 1180  0 0 0
#   a0 3656  1 0 0
#   ad 5177  2 1 0
#   ac 9438  4 2 1

# Reads INFO chunky once, so the INFO calls do not show up in the counters.
proc chunky_snap {} {
    set ::chunky_info [r info chunky]
}

proc chunky {field} {
    getInfoProperty $::chunky_info chunky_$field
}

# Sends all commands in one write so the server reads them in one pass.
proc chunky_pipeline {rd cmds} {
    set buf ""
    foreach cmd $cmds {
        append buf [format_command {*}$cmd]
    }
    $rd write $buf
    $rd flush
    foreach cmd $cmds {
        $rd read
    }
}

start_server {tags {"info chunky external:skip"}} {
    test {chunky-stats is off by default and counts nothing} {
        assert_equal {chunky-stats no} [r config get chunky-stats]
        r config resetstat
        r set aa 1
        r set ac 1
        chunky_snap
        assert_equal 0 [chunky stats_enabled]
        assert_equal 0 [chunky cmds]
        assert_equal 0 [chunky batches]
        assert_equal {n2=0,n4=0,n8=0} [chunky switches]
    }

    r config set chunky-stats yes

    test {chunky counts instance switches, keyless commands and mixed batches} {
        set rd [valkey_deferring_client]
        r config resetstat
        chunky_pipeline $rd {{get aa} {get a0} {get ad} {get ac} {ping}}
        chunky_snap
        assert_equal {n2=1,n4=2,n8=3} [chunky switches]
        assert_equal 4 [chunky cmds_keyed]
        # PING and the INFO that reads the counters.
        assert_equal 2 [chunky cmds_keyless]
        assert_equal {n2=1,n4=1,n8=1} [chunky batches_mixed]
        assert_equal {n2=5,n4=5,n8=5} [chunky batch_cmds_mixed]

        # Batches on one instance are not mixed, and keyless commands do not
        # reset the connection's instance.
        r config resetstat
        chunky_pipeline $rd {{get aa} {ping} {get a0} {get aa}}
        # The connection was last on ac, so aa switches for every N.
        chunky_snap
        assert_equal {n2=1,n4=1,n8=3} [chunky switches]
        assert_equal {n2=0,n4=0,n8=1} [chunky batches_mixed]
        assert_equal 1 [chunky batches_multi_cmd]

        # Switches are tracked per connection across batches.
        r config resetstat
        chunky_pipeline $rd {{get ac}}
        chunky_snap
        assert_equal {n2=1,n4=1,n8=1} [chunky switches]
        assert_equal {n2=0,n4=0,n8=0} [chunky batches_mixed]
        $rd close
    }

    test {chunky counts multi-slot commands by name} {
        r config resetstat
        r mset aa 1 a0 1
        r del aa ac
        r mset "{t}1" 1 "{t}2" 2
        chunky_snap
        assert_equal 2 [chunky cmds_multislot]
        assert_equal {n2=1,n4=1,n8=2} [chunky cmds_multi_instance]
        assert_equal {calls=1} [chunky multislot_cmd_mset]
        assert_equal {calls=1} [chunky multislot_cmd_del]
    }

    test {chunky counts transactions spanning instances} {
        r config resetstat
        r multi
        r set aa 1
        r set ad 1
        r exec
        r multi
        r set aa 1
        r exec
        chunky_snap
        assert_equal 2 [chunky exec]
        assert_equal {n2=0,n4=1,n8=1} [chunky exec_mixed]
    }

    test {chunky counts scripts with undeclared keys and compat mode evals} {
        r config resetstat
        r eval {return redis.call('get', KEYS[1])} 1 aa
        r eval {redis.call('set', 'ac', 1); return redis.call('get', KEYS[1])} 1 aa
        r eval {return redis.call('get', 'a0')} 0
        r eval {#!lua
            return redis.call('get', KEYS[1])} 1 aa
        chunky_snap
        assert_equal 4 [chunky script_runs]
        assert_equal 3 [chunky script_eval_compat]
        assert_equal 2 [chunky script_undeclared_runs]
        assert_equal 2 [chunky script_undeclared_keys]
        assert_equal 1 [chunky script_nodecl_runs]
        assert_equal 1 [chunky script_multislot_runs]
        assert_equal {n2=1,n4=1,n8=1} [chunky script_mixed_runs]

        r function load replace {#!lua name=chunkylib
            server.register_function{function_name='chunkyfn', callback=function(keys, args)
                return server.call('get', 'ad') end, flags={'allow-cross-slot-keys'}}}
        r config resetstat
        r fcall chunkyfn 1 aa
        chunky_snap
        assert_equal 1 [chunky script_runs]
        assert_equal 0 [chunky script_eval_compat]
        assert_equal 1 [chunky script_flag_cross_slot]
        assert_equal 1 [chunky script_undeclared_runs]
        assert_equal {n2=0,n4=1,n8=1} [chunky script_mixed_runs]
    }

    test {chunky counters stop when disabled} {
        r config resetstat
        r config set chunky-stats no
        r mset aa 1 ac 1
        r eval {return redis.call('get', 'a0')} 0
        chunky_snap
        assert_equal 0 [chunky cmds_multislot]
        assert_equal 0 [chunky script_runs]
        r config set chunky-stats yes
    }
}

start_cluster 1 0 {tags {"info chunky external:skip cluster"}} {
    test {chunky counts in cluster mode, including cross-slot scripts} {
        R 0 config set chunky-stats yes
        R 0 config resetstat
        R 0 get aa
        R 0 get ac
        assert_error {*CROSSSLOT*} {R 0 mset aa 1 ac 1}
        R 0 eval {redis.call('set', 'ad', 1); return redis.call('get', KEYS[1])} 1 aa
        set info [R 0 info chunky]
        assert_equal {n2=2,n4=2,n8=2} [getInfoProperty $info chunky_switches]
        assert_equal 1 [getInfoProperty $info chunky_cmds_multislot]
        assert_equal {calls=1} [getInfoProperty $info chunky_multislot_cmd_mset]
        assert_equal 1 [getInfoProperty $info chunky_script_undeclared_runs]
        assert_equal 1 [getInfoProperty $info chunky_script_multislot_runs]
    }
}
