set testmodule [file normalize tests/modules/auth.so]

start_server {tags {"modules"}} {
    r module load $testmodule

    test {Modules can create a user that can be authenticated} {
        # Make sure we start authenticated with default user
        r auth default ""
        assert_equal [r acl whoami] "default"
        r auth.createmoduleuser

        set id [r auth.authmoduleuser]
        assert_equal [r client id] $id

        # Verify returned id is the same as our current id and
        # we are authenticated with the specified user
        assert_equal [r acl whoami] "global"
    }

    test {De-authenticating clients is tracked and kills clients} {
        assert_equal [r auth.changecount] 0
        r auth.createmoduleuser

        # Catch the I/O exception that was thrown when the server
        # disconnected with us. 
        catch { [r ping] } e
        assert_match {*I/O*} $e

        # Check that a user change was registered
        assert_equal [r auth.changecount] 1
    }

    test {Modules can't authenticate with ACLs users that dont exist} {
        catch { [r auth.authrealuser auth-module-test-fake] } e
        assert_match {*Invalid user*} $e
    }

    test {Modules can authenticate with ACL users} {
        assert_equal [r acl whoami] "default"

        # Create user to auth into
        r acl setuser auth-module-test on allkeys allcommands

        set id [r auth.authrealuser auth-module-test]

        # Verify returned id is the same as our current id and
        # we are authenticated with the specified user
        assert_equal [r client id] $id
        assert_equal [r acl whoami] "auth-module-test"
    }

    test {Client callback is called on user switch} {
        assert_equal [r auth.changecount] 0

        # Auth again and validate change count
        r auth.authrealuser auth-module-test
        assert_equal [r auth.changecount] 1

        # Re-auth with the default user
        r auth default ""
        assert_equal [r auth.changecount] 1
        assert_equal [r acl whoami] "default"

        # Re-auth with the default user again, to
        # verify the callback isn't fired again
        r auth default ""
        assert_equal [r auth.changecount] 0
        assert_equal [r acl whoami] "default"
    }

    test {Subscribers are killed when a module authenticates them as a user without channel permission} {
        r acl setuser nochannels on nopass resetchannels +@all
        set rd [valkey_deferring_client]
        $rd HELLO 3 SETNAME deathrow
        $rd read
        $rd SUBSCRIBE foo
        $rd read
        $rd auth.authrealuser nochannels
        $rd read
        wait_for_condition 50 100 {
            ![string match {*deathrow*} [r client list]]
        } else {
            fail "Subscriber was not killed after switching to a user without channel permission"
        }
        $rd close
        r acl deluser nochannels
    } {1}

    test {modules can redact arguments} {
        r config set slowlog-log-slower-than 0
        r slowlog reset
        r auth.redact 1 2 3 4
        r auth.redact 1 2 3
        r config set slowlog-log-slower-than -1
        set slowlog_resp [r slowlog get]

        # There will be 3 records, slowlog reset and the
        # two auth redact calls.
        assert_equal 3 [llength $slowlog_resp]
        assert_equal {slowlog reset} [lindex [lindex $slowlog_resp 2] 3]
        assert_equal {auth.redact 1 (redacted) 3 (redacted)} [lindex [lindex $slowlog_resp 1] 3]
        assert_equal {auth.redact (redacted) 2 (redacted)} [lindex [lindex $slowlog_resp 0] 3]
    }

    test "Unload the module - testacl" {
        assert_equal {OK} [r module unload testacl]
    }
}
