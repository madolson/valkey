set testmodule [file normalize tests/modules/zset.so]

start_server {tags {"modules"}} {
    r module load $testmodule

    test {Module zset rem} {
        r del k
        r zadd k 100 hello 200 world
        assert_equal 1 [r zset.rem k hello]
        assert_equal 0 [r zset.rem k hello]
        assert_equal 1 [r exists k]
        # Check that removing the last element deletes the key
        assert_equal 1 [r zset.rem k world]
        assert_equal 0 [r exists k]
    }

    test {Module zset add} {
        r del k
        # Check that failure does not create empty key
        assert_error "ERR ZsetAdd failed" {r zset.add k nan hello}
        assert_equal 0 [r exists k]

        r zset.add k 100 hello
        assert_equal {hello 100} [r zrange k 0 -1 withscores]
    }

    test {Module zset incrby} {
        r del k
        # Check that failure does not create empty key
        assert_error "ERR ZsetIncrby failed" {r zset.incrby k hello nan}
        assert_equal 0 [r exists k]

        r zset.incrby k hello 100
        assert_equal {hello 100} [r zrange k 0 -1 withscores]
    }

    test {Module zset rangebylex} {
        # Should give wrong arity error
        assert_error "ERR wrong number of arguments*" {r zset.rangebylex}
        assert_error "ERR wrong number of arguments*" {r zset.revrangebylex}

        # Should give wrong type error
        r del k
        r set k v
        assert_error "WRONGTYPE Operation against a key*" {r zset.rangebylex k - +}

        # Should give invalid range error
        r del k
        r zadd k 0 ele
        assert_error "invalid range" {r zset.rangebylex k - a}
        assert_error "invalid range" {r zset.revrangebylex k - a}

        # Check if the data structure of the sorted set is skiplist
        r del k
        r config set zset-max-listpack-entries 2
        r config set zset-max-listpack-value 64
        for {set i 0} {$i < 4} {incr i} {
            r zadd k 0 "ele$i"
        }
        assert_equal {ele0 ele1 ele2 ele3} [r zset.rangebylex k - +]
        assert_equal {ele3 ele2 ele1 ele0} [r zset.revrangebylex k - +]
        assert_equal {ele1 ele2} [r zset.rangebylex k "(ele0" "(ele3"]
        assert_equal {ele2 ele1} [r zset.revrangebylex k "(ele0" "(ele3"]

        # Check if the data structure of the sorted set is listpack
        r del k
        r config set zset-max-listpack-entries 128
        r config set zset-max-listpack-value 64
        for {set i 0} {$i < 4} {incr i} {
            r zadd k 0 "ele$i"
        }
        assert_equal {ele0 ele1 ele2 ele3} [r zset.rangebylex k - +]
        assert_equal {ele3 ele2 ele1 ele0} [r zset.revrangebylex k - +]
        assert_equal {ele1 ele2} [r zset.rangebylex k "(ele0" "(ele3"]
        assert_equal {ele2 ele1} [r zset.revrangebylex k "(ele0" "(ele3"]
    }

    foreach {encoding entries} {listpack 128 btree 0} {
        test "Module zset range iterator changes direction - $encoding" {
            r config set zset-max-listpack-entries $entries
            r del k kl
            r zadd k 1 a 2 b 3 c 4 d 5 e
            r zadd kl 0 a 0 b 0 c 0 d 0 e
            assert_encoding $encoding k
            assert_encoding $encoding kl

            foreach {key type min max} {k score 2 4 kl lex {[b} {[d}} {
                assert_equal {b {}} [r zset.walk $key $type $min $max first prev]
                assert_equal {b c b} [r zset.walk $key $type $min $max first next prev]
                assert_equal {d {}} [r zset.walk $key $type $min $max last next]
                assert_equal {d c d} [r zset.walk $key $type $min $max last prev next]
                # A move that fails the range check leaves the iterator on the
                # current element, so turning around yields its neighbour.
                assert_equal {b c d {} c} [r zset.walk $key $type $min $max first next next next prev]
                assert_equal {d c b {} c} [r zset.walk $key $type $min $max last prev prev prev next]
            }
            foreach {key type min max} {k score 1 5 kl lex - +} {
                # Moves that run off either end of the set.
                assert_equal {a {} b} [r zset.walk $key $type $min $max first prev next]
                assert_equal {e {} d} [r zset.walk $key $type $min $max last next prev]
            }

            r del k
            r zadd k 1 a
            assert_encoding $encoding k
            assert_equal {a {} {}} [r zset.walk k score 1 1 first prev next]
            assert_equal {a {} {}} [r zset.walk k score 1 1 last next prev]
            r config set zset-max-listpack-entries 128
        }
    }

    test {Module zset members} {
        # Should give wrong arity error
        assert_error "ERR wrong number of arguments*" {r zset.members}

        # Should give wrong type error
        r del k
        r set k v
        assert_error "WRONGTYPE Operation against a key*" {r zset.members k}

        # Check if the data structure of the sorted set is skiplist
        r del k
        r config set zset-max-listpack-entries 2
        r config set zset-max-listpack-value 64
        for {set i 0} {$i < 4} {incr i} {
            r zadd k 0 "ele$i"
        }
        assert_equal {ele0 ele1 ele2 ele3} [lsort [r zset.members k]]

        # Check if the data structure of the sorted set is listpack
        r del k
        r config set zset-max-listpack-entries 128
        r config set zset-max-listpack-value 64
        for {set i 0} {$i < 4} {incr i} {
            r zadd k 0 "ele$i"
        }
        assert_equal {ele0 ele1 ele2 ele3} [lsort [r zset.members k]]
    }

    test "Unload the module - zset" {
        assert_equal {OK} [r module unload zset]
    }
}
