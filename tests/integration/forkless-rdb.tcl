tags {"rdb external:skip"} {

# Fork-less disk BGSAVE (Stage 3). DEBUG RELOAD NOSAVE loads the
# fork-less-produced RDB back.

start_server {overrides {save ""}} {
    test {forkless BGSAVE produces a load-equal RDB (quiesced)} {
        r flushall
        for {set i 0} {$i < 500} {incr i} { r set str:$i "value-number-$i" }
        r set int:key 12345
        r set ttl:key v ex 100000
        for {set i 0} {$i < 60} {incr i} { r hset myhash f$i v$i }
        r rpush mylist a b c d e f g
        r sadd myintset 1 2 3 4 5 6 7 8
        r sadd mystrset alpha beta gamma
        for {set i 0} {$i < 300} {incr i} { r zadd myzset $i member$i }

        set d1 [debug_digest]
        set ls [r lastsave]
        after 1100
        r bgsave
        wait_for_condition 200 50 {
            [r lastsave] > $ls
        } else {
            fail "fork-less bgsave did not complete"
        }
        # Load the fork-less-produced RDB from disk (no re-save).
        r debug reload nosave
        assert_equal $d1 [debug_digest]
    }

    test {forkless BGSAVE is point-in-time under concurrent overwrites} {
        r flushall
        # Enough keys that the walk spans several ticks, so overwrites interleave.
        for {set i 0} {$i < 30000} {incr i} { r set k:$i orig }
        set ls [r lastsave]
        after 1100
        r bgsave
        # Overwrite every key to "new" while the save is in flight. Each command
        # is an event-loop iteration that both drives a walk step and fires the
        # serialize-before-mutate hook.
        for {set i 0} {$i < 30000} {incr i} { r set k:$i new }
        wait_for_condition 200 50 {
            [r lastsave] > $ls
        } else {
            fail "fork-less bgsave did not complete"
        }
        # Live keyspace reflects the overwrites...
        assert_equal new [r get k:0]
        assert_equal new [r get k:29999]
        # ...but the fork-less RDB captured the at-cut ("orig") state.
        r debug reload nosave
        set orig 0
        for {set i 0} {$i < 30000} {incr i} { if {[r get k:$i] eq "orig"} {incr orig} }
        assert_equal 30000 $orig
    }

    test {forkless save survives an ADD during the save (new keys excluded)} {
        r flushall
        for {set i 0} {$i < 20000} {incr i} { r set base:$i v }
        set d_base [debug_digest]
        set ls [r lastsave]
        after 1100
        r bgsave
        # Add brand-new keys during the save; they are after the cut and must not
        # appear in the snapshot.
        for {set i 0} {$i < 5000} {incr i} { r set added:$i v }
        wait_for_condition 200 50 {
            [r lastsave] > $ls
        } else {
            fail "fork-less bgsave did not complete"
        }
        assert_equal v [r get added:0]
        r debug reload nosave
        # Only the base keys were present at the cut.
        assert_equal 20000 [r dbsize]
        assert_equal $d_base [debug_digest]
    }
}

start_server {overrides {save ""}} {
    test {forkless BGSAVE saves multiple DBs without fork} {
        r select 0
        for {set i 0} {$i < 200} {incr i} { r set a:$i v0 }
        r select 1
        for {set i 0} {$i < 200} {incr i} { r set b:$i v1 }
        r select 0
        set d0 [debug_digest]
        r select 1
        set d1 [debug_digest]
        set forks [s total_forks]
        set ls [r lastsave]
        after 1100
        r bgsave
        wait_for_condition 200 50 {
            [r lastsave] > $ls
        } else {
            fail "bgsave did not complete"
        }
        assert_equal $forks [s total_forks]
        r debug reload nosave
        r select 0
        assert_equal $d0 [debug_digest]
        r select 1
        assert_equal $d1 [debug_digest]
        r select 0
    }
}
}
