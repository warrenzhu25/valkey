# Safe single-slot reads run on the owning execution shard's executor client instead of
# the coordinator's real client (notes/proposal-slot-per-thread.md §19). At this step the
# executor still runs on the main thread, so the only observable requirement is that the
# execute-on-executor -> detach reply -> reattach path is byte-identical to running the
# command directly. c->slot is populated only in cluster mode, which is why this needs the
# cluster harness. shard-threads is dormant otherwise, so a single master is enough.

proc shard_threads_remote_tag {rd prefix {avoid_owner -1}} {
    $rd deferred 0
    set client_shard [$rd debug current-shard]
    $rd deferred 1
    for {set i 0} {$i < 10000} {incr i} {
        set tag "$prefix:$i"
        set slot [::valkey_cluster::hash "{$tag}:k"]
        set owner [R 0 debug slot-shard $slot]
        if {$owner != $client_shard && $owner != $avoid_owner} {
            return [list $tag $owner]
        }
    }
    fail "No remote hash tag found for client shard $client_shard"
}

proc shard_threads_resp {args} {
    set cmd "*[llength $args]\r\n"
    foreach arg $args {
        append cmd "$[string length $arg]\r\n$arg\r\n"
    }
    return $cmd
}

proc shard_threads_pipeline {rd commands} {
    set payload {}
    foreach cmd $commands {
        append payload [shard_threads_resp {*}$cmd]
    }
    $rd write $payload
    $rd flush
    set replies {}
    foreach cmd $commands {
        catch {$rd read} reply
        lappend replies $reply
    }
    return $replies
}

start_cluster 1 0 {tags {external:skip cluster} overrides {shard-threads 4}} {
    test {shard-threads worker threads are present} {
        assert_equal 4 [getInfoProperty [R 0 info server] shard_threads]
        assert_equal 3 [getInfoProperty [R 0 info server] shard_threads_active]
    }

    test {reads route through the executor and are byte-identical} {
        assert_equal OK   [R 0 set foo bar]
        assert_equal bar  [R 0 get foo]          ;# read via executor
        assert_equal 4    [R 0 append foo Z]
        assert_equal barZ [R 0 get foo]          ;# read via executor, after a write
        assert_equal 4    [R 0 strlen foo]       ;# read via executor
        assert_equal 3    [R 0 rpush L a b c]
        assert_equal {a b c} [R 0 lrange L 0 -1] ;# multi-element read via executor
        assert_equal list [R 0 type L]
        assert_equal 2    [R 0 hset H a 1 b 2]
        assert_equal {a 1 b 2} [R 0 hgetall H]
        assert_equal {}   [R 0 get nokey]        ;# nil via executor
        assert_equal 1    [R 0 exists foo]
    }

    test {multi-key same-slot read routes through the executor} {
        R 0 mset "{tag}a" 1 "{tag}b" 2 "{tag}c" 3
        assert_equal {1 2 3} [R 0 mget "{tag}a" "{tag}b" "{tag}c"]
    }

    test {RESP3 map reply is byte-identical through the executor} {
        R 0 hello 3
        assert_equal {a 1 b 2} [R 0 hgetall H]
        R 0 hello 2
    }

    test {an executor read treats an expired key as missing without deleting it} {
        # The executor runs with keep-expired semantics (replica-like): an expired key
        # reads as missing but is not deleted/propagated on the read path -- that is left
        # to the active-expire cycle, so a worker read never writes replication state.
        R 0 set tk v
        R 0 pexpire tk 20
        after 60
        assert_equal {} [R 0 get tk]             ;# reads as missing
        assert_equal 0  [R 0 exists tk]          ;# reads as missing
    }

    test {same-owner remote pipeline is squashed and replies remain ordered} {
        R 0 config resetstat
        set rd [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
        lassign [shard_threads_remote_tag $rd batch-basic] tag owner
        set commands {}
        for {set i 0} {$i < 10} {incr i} {
            set key "{$tag}:$i"
            R 0 set $key "v$i"
            lappend commands [list get $key]
        }
        assert_equal {v0 v1 v2 v3 v4 v5 v6 v7 v8 v9} [shard_threads_pipeline $rd $commands]
        assert {[getInfoProperty [R 0 info stats] shard_remote_batches] >= 1}
        assert {[getInfoProperty [R 0 info stats] shard_remote_batched_commands] >= 10}
        $rd close
    }

    test {same-owner remote pipeline can mix reads and writes} {
        R 0 config resetstat
        set rd [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
        lassign [shard_threads_remote_tag $rd batch-mixed] tag owner
        set key "{$tag}:counter"
        R 0 del $key
        set replies [shard_threads_pipeline $rd [list \
            [list set $key 1] \
            [list incr $key] \
            [list get $key] \
            [list append $key x] \
            [list get $key]]]
        assert_equal {OK 2 2 2 2x} $replies
        assert_equal 2x [R 0 get $key]
        assert_equal 1 [getInfoProperty [R 0 info stats] shard_remote_batches]
        assert_equal 5 [getInfoProperty [R 0 info stats] shard_remote_batched_commands]
        $rd close
    }

    test {same-owner remote pipeline handles consecutive writes} {
        R 0 config resetstat
        set rd [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
        lassign [shard_threads_remote_tag $rd batch-writes] tag owner
        set commands {}
        set expected {}
        for {set i 0} {$i < 64} {incr i} {
            lappend commands [list set "{$tag}:$i" "v$i"]
            lappend expected OK
        }
        assert_equal $expected [shard_threads_pipeline $rd $commands]
        for {set i 0} {$i < 64} {incr i} {
            assert_equal "v$i" [R 0 get "{$tag}:$i"]
        }
        assert {[getInfoProperty [R 0 info stats] shard_remote_batches] >= 2}
        assert {[getInfoProperty [R 0 info stats] shard_remote_batched_commands] >= 64}
        $rd close
    }

    test {remote pipeline fans out across owners and preserves reply order} {
        R 0 config resetstat
        set rd [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
        lassign [shard_threads_remote_tag $rd batch-owner-a] tag1 owner1
        lassign [shard_threads_remote_tag $rd batch-owner-b $owner1] tag2 owner2
        R 0 set "{$tag1}:a" A
        R 0 set "{$tag1}:b" B
        R 0 set "{$tag1}:c" C
        R 0 set "{$tag2}:d" D
        set replies [shard_threads_pipeline $rd [list \
            [list get "{$tag1}:a"] \
            [list get "{$tag2}:d"] \
            [list get "{$tag1}:b"] \
            [list get "{$tag1}:c"]]]
        assert_equal {A D B C} $replies
        assert_equal 1 [getInfoProperty [R 0 info stats] shard_remote_batches]
        assert_equal 1 [getInfoProperty [R 0 info stats] shard_remote_fanout_batches]
        assert_equal 4 [getInfoProperty [R 0 info stats] shard_remote_batched_commands]
        $rd close
    }

    test {batch stops before rejected command and error position is preserved} {
        R 0 config resetstat
        set rd [valkey_deferring_client_by_addr [srv 0 host] [srv 0 port]]
        lassign [shard_threads_remote_tag $rd batch-reject] tag owner
        R 0 set "{$tag}:a" A
        R 0 set "{$tag}:b" B
        set replies [shard_threads_pipeline $rd [list \
            [list get "{$tag}:a"] \
            [list get "{$tag}:b"] \
            [list get]]]
        assert_equal A [lindex $replies 0]
        assert_equal B [lindex $replies 1]
        assert_match {*wrong number of arguments*} [lindex $replies 2]
        assert_equal 1 [getInfoProperty [R 0 info stats] shard_remote_batches]
        assert_equal 2 [getInfoProperty [R 0 info stats] shard_remote_batched_commands]
        $rd close
    }
}
