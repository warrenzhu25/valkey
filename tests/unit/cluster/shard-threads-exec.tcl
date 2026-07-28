# Safe single-slot reads run on the owning execution shard's executor client instead of
# the coordinator's real client (notes/proposal-slot-per-thread.md §19). At this step the
# executor still runs on the main thread, so the only observable requirement is that the
# execute-on-executor -> detach reply -> reattach path is byte-identical to running the
# command directly. c->slot is populated only in cluster mode, which is why this needs the
# cluster harness. shard-threads is dormant otherwise, so a single master is enough.

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
}
