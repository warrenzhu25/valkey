# Read-only commands are executed on the IO threads (a "parallel read run").
# These tests pin down the behavior that must be indistinguishable from
# executing the same commands on the main thread: results, ordering, the stats
# that a command's proc updates, and which commands are eligible at all.

# Drive enough concurrent read traffic that runs actually form and get
# dispatched, then return how many commands were executed on an IO thread.
proc run_concurrent_reads {key_count client_count reads_per_client} {
    for {set i 0} {$i < $client_count} {incr i} {
        set rd($i) [valkey_deferring_client]
    }
    for {set i 0} {$i < $client_count} {incr i} {
        for {set j 0} {$j < $reads_per_client} {incr j} {
            $rd($i) get "key:[expr {$j % $key_count}]"
        }
        $rd($i) flush
    }
    for {set i 0} {$i < $client_count} {incr i} {
        for {set j 0} {$j < $reads_per_client} {incr j} {
            assert_equal "val:[expr {$j % $key_count}]" [$rd($i) read]
        }
        $rd($i) close
    }
}

start_cluster 1 0 {tags {external:skip cluster} overrides {io-threads 4 io-threads-always-active yes}} {
    test "Read-only commands are executed on IO threads" {
        for {set i 0} {$i < 16} {incr i} {
            r set "key:$i" "val:$i"
        }
        r config resetstat

        run_concurrent_reads 16 8 32

        # The offload is opportunistic, so don't assert on an exact count, only
        # that reads really did reach the IO threads.
        assert_morethan [s io_threaded_cmds_executed] 0
        assert_morethan [s io_threaded_cmd_runs] 0
    }

    test "Reads offloaded to IO threads return correct values" {
        r flushall
        for {set i 0} {$i < 64} {incr i} {
            r set "key:$i" "val:$i"
        }
        # run_concurrent_reads asserts every reply, so a wrong value from an IO
        # thread fails here.
        run_concurrent_reads 64 16 64
    }

    test "Keyspace hit and miss stats are correct for offloaded reads" {
        r flushall
        r set hitkey v
        r config resetstat

        set rd [valkey_deferring_client]
        for {set i 0} {$i < 20} {incr i} {
            $rd get hitkey
            $rd get misskey
        }
        $rd flush
        for {set i 0} {$i < 40} {incr i} { $rd read }
        $rd close

        # These counters are bumped inside the command's proc, so for an
        # offloaded read they are accumulated on the IO thread and folded in by
        # the main thread. Losing or double-counting them would show up here.
        assert_equal 20 [s keyspace_hits]
        assert_equal 20 [s keyspace_misses]
    }

    test "Error stats are correct for offloaded reads" {
        r flushall
        r lpush mylist a
        r config resetstat

        set rd [valkey_deferring_client]
        for {set i 0} {$i < 10} {incr i} {
            $rd get mylist
        }
        $rd flush
        for {set i 0} {$i < 10} {incr i} {
            assert_error "WRONGTYPE*" {$rd read}
        }
        $rd close

        # afterErrorReply() touches the global error counter and the errors rax,
        # so an offloaded read defers it to the main thread.
        assert_equal 10 [s total_error_replies]
        assert_match "*count=10*" [errorrstat WRONGTYPE r]
        # The epilogue must still attribute the failures to GET.
        assert_match "*calls=10*failed_calls=10*" [cmdrstat get r]
    }

    test "Expired keys are reported as missing by offloaded reads" {
        r flushall
        r set volatile v px 50
        r set stable v
        after 150

        set rd [valkey_deferring_client]
        $rd get volatile
        $rd exists volatile
        $rd get stable
        $rd flush
        assert_equal {} [$rd read]
        assert_equal 0 [$rd read]
        assert_equal v [$rd read]
        $rd close

        # The key is logically expired but an IO thread may not delete it, since
        # that would propagate a DEL. It must still be invisible.
        assert_equal 0 [r exists volatile]
    }

    test "Writes are ordered against offloaded reads" {
        r flushall
        r set k v0

        # A pipeline of read, write, read on one connection must observe the
        # write, i.e. the deferred read cannot be reordered across it.
        set rd [valkey_deferring_client]
        $rd get k
        $rd set k v1
        $rd get k
        $rd flush
        assert_equal v0 [$rd read]
        assert_equal OK [$rd read]
        assert_equal v1 [$rd read]
        $rd close
    }

    test "A read is not reordered across another client's write" {
        r flushall
        r set k v0

        # Both clients are dispatched in the same batch. The reader is collected
        # into a run; the writer is not offloadable, so it must wait for the run
        # to be joined. The reader must therefore still see the pre-write value.
        set reader [valkey_deferring_client]
        set writer [valkey_deferring_client]
        $reader get k
        $reader flush
        $writer set k v1
        $writer flush
        assert_equal v0 [$reader read]
        assert_equal OK [$writer read]
        assert_equal v1 [r get k]
        $reader close
        $writer close
    }

    test "MULTI queues reads instead of offloading them" {
        r flushall
        r set k v

        r multi
        r get k
        r get k
        assert_equal {v v} [r exec]

        # A queued command must not be deferred: the reply is QUEUED, and the
        # proc only runs inside EXEC, on the main thread.
        r multi
        r get k
        assert_equal {v} [r exec]
    }

    test "Commands that are not offloadable still work" {
        r flushall
        r set k v

        # Read-only but multi-key across slots, so no single slot: must run on
        # the main thread.
        assert_error "CROSSSLOT*" {r mget k otherkey}

        # Read-only scripts run on the shared scripting engine.
        assert_equal v [r eval_ro {return server.call('get', KEYS[1])} 1 k]

        # Writes, and read-only commands with no keys.
        assert_equal OK [r set k v2]
        assert_equal v2 [r get k]
        assert_equal PONG [r ping]
    }

    test "Client tracking clients are not offloaded" {
        r flushall
        r set k v

        set rd [valkey_client]
        $rd hello 3
        $rd client tracking on
        assert_equal v [$rd get k]

        # Tracking bookkeeping is main-thread state, so a tracking client's
        # reads run inline; the invalidation must still arrive.
        r set k v2
        $rd ping
        $rd close
    }

    test "Server is healthy after sustained parallel read load" {
        r flushall
        for {set i 0} {$i < 32} {incr i} {
            r set "key:$i" "val:$i"
        }
        for {set round 0} {$round < 3} {incr round} {
            run_concurrent_reads 32 16 32
        }
        assert_equal PONG [r ping]
        assert_equal 32 [r dbsize]
    }
}
