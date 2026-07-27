# The `shard-threads` config partitions the 16384 hash slots across that many
# execution shards. It is dormant: it shapes the ownership map and nothing else,
# so these tests check the map and that nothing about command execution changes.

# Expected owner of $slot when the slots are split across $shards shards.
proc expected_shard {slot shards} {
    expr {$slot * $shards / 16384}
}

start_server {tags {"shard-threads"}} {
    test {shard-threads defaults to 1 and is reported by INFO} {
        assert_equal {shard-threads 1} [r config get shard-threads]
        assert_equal 1 [s shard_threads]
    }

    test {DEBUG SLOT-SHARD maps every slot to shard 0 by default} {
        foreach slot {0 1 4095 4096 8192 12288 16382 16383} {
            assert_equal 0 [r debug slot-shard $slot]
        }
    }

    test {CONFIG SET shard-threads repartitions the map} {
        foreach shards {2 3 4 64 16384} {
            r config set shard-threads $shards
            assert_equal $shards [s shard_threads]
            assert_equal "shard-threads $shards" [r config get shard-threads]

            # Range boundaries are where an off-by-one in the partition shows up.
            foreach slot {0 1 4095 4096 5461 5462 8191 8192 12287 12288 16383} {
                assert_equal [expected_shard $slot $shards] [r debug slot-shard $slot]
            }
        }
        r config set shard-threads 1
    }

    test {shard-threads partition is contiguous and balanced} {
        r config set shard-threads 7
        # Walk a sample of the slot space: the owner never decreases, never skips
        # a shard, and the first and last slots anchor the range.
        assert_equal 0 [r debug slot-shard 0]
        assert_equal 6 [r debug slot-shard 16383]
        set prev 0
        for {set slot 0} {$slot < 16384} {incr slot 37} {
            set shard [r debug slot-shard $slot]
            assert {$shard >= $prev}
            assert {$shard <= $prev + 1}
            set prev $shard
        }
        r config set shard-threads 1
    }

    test {DEBUG SLOT-SHARD rejects out of range slots} {
        assert_error "*Invalid or out of range slot*" {r debug slot-shard -1}
        assert_error "*Invalid or out of range slot*" {r debug slot-shard 16384}
        assert_error "*Invalid or out of range slot*" {r debug slot-shard notanumber}
    }

    test {shard-threads rejects values outside its bounds} {
        assert_error "*argument must be between 1 and 16384*" {r config set shard-threads 0}
        assert_error "*argument must be between 1 and 16384*" {r config set shard-threads 16385}
        assert_equal 1 [s shard_threads]
    }

    test {keyspace behavior is identical with shard-threads > 1} {
        # Capture a workload's results at the default, then replay it at a
        # higher shard count. The config must change nothing.
        proc run_workload {} {
            r flushall
            set res {}
            lappend res [r mset k1 v1 k2 v2 k3 v3]
            lappend res [r mget k1 k2 k3]
            lappend res [r dbsize]
            lappend res [r incr counter]
            lappend res [r lpush mylist a b c]
            lappend res [r lrange mylist 0 -1]
            lappend res [r del k1 k2]
            lappend res [r exists k3]
            lappend res [lsort [r keys *]]
            lappend res [r ttl k3]
            return $res
        }

        r config set shard-threads 1
        set baseline [run_workload]

        foreach shards {2 8 64} {
            r config set shard-threads $shards
            assert_equal $baseline [run_workload]
        }
        r config set shard-threads 1
    }

    test {shard-threads survives a config rewrite cycle} {
        r config set shard-threads 4
        assert_equal 4 [s shard_threads]
        r config set shard-threads 1
        assert_equal 1 [s shard_threads]
        # Reverting must leave no stale entries in the map.
        foreach slot {0 4096 8192 12288 16383} {
            assert_equal 0 [r debug slot-shard $slot]
        }
    }
}

start_server {tags {"shard-threads external:skip"} overrides {shard-threads 4}} {
    test {shard-threads can be set at startup} {
        assert_equal 4 [s shard_threads]
        assert_equal 0 [r debug slot-shard 0]
        assert_equal 1 [r debug slot-shard 4096]
        assert_equal 2 [r debug slot-shard 8192]
        assert_equal 3 [r debug slot-shard 16383]
    }

    test {commands behave normally with shard-threads set at startup} {
        r set foo bar
        assert_equal bar [r get foo]
        assert_equal 1 [r dbsize]
    }

    test {shard-threads > 1 spawns worker threads} {
        # 4 shards = 1 main + 3 workers. The idle loops own nothing yet; this
        # only proves they spawned and the server serves normally alongside them.
        assert_equal 3 [s shard_threads_active]
    }
}

start_server {tags {"shard-threads external:skip"}} {
    test {shard-threads 1 spawns no worker threads} {
        assert_equal 1 [s shard_threads]
        assert_equal 0 [s shard_threads_active]
    }
}

# The executor read path (safe single-slot reads run on the owning shard's executor
# client) needs c->slot populated, i.e. cluster mode. That test lives in
# tests/unit/cluster/shard-threads-exec.tcl, which uses the cluster harness.

# Clean startup + shutdown with worker threads present is exercised by the
# per-server teardown of the block above (SHUTDOWN joins the shard threads); a
# leaked or unjoined thread would surface in the suite's memory-leak check.
