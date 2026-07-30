# `shard-threads` partitions the 16384 hash slots across that many execution shards and,
# at > 1, spawns a thread per shard. It is startup-only (immutable at runtime): the
# threads own slots, so the count cannot change under a running server. The exhaustive
# partition math lives in the unit test (test_slot_shard.cpp); these tests cover the
# config surface, thread spawn/teardown, and the barrier end to end.

source tests/support/cluster.tcl

# Expected owner of $slot when the slots are split across $shards shards.
proc expected_shard {slot shards} {
    expr {$slot * $shards / 16384}
}

proc key_for_different_shard {client_shard shards prefix} {
    for {set i 0} {$i < 10000} {incr i} {
        set key "$prefix:$i"
        set slot [::valkey_cluster::hash $key]
        if {[expected_shard $slot $shards] != $client_shard} {
            return $key
        }
    }
    fail "No key found outside client shard $client_shard"
}

proc cmdstat_calls {cmd} {
    set info [r info commandstats]
    if {![regexp "cmdstat_${cmd}:calls=(\[0-9\]+)" $info -> calls]} {
        fail "No commandstats entry for $cmd"
    }
    return $calls
}

start_server {tags {"shard-threads"}} {
    test {shard-threads defaults to 1 and is reported by INFO} {
        assert_equal {shard-threads 1} [r config get shard-threads]
        assert_equal 1 [s shard_threads]
        assert_equal 0 [s shard_threads_active]
    }

    test {DEBUG SLOT-SHARD maps every slot to shard 0 by default} {
        foreach slot {0 1 4095 4096 8192 12288 16382 16383} {
            assert_equal 0 [r debug slot-shard $slot]
        }
    }

    test {shard-threads is immutable at runtime} {
        # It spawns threads that own slots, so the count is fixed for the process.
        assert_error "*immutable*" {r config set shard-threads 4}
        assert_equal 1 [s shard_threads]
    }

    test {DEBUG SLOT-SHARD rejects out of range slots} {
        assert_error "*Invalid or out of range slot*" {r debug slot-shard -1}
        assert_error "*Invalid or out of range slot*" {r debug slot-shard 16384}
        assert_error "*Invalid or out of range slot*" {r debug slot-shard notanumber}
    }

    test {the barrier is a no-op with no workers} {
        assert_equal 0 [r debug shard-barrier]
    }
}

start_server {tags {"shard-threads external:skip"} overrides {shard-threads 4}} {
    test {shard-threads 4 partitions the map into contiguous balanced ranges} {
        assert_equal 4 [s shard_threads]
        foreach slot {0 1 4095 4096 5461 5462 8191 8192 12287 12288 16383} {
            assert_equal [expected_shard $slot 4] [r debug slot-shard $slot]
        }
    }

    test {shard-threads 4 spawns 3 worker threads} {
        assert_equal 3 [s shard_threads_active]
    }

    test {the escalation barrier quiesces every worker} {
        # DEBUG SHARD-BARRIER takes the barrier and releases it, replying with the number
        # of workers that parked. All 3 park, repeatably; commands keep working after.
        for {set i 0} {$i < 20} {incr i} {
            assert_equal 3 [r debug shard-barrier]
        }
        r set bk bv
        assert_equal bv [r get bk]
    }

    test {keyspace behavior is identical at shard-threads 4 (standalone)} {
        # Standalone has no slots (c->slot is -1), so commands don't take the
        # slot-owner remote read path. Results must match a plain server.
        r flushall
        assert_equal OK [r mset k1 v1 k2 v2 k3 v3]
        assert_equal {v1 v2 v3} [r mget k1 k2 k3]
        assert_equal 3 [r dbsize]
        assert_equal 1 [r incr counter]
        assert_equal 3 [r lpush mylist a b c]
        assert_equal {c b a} [r lrange mylist 0 -1]
        assert_equal 2 [r del k1 k2]
        assert_equal 1 [r exists k3]
    }

    test {accepted TCP clients execute and appear in CLIENT LIST at shard-threads 4} {
        set clients {}
        for {set i 0} {$i < 12} {incr i} {
            set rd [valkey_client]
            lappend clients $rd
            assert_equal PONG [$rd ping]
            assert_equal OK [$rd client setname shard-client-$i]
            assert_equal OK [$rd set shard-client:$i $i]
            assert_equal $i [$rd get shard-client:$i]
        }

        set listed [r client list]
        for {set i 0} {$i < 12} {incr i} {
            assert_match "*name=shard-client-$i *" $listed
        }

        foreach rd $clients {
            $rd close
        }
        wait_for_condition 50 100 {
            [regexp {connected_clients:1\r\n} [r info clients]] &&
            [lsearch [split [r client list] "\r\n"] *name=shard-client-*] == -1
        } else {
            fail "Shard-thread clients did not disconnect"
        }
        r commandlog reset slow
        r commandlog reset large-request
        r commandlog reset large-reply
        r flushall
    }

    test {remote slot-owner SET updates commandstats} {
        r config resetstat
        set rd [valkey_client]
        set client_shard [$rd debug current-shard]
        set key [key_for_different_shard $client_shard 4 remote-cmdstats]
        assert_equal OK [$rd set $key remote-cmdstats-value]
        assert_equal remote-cmdstats-value [$rd get $key]
        $rd close
        assert {[cmdstat_calls set] >= 1}
        r del $key
    }

    test {remote slot-owner write variants preserve state and replies} {
        set rd [valkey_client]
        set client_shard [$rd debug current-shard]
        set key [key_for_different_shard $client_shard 4 remote-write-variants]

        assert_equal OK [$rd set $key v1]
        assert_equal v1 [$rd getset $key v2]
        assert_equal v2 [$rd get $key]
        assert_equal v2 [$rd getdel $key]
        assert_equal {} [$rd get $key]

        assert_equal OK [$rd set $key v3]
        assert_equal 1 [$rd del $key]
        assert_equal 0 [$rd exists $key]

        assert_equal OK [$rd set $key v4]
        assert_equal 1 [$rd expire $key 100]
        assert_equal v4 [$rd get $key]

        $rd close
        r del $key
    }
}

start_server {tags {"shard-threads external:skip"} overrides {shard-threads 4 appendonly yes appendfsync always save ""}} {
    test {remote slot-owner SET is loaded from AOF} {
        set rd [valkey_client]
        set client_shard [$rd debug current-shard]
        set key [key_for_different_shard $client_shard 4 remote-aof]
        assert_equal OK [$rd set $key remote-aof-value]
        $rd close

        restart_server 0 true false
        assert_equal remote-aof-value [r get $key]
    }
}

start_server {tags {"shard-threads external:skip"} overrides {shard-threads 4 save ""}} {
    start_server {overrides {save ""}} {
        test {remote slot-owner SET is replicated} {
            set primary [srv -1 client]
            set primary_host [srv -1 host]
            set primary_port [srv -1 port]
            set replica [srv 0 client]

            $replica replicaof $primary_host $primary_port
            wait_for_condition 50 100 {
                [status $primary connected_slaves] == 1
            } else {
                fail "Replica did not connect"
            }

            set rd [valkey_client -1]
            set client_shard [$rd debug current-shard]
            set key [key_for_different_shard $client_shard 4 remote-repl]
            assert_equal OK [$rd set $key remote-repl-value]
            $rd close

            wait_for_condition 50 100 {
                [$replica get $key] eq {remote-repl-value}
            } else {
                fail "Replica did not receive remote slot-owner SET"
            }
        }
    }
}

start_server {tags {"shard-threads external:skip"} overrides {shard-threads 7}} {
    test {shard-threads 7 partition is contiguous and balanced} {
        assert_equal 0 [r debug slot-shard 0]
        assert_equal 6 [r debug slot-shard 16383]
        set prev 0
        for {set slot 0} {$slot < 16384} {incr slot 37} {
            set shard [r debug slot-shard $slot]
            assert {$shard >= $prev}
            assert {$shard <= $prev + 1}
            set prev $shard
        }
    }
}

# The executor read path (safe single-slot reads execute on the owning shard's thread)
# needs c->slot populated, i.e. cluster mode. It is tested in
# tests/unit/cluster/shard-threads-exec.tcl, which uses the cluster harness.
#
# Clean startup + shutdown with worker threads present is exercised by each block's
# per-server teardown (SHUTDOWN joins the shard threads); a leaked or unjoined thread
# would surface in the suite's memory-leak check.
