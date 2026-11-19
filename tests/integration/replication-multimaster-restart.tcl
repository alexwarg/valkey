# A multi-master node must come back from a restart as a primary with its
# peer links restored, whether or not an RDB is loaded. Peers must never be
# turned into the node's primary, otherwise two restarted peers become each
# other's replicas and a node's own replicas end up chained behind a peer.

start_server {tags {"multi-master external:skip"} overrides {save {} active-replica yes multi-master yes replica-read-only no}} {
start_server {overrides {save {} active-replica yes multi-master yes replica-read-only no}} {

    set RH(0) [srv 0 host]
    set RP(0) [srv 0 port]

    [srv -1 client] multimaster add $RH(0) $RP(0)

    proc assert_mm_primaries_linked {} {
        wait_for_condition 100 100 {
            [s 0 loading] eq {0} && [s -1 loading] eq {0} &&
            [s 0 active_upstream_runtime_links] >= 1 &&
            [s -1 active_upstream_runtime_links] >= 1
        } else {
            fail "multimaster links not established"
        }
        assert_equal master [s 0 role]
        assert_equal master [s -1 role]
        assert_equal 1 [s 0 configured_upstreams]
        assert_equal 1 [s -1 configured_upstreams]
    }

    proc assert_bidirectional_replication {key} {
        set r0 [srv 0 client]
        set r1 [srv -1 client]
        $r0 set $key from0
        wait_for_condition 50 100 {
            [$r1 get $key] eq "from0"
        } else {
            fail "write on node 0 did not reach node 1"
        }
        $r1 set $key from1
        wait_for_condition 50 100 {
            [$r0 get $key] eq "from1"
        } else {
            fail "write on node 1 did not reach node 0"
        }
    }

    test "Multimaster add links both nodes as primaries" {
        assert_mm_primaries_linked
        assert_bidirectional_replication mm_restart_initial
    }

    test "Restarting both nodes without RDB restores peers from local state" {
        # Restart both: a surviving peer would otherwise re-add itself over its
        # link and hide a missing restore.
        assert_equal 0 [file exists [file join [lindex [[srv 0 client] config get dir] 1] dump.rdb]]
        assert_equal 0 [file exists [file join [lindex [[srv -1 client] config get dir] 1] dump.rdb]]
        restart_server 0 true false
        restart_server -1 true false
        assert_mm_primaries_linked
        assert_bidirectional_replication mm_restart_no_rdb
    }

    test "Restart with RDB restores peers and stays primary" {
        [srv 0 client] save
        restart_server 0 true false
        assert_mm_primaries_linked
        assert_bidirectional_replication mm_restart_rdb
    }

    test "Restarting both nodes with RDB keeps both primaries" {
        [srv 0 client] save
        [srv -1 client] save
        restart_server 0 true false
        restart_server -1 true false
        assert_mm_primaries_linked
        # Startup merges briefly hold rdb-only snapshot links; once they are
        # done, neither node is a replica of the other.
        wait_for_condition 100 100 {
            [s 0 multi_master_merge_pending_peers] == 0 &&
            [s -1 multi_master_merge_pending_peers] == 0 &&
            [s 0 connected_slaves] == 0 &&
            [s -1 connected_slaves] == 0
        } else {
            fail "startup merges did not finish or a node kept a replica"
        }
        assert_bidirectional_replication mm_restart_both
    }

    proc wait_startup_merges_done {} {
        wait_for_condition 100 100 {
            [s 0 loading] eq {0} &&
            [s 0 multi_master_merge_pending_peers] == 0 &&
            [s 0 multi_master_merge_in_progress] == 0
        } else {
            fail "startup merge did not finish"
        }
        assert_equal ok [s 0 multi_master_merge_last_status]
    }

    test "Restart without RDB merges the peer's data, writes during the merge arrive once" {
        set r1 [srv -1 client]
        for {set i 0} {$i < 20} {incr i} { $r1 set mm_merge:pre:$i v$i }
        $r1 rpush mm_merge:list a b c
        wait_for_condition 50 100 {
            [[srv 0 client] llen mm_merge:list] == 3
        } else {
            fail "data not replicated to node 0 before the restart"
        }

        set dir [lindex [[srv 0 client] config get dir] 1]
        file delete [file join $dir dump.rdb]
        restart_server 0 true false
        # Writes on the peer while node 0 merges: they must neither be lost
        # nor applied twice (the list would show duplicates).
        for {set i 0} {$i < 20} {incr i} {
            $r1 rpush mm_merge:list d$i
            after 20
        }
        wait_startup_merges_done
        assert_mm_primaries_linked

        set r0 [srv 0 client]
        wait_for_condition 50 100 {
            [$r0 llen mm_merge:list] == 23
        } else {
            fail "writes made during the startup merge did not reach node 0: [$r0 lrange mm_merge:list 0 -1]"
        }
        assert_equal [$r1 lrange mm_merge:list 0 -1] [$r0 lrange mm_merge:list 0 -1]
        assert_equal v19 [$r0 get mm_merge:pre:19]
        assert_equal [$r1 debug digest] [$r0 debug digest]
        assert_bidirectional_replication mm_merge_after
    }

    test "Restart with RDB recovers writes made after the last save" {
        set r0 [srv 0 client]
        set r1 [srv -1 client]
        $r0 save
        # Written after the save: replicated to node 1, then lost by node 0's
        # restart (save is disabled, so shutdown does not save it).
        $r0 set mm_merge:after-save from-node0
        wait_for_condition 50 100 {
            [$r1 get mm_merge:after-save] eq "from-node0"
        } else {
            fail "write did not reach node 1"
        }

        restart_server 0 true false
        wait_startup_merges_done
        assert_equal from-node0 [[srv 0 client] get mm_merge:after-save]
        assert_equal [$r1 debug digest] [[srv 0 client] debug digest]
    }

    test "Replay queue overflow makes the peer merge the sender's snapshot" {
        set r0 [srv 0 client]
        set r1 [srv -1 client]
        set max_entries [lindex [$r1 config get rreplay-pending-max-entries] 1]
        set requests [s -1 upstream_runtime_replay_fullsync_requests]
        $r1 config set rreplay-pending-max-entries 5

        # node 0 stops acking while node 1 writes, so node 1's queue overflows.
        set pid0 [srv 0 pid]
        exec kill -STOP $pid0
        if {[catch {
            for {set i 0} {$i < 40} {incr i} { $r1 set mm_overflow:$i v$i }
        } err]} {
            exec kill -CONT $pid0
            error $err
        }
        exec kill -CONT $pid0

        wait_for_condition 100 100 {
            [s -1 upstream_runtime_replay_fullsync_requests] > $requests
        } else {
            fail "node 1 did not ask node 0 to merge after its replay queue overflowed"
        }
        wait_for_condition 100 100 {
            [$r0 get mm_overflow:39] eq "v39" && [s 0 multi_master_merge_pending_peers] == 0
        } else {
            fail "node 0 did not recover the writes dropped from node 1's replay queue"
        }
        for {set i 0} {$i < 40} {incr i} {
            assert_equal v$i [$r0 get mm_overflow:$i]
        }
        $r1 config set rreplay-pending-max-entries $max_entries
        assert_equal [$r1 debug digest] [$r0 debug digest]
    }
}
}
