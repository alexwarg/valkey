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
        assert_equal 0 [s 0 connected_slaves]
        assert_equal 0 [s -1 connected_slaves]
        assert_bidirectional_replication mm_restart_both
    }

    test "MULTIMASTER MERGE restores a node restarted without RDB" {
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
        assert_mm_primaries_linked
        set r0 [srv 0 client]
        assert_equal 0 [$r0 exists mm_merge:list]

        assert_equal OK [$r0 multimaster merge [srv -1 host] [srv -1 port]]
        wait_for_condition 100 100 {
            [s 0 multi_master_merge_in_progress] == 0
        } else {
            fail "MULTIMASTER MERGE did not finish"
        }
        assert_equal ok [s 0 multi_master_merge_last_status]
        assert_equal {a b c} [$r0 lrange mm_merge:list 0 -1]
        assert_equal v19 [$r0 get mm_merge:pre:19]
        assert_equal [$r1 debug digest] [$r0 debug digest]
        assert_mm_primaries_linked
        assert_bidirectional_replication mm_merge_after
    }
}
}
