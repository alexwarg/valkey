# Applying an RREPLAY payload must not lazily expire keys and re-originate
# the deletion as a new RREPLAY UNLINK. Such an UNLINK carries a fresh HLC
# that is newer than the write being applied, so it deletes that write on
# every node (lost write) or only on the origin (permanent divergence).

start_server {tags {"multi-master external:skip"} overrides {save {} active-replica yes multi-master yes replica-read-only no}} {
start_server {overrides {save {} active-replica yes multi-master yes replica-read-only no}} {

    set R(0) [srv 0 client]
    set R(1) [srv -1 client]
    set RH(0) [srv 0 host]
    set RP(0) [srv 0 port]

    $R(1) multimaster add $RH(0) $RP(0)
    wait_for_condition 150 100 {
        [s -1 active_upstream_runtime_links] >= 1 &&
        [s 0 active_upstream_runtime_links] >= 1
    } else {
        fail "multimaster links not established"
    }

    # Disable active expiry so expired keys stay in the keyspace until a
    # command touches them, which makes the lazy-expire path deterministic.
    $R(0) debug set-active-expire 0
    $R(1) debug set-active-expire 0

    proc wait_for_value_on_both {key expected} {
        wait_for_condition 50 100 {
            [$::R0 get $key] eq $expected && [$::R1 get $key] eq $expected
        } else {
            fail "expected '$expected' on both nodes, got node0='[$::R0 get $key]' node1='[$::R1 get $key]'"
        }
    }
    set ::R0 $R(0)
    set ::R1 $R(1)

    # Create key with a TTL on node 0 and wait until node 1 has it, then let
    # it expire on both nodes without touching it.
    proc setup_expired_key {key} {
        $::R0 set $key v1 EX 1
        wait_for_condition 50 100 {
            [$::R1 get $key] eq "v1"
        } else {
            fail "v1 not replicated to node 1"
        }
        after 1200
    }

    test "SET over a key that expired on both nodes keeps the new value" {
        # Node 1 lazily expires v1 itself and replays UNLINK + SET; node 0
        # must apply both without generating its own UNLINK.
        setup_expired_key mm_expire_lazy
        set expired0 [s 0 expired_keys]
        $R(1) set mm_expire_lazy v2
        wait_for_value_on_both mm_expire_lazy v2
        # Make sure no late UNLINK arrives afterwards.
        after 300
        assert_equal v2 [$R(0) get mm_expire_lazy]
        assert_equal v2 [$R(1) get mm_expire_lazy]
        # Node 0 must not have expired the key while applying the replay.
        assert_equal $expired0 [s 0 expired_keys]
    }

    test "SET over a key already expired on the origin keeps the new value" {
        # Node 1 removes v1 by active expiry (not replicated), so only the SET
        # is replayed and node 0 applies it on top of its expired v1.
        setup_expired_key mm_expire_active
        set expired1 [s -1 expired_keys]
        $R(1) debug set-active-expire 1
        wait_for_condition 50 100 {
            [s -1 expired_keys] > $expired1
        } else {
            fail "node 1 did not actively expire the key"
        }
        $R(1) debug set-active-expire 0
        $R(1) set mm_expire_active v2
        wait_for_value_on_both mm_expire_active v2
        after 300
        assert_equal v2 [$R(0) get mm_expire_active]
        assert_equal v2 [$R(1) get mm_expire_active]
    }

    test "A client access that expires a key does not forward the delete" {
        # Each node expires the key itself from its absolute TTL. A forwarded
        # delete would carry a fresh HLC and could beat a newer write.
        setup_expired_key mm_expire_local
        set size1 [$R(1) dbsize]
        assert_equal {} [$R(0) get mm_expire_local]
        after 300
        # Nobody touched the key on node 1, so it still holds the expired copy.
        assert_equal $size1 [$R(1) dbsize]
    }

    test "A write racing with an expiry on the other node is never lost" {
        set keys 20
        for {set i 0} {$i < $keys} {incr i} { $R(0) set mm_expire_race:$i v1 EX 1 }
        wait_for_condition 50 100 {
            [$R(1) get mm_expire_race:[expr {$keys - 1}]] eq "v1"
        } else {
            fail "v1 not replicated to node 1"
        }
        after 1200

        # Send the new write on node 1 and the expiring read on node 0 before
        # either reply is read, so they are concurrent.
        set rd0 [valkey_deferring_client 0]
        set rd1 [valkey_deferring_client -1]
        for {set i 0} {$i < $keys} {incr i} {
            $rd1 set mm_expire_race:$i v2
            $rd0 get mm_expire_race:$i
            $rd1 read
            $rd0 read
        }
        $rd0 close
        $rd1 close

        wait_for_condition 50 100 {
            [$R(0) get mm_expire_race:[expr {$keys - 1}]] eq "v2"
        } else {
            fail "last write did not reach node 0"
        }
        after 300
        for {set i 0} {$i < $keys} {incr i} {
            assert_equal v2 [$R(0) get mm_expire_race:$i]
            assert_equal v2 [$R(1) get mm_expire_race:$i]
        }
    }

    $R(0) debug set-active-expire 1
    $R(1) debug set-active-expire 1
}
}
