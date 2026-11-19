# Snapshot merge (RDBFLAGS_MERGE_HLC, via DEBUG RELOAD MERGE-HLC): an RDB is
# merged into the live dataset per key by HLC last-write-wins. Clocks kept for
# deleted keys act as tombstones, and equal clocks resolve deterministically so
# that two nodes merging each other's snapshots converge.

start_server {tags {"multi-master external:skip needs:debug"} overrides {save {} active-replica yes multi-master yes}} {
start_server {overrides {save {} active-replica yes multi-master yes}} {

    # Node A (srv 0) is the merging node, node B (srv -1) provides the snapshot.
    set A [srv 0 client]
    set B [srv -1 client]

    # Write a string value with an explicit HLC wall time.
    proc set_with_clock {r key value ts} {
        $r set __mm_merge_tmp $value
        set payload [$r dump __mm_merge_tmp]
        $r del __mm_merge_tmp
        $r hlcrestore $key 0 $payload $ts replace
    }

    # A key that was written at 'ts' and then deleted: only its clock remains.
    proc tombstone_with_clock {r key ts} {
        set_with_clock $r $key gone $ts
        $r del $key
    }

    # Merge the snapshot of node 'src' (level) into node 'dst' (level).
    proc merge_snapshot {src dst} {
        [srv $src client] save
        set src_dir [lindex [[srv $src client] config get dir] 1]
        set dst_dir [lindex [[srv $dst client] config get dir] 1]
        file copy -force [file join $src_dir dump.rdb] [file join $dst_dir dump.rdb]
        assert_equal OK [[srv $dst client] debug reload merge-hlc]
    }

    test "Snapshot merge resolves each key by HLC" {
        # newer local value survives
        set_with_clock $A k:local-newer local 200
        set_with_clock $B k:local-newer snap 100
        # newer snapshot value replaces the local one
        set_with_clock $A k:snap-newer local 100
        set_with_clock $B k:snap-newer snap 200
        # key only in the snapshot is added
        set_with_clock $B k:snap-only snap 100
        # snapshot tombstone deletes an older local key
        set_with_clock $A k:snap-deleted local 100
        tombstone_with_clock $B k:snap-deleted 200
        # local tombstone blocks an older snapshot value
        tombstone_with_clock $A k:local-deleted 200
        set_with_clock $B k:local-deleted snap 100
        # local key without any clock and absent from the snapshot is kept
        $A set k:local-only local

        merge_snapshot -1 0

        assert_equal local [$A get k:local-newer]
        assert_equal snap [$A get k:snap-newer]
        assert_equal snap [$A get k:snap-only]
        assert_equal 0 [$A exists k:snap-deleted]
        assert_equal 0 [$A exists k:local-deleted]
        assert_equal local [$A get k:local-only]
    }

    test "Snapshot merge keeps an older snapshot tombstone from deleting a newer local key" {
        set_with_clock $A k:tomb-older local 300
        tombstone_with_clock $B k:tomb-older 200
        merge_snapshot -1 0
        assert_equal local [$A get k:tomb-older]
    }

    test "Merging each other's snapshots converges, including equal clocks" {
        set_with_clock $A k:tie aaa 500
        set_with_clock $B k:tie zzz 500
        set_with_clock $A k:a-only from-a 400
        set_with_clock $B k:b-only from-b 400

        merge_snapshot -1 0
        merge_snapshot 0 -1

        assert_equal [$A get k:tie] [$B get k:tie]
        assert_equal from-a [$B get k:a-only]
        assert_equal from-b [$A get k:b-only]
        assert_equal [$A debug digest] [$B debug digest]
    }

    test "Merging the same snapshot twice changes nothing" {
        set digest [$A debug digest]
        merge_snapshot -1 0
        assert_equal $digest [$A debug digest]
        merge_snapshot -1 0
        assert_equal $digest [$A debug digest]
    }

    test "DEBUG RELOAD MERGE-HLC rejects MERGE" {
        assert_error {*cannot be combined*} {$A debug reload merge merge-hlc}
    }

    test "Multi-master primary without a backlog keeps HLC clocks across restart" {
        # No replica ever connected, so there is no replication info to save;
        # the multi-master fields must be saved anyway.
        assert_equal 0 [s 0 repl_backlog_active]
        set entries [s 0 hlc_key_clock_entries]
        assert {$entries > 0}
        $A save
        restart_server 0 true false
        assert_equal $entries [s 0 hlc_key_clock_entries]
    }
}
}
