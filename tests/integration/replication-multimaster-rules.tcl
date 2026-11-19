# Multi-master replaces primary/replica replication: a multi-master node can
# neither be a replica nor have replicas, and cannot run in cluster mode. It
# only serves rdb-only snapshots, which peers merge into their dataset.

start_server {tags {"multi-master external:skip"} overrides {save {} active-replica yes multi-master yes replica-read-only no}} {
start_server {overrides {save {}}} {
    set mm [srv -1 client]
    set mm_host [srv -1 host]
    set mm_port [srv -1 port]
    set plain [srv 0 client]
    set plain_host [srv 0 host]
    set plain_port [srv 0 port]

    test "REPLICAOF is refused in multi-master mode" {
        assert_error {*REPLICAOF not allowed in multi-master mode*} {$mm replicaof $plain_host $plain_port}
        assert_error {*REPLICAOF not allowed in multi-master mode*} {$mm replicaof no one}
        assert_equal master [s -1 role]
    }

    test "A multi-master node refuses SYNC and PSYNC from replicas" {
        assert_error {*do not accept replicas*} {$mm sync}
        assert_error {*do not accept replicas*} {$mm psync ? -1}
    }

    test "A replica of a multi-master node never gets online" {
        $plain replicaof $mm_host $mm_port
        after 1500
        assert_equal down [s 0 master_link_status]
        assert_equal 0 [s -1 connected_slaves]
        $plain replicaof no one
    }

    test "A multi-master node serves rdb-only snapshots" {
        $mm set mm:rules:snapshot v1
        set rdb [file join [lindex [$plain config get dir] 1] mm-rules-snapshot.rdb]
        exec src/valkey-cli -h $mm_host -p $mm_port --rdb $rdb 2>@1
        assert {[file size $rdb] > 0}
        file delete $rdb
    }

    test "Multi-master cannot be enabled on a replica" {
        $plain config set active-replica yes
        $plain replicaof $mm_host $mm_port
        assert_error {*cannot be enabled on a replica*} {$plain config set multi-master yes}
        $plain replicaof no one
        $plain config set active-replica no
    }
}
}

start_server {tags {"multi-master external:skip"}} {
    test "Startup refuses multi-master with cluster mode or a replicaof directive" {
        set server_path [tmpdir server.mm-rules]
        catch {exec src/valkey-server --port 0 --dir $server_path --active-replica yes --multi-master yes --cluster-enabled yes} err
        assert_match {*multi-master is not compatible with cluster mode*} $err
        catch {exec src/valkey-server --port 0 --dir $server_path --active-replica yes --multi-master yes --replicaof "127.0.0.1 1"} err
        assert_match {*replicaof directive not allowed with multi-master*} $err
    }
}
