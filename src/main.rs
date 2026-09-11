mod store;
mod wal;
mod raft;
mod rpc;

fn main() {
    println!("raft-kv node starting...");

    // === Stage 1: demo ===
    let mut kv = store::KvStore::new();

    kv.set("user:1".into(), "alice".into());
    kv.set("user:2".into(), "bob".into());
    kv.set("user:3".into(), "carol".into());
    kv.set("config:host".into(), "localhost".into());
    kv.set("config:port".into(), "8080".into());

    println!("get user:1  => {:?}", kv.get("user:1"));
    println!("get ghost   => {:?}", kv.get("ghost"));

    kv.set("user:1".into(), "ALICE-updated".into());
    println!("after overwrite user:1 => {:?}", kv.get("user:1"));

    let removed = kv.delete("user:2");
    println!("deleted user:2 => {:?}", removed);
    println!("get user:2 after delete => {:?}", kv.get("user:2"));

    println!("scan prefix 'user:' =>");
    for (k, v) in kv.scan("user:") {
        println!("  {} = {}", k, v);
    }

    println!("total keys: {}", kv.len());

    // === Stage 2 ===
    // wire up WAL so writes survive a restart

    // === Stage 3 ===
    // spin up the Raft state machine + start listening for RPCs from peers

    // === Stage 4/5 ===
    // handle client requests, snapshotting, etc.
}
