mod store;
mod wal;
mod raft;
mod rpc;

fn main() {
    println!("raft-kv node starting...");

    // === Stage 1 ===
    // let mut kv = store::KvStore::new();
    // kv.set("hello".into(), "world".into());
    // println!("{:?}", kv.get("hello"));

    // === Stage 2 ===
    // wire up WAL so writes survive a restart

    // === Stage 3 ===
    // spin up the Raft state machine + start listening for RPCs from peers

    // === Stage 4/5 ===
    // handle client requests, snapshotting, etc.
}
