mod store;
mod wal;
mod raft;
mod rpc;

use store::KvStore;
use wal::{LogEntry, Wal};

const WAL_PATH: &str = "raft-kv.wal";

fn main() {
    println!("raft-kv node starting...");

    // === Stage 2: WAL-backed KV store ===
    //
    // On startup: open (or create) the WAL and replay any prior entries
    // into a fresh in-memory store.  This is what makes data survive a
    // crash or restart.
    let mut wal = Wal::open(WAL_PATH).expect("failed to open WAL");
    let mut kv = KvStore::new();

    let replayed = wal.recover_into(&mut kv).expect("WAL replay failed");
    println!("[WAL] replayed {} entries from disk", replayed);
    println!("[State after replay] total keys: {}", kv.len());

    // Apply new writes through the WAL: log it first, then apply.
    let writes: Vec<LogEntry> = vec![
        LogEntry::Set { key: "user:1".into(), value: "alice".into() },
        LogEntry::Set { key: "user:2".into(), value: "bob".into() },
        LogEntry::Set { key: "config:host".into(), value: "localhost".into() },
        LogEntry::Delete { key: "user:2".into() },
    ];

    for entry in writes {
        wal.append(&entry).expect("WAL append failed");
        match &entry {
            LogEntry::Set { key, value } => kv.set(key.clone(), value.clone()),
            LogEntry::Delete { key } => { kv.delete(key); }
        }
    }

    println!("[After writes]");
    println!("  get user:1       => {:?}", kv.get("user:1"));
    println!("  get user:2       => {:?}", kv.get("user:2"));  // deleted
    println!("  get config:host  => {:?}", kv.get("config:host"));
    println!("  total keys       => {}", kv.len());
    println!();
    println!("Try restarting the binary — data will still be there.");
    println!("Delete '{}' to start fresh.", WAL_PATH);

    // === Stage 3 ===
    // spin up the Raft state machine + start listening for RPCs from peers

    // === Stage 4/5 ===
    // handle client requests, snapshotting, etc.
}
