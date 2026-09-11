//! Stage 2: Write-Ahead Log (WAL).
//!
//! Goal: every write to the store is first appended to a log file on disk.
//! On restart, replay the log to rebuild the in-memory state. This is the
//! same idea used by real databases (Postgres, SQLite, etc.) for durability.
//!
//! TODO:
//! - [ ] Define a `LogEntry` enum: Set { key, value } | Delete { key }
//! - [ ] Append entries to a file, one per line (e.g. JSON or bincode)
//! - [ ] fsync after each write (or batch, and explain the tradeoff)
//! - [ ] On startup, read the file back and replay entries into KvStore
//! - [ ] (Later, Stage 5) implement log compaction / snapshotting so the
//!       file doesn't grow forever

pub struct Wal {
    // path: PathBuf,
    // file: File,
}

impl Wal {
    pub fn open(_path: &str) -> Self {
        todo!("open or create the log file")
    }

    pub fn append(&mut self /*, entry: LogEntry */) {
        todo!("serialize entry and append to file, then fsync")
    }

    pub fn replay(&self) /* -> Vec<LogEntry> */
    {
        todo!("read all entries from the file in order")
    }
}
