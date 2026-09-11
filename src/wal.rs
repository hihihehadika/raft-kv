//! Stage 2: Write-Ahead Log (WAL).
//!
//! Every write to the KV store is first appended to an on-disk log file,
//! then applied to the in-memory HashMap. On restart, we replay the log
//! from top to bottom to rebuild the exact same state — this is how real
//! databases (Postgres WAL, SQLite journal) survive crashes.
//!
//! Format: one JSON line per entry, newline-delimited (NDJSON).
//! After every write we call `sync_all()` (fsync equivalent) so the OS
//! guarantees the bytes hit durable storage before we return to the caller.
//!
//! Stage 5 will add log compaction / snapshotting so this file doesn't
//! grow forever.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::store::KvStore;

/// A single durable command in the log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op")]
pub enum LogEntry {
    /// Insert or overwrite a key.
    Set { key: String, value: String },
    /// Remove a key (no-op if the key doesn't exist).
    Delete { key: String },
}

/// Append-only log backed by a file on disk.
pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    /// Open an existing WAL file or create a new one at `path`.
    ///
    /// The file is opened in append mode so every new entry lands
    /// at the end without seeking.
    pub fn open(path: &str) -> io::Result<Self> {
        let path = PathBuf::from(path);
        let file = OpenOptions::new()
            .create(true)  // create if it doesn't exist
            .append(true)  // always write at the end
            .open(&path)?;
        Ok(Wal { path, file })
    }

    /// Serialize `entry` as a JSON line and flush+fsync to disk.
    ///
    /// The fsync (`sync_all`) is intentionally called on every write.
    /// This is the safe default: you know a write is truly durable the
    /// moment this function returns. The trade-off is throughput — a
    /// real system would batch writes and fsync once per batch (the
    /// "group commit" pattern used by Postgres & InnoDB).
    pub fn append(&mut self, entry: &LogEntry) -> io::Result<()> {
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.sync_all()?; // fsync — guarantees durability
        Ok(())
    }

    /// Read every entry from the log file in order.
    ///
    /// Called once at startup to replay the log into a fresh `KvStore`.
    /// Blank lines and malformed lines are silently skipped so a partial
    /// write at the tail (e.g. from a mid-write crash) doesn't corrupt
    /// the whole log.
    pub fn replay(&self) -> io::Result<Vec<LogEntry>> {
        let f = File::open(&self.path)?;
        let reader = BufReader::new(f);
        let entries = reader
            .lines()
            .filter_map(|line| {
                let line = line.ok()?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return None; // skip blank lines
                }
                // Skip lines that can't be parsed (e.g. partial tail write
                // after a crash) rather than blowing up the whole replay.
                serde_json::from_str(trimmed).ok()
            })
            .collect();
        Ok(entries)
    }

    /// Convenience: replay the log and apply every entry to `store`.
    ///
    /// This is what `main` calls on startup to rebuild in-memory state.
    pub fn recover_into(&self, store: &mut KvStore) -> io::Result<usize> {
        let entries = self.replay()?;
        let count = entries.len();
        for entry in entries {
            match entry {
                LogEntry::Set { key, value } => store.set(key, value),
                LogEntry::Delete { key } => {
                    store.delete(&key);
                }
            }
        }
        Ok(count)
    }
}

// --------------------------------------------------------------------------
// Unit tests
// --------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Create a WAL in a temporary file, run `f`, then delete the file.
    fn with_temp_wal<F: FnOnce(&mut Wal, &str)>(name: &str, f: F) {
        let path = format!("target/test_{}.wal", name);
        // Remove any leftover from a previous failed run
        let _ = fs::remove_file(&path);
        let mut wal = Wal::open(&path).expect("open WAL");
        f(&mut wal, &path);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_append_and_replay_set() {
        with_temp_wal("append_replay_set", |wal, path| {
            wal.append(&LogEntry::Set {
                key: "hello".into(),
                value: "world".into(),
            })
            .unwrap();

            // Re-open a fresh WAL handle for replay (simulates restart)
            let wal2 = Wal::open(path).unwrap();
            let entries = wal2.replay().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(
                entries[0],
                LogEntry::Set {
                    key: "hello".into(),
                    value: "world".into()
                }
            );
        });
    }

    #[test]
    fn test_multiple_entries_round_trip() {
        with_temp_wal("multi_entries", |wal, path| {
            wal.append(&LogEntry::Set {
                key: "a".into(),
                value: "1".into(),
            })
            .unwrap();
            wal.append(&LogEntry::Set {
                key: "b".into(),
                value: "2".into(),
            })
            .unwrap();
            wal.append(&LogEntry::Delete { key: "a".into() }).unwrap();

            let wal2 = Wal::open(path).unwrap();
            let entries = wal2.replay().unwrap();
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[2], LogEntry::Delete { key: "a".into() });
        });
    }

    #[test]
    fn test_recover_into_rebuilds_store() {
        with_temp_wal("recover_into", |wal, path| {
            wal.append(&LogEntry::Set {
                key: "x".into(),
                value: "10".into(),
            })
            .unwrap();
            wal.append(&LogEntry::Set {
                key: "y".into(),
                value: "20".into(),
            })
            .unwrap();
            wal.append(&LogEntry::Delete { key: "x".into() }).unwrap();

            // Simulate a restart: fresh store + fresh WAL handle
            let mut store = KvStore::new();
            let wal2 = Wal::open(path).unwrap();
            let replayed = wal2.recover_into(&mut store).unwrap();

            assert_eq!(replayed, 3);
            assert_eq!(store.get("x"), None);           // was deleted
            assert_eq!(store.get("y"), Some(&"20".to_string()));
        });
    }

    #[test]
    fn test_replay_empty_wal() {
        with_temp_wal("replay_empty", |wal, _path| {
            let entries = wal.replay().unwrap();
            assert!(entries.is_empty());
        });
    }

    #[test]
    fn test_delete_nonexistent_key_is_harmless() {
        with_temp_wal("delete_nonexistent", |wal, path| {
            wal.append(&LogEntry::Delete { key: "ghost".into() }).unwrap();

            let mut store = KvStore::new();
            let wal2 = Wal::open(path).unwrap();
            wal2.recover_into(&mut store).unwrap();
            // Nothing should have blown up; store stays empty
            assert!(store.is_empty());
        });
    }
}
