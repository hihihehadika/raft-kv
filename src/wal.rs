//! Stage 2 & 5: Write-Ahead Log (WAL) and Crash Recovery.
//!
//! We've evolved the WAL from just storing KV operations to storing full Raft state.
//! Raft requires persisting `current_term`, `voted_for`, and the `log` to stable storage.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::raft::LogEntry as RaftLogEntry;

/// A single durable record in the Raft WAL.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum DiskRecord {
    /// Update to current_term and voted_for
    State { current_term: u64, voted_for: Option<u64> },
    /// Appended a new log entry
    Append { entry: RaftLogEntry },
    /// Truncated the log from a specific physical index onwards (conflict resolution)
    Truncate { physical_index: usize },
    /// A compacted snapshot of the state machine
    Snapshot {
        last_included_index: u64,
        last_included_term: u64,
        store_data: HashMap<String, String>,
    },
}

/// Append-only log backed by a file on disk.
pub struct Wal {
    path: PathBuf,
    file: File,
}

impl Wal {
    pub fn open(path: &str) -> io::Result<Self> {
        let path = PathBuf::from(path);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Wal { path, file })
    }

    pub fn append(&mut self, record: &DiskRecord) -> io::Result<()> {
        let mut line = serde_json::to_string(record)?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.sync_all()?; // fsync guarantees durability
        Ok(())
    }

    /// Read every entry from the log file in order.
    pub fn replay(&self) -> io::Result<Vec<DiskRecord>> {
        let f = File::open(&self.path)?;
        let reader = BufReader::new(f);
        let entries = reader
            .lines()
            .filter_map(|line| {
                let line = line.ok()?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return None;
                }
                serde_json::from_str(trimmed).ok()
            })
            .collect();
        Ok(entries)
    }
}
