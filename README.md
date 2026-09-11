# raft-kv

A distributed key-value store with Raft consensus, built from scratch in Rust.

Goal: understand — by actually implementing it — how systems like etcd,
CockroachDB, and TiKV keep data consistent across multiple machines.

## Roadmap

### Stage 1 — Single-node KV store (`src/store.rs`)
Plain `HashMap<String, String>` behind `get` / `set` / `delete`. Nothing fancy —
this stage exists so every later stage has a state machine to apply
committed commands to.

**Done when:** you can run the binary, set a few keys, get them back.

### Stage 2 — Durability with a WAL (`src/wal.rs`)
Every write goes to an append-only log file on disk *before* it's applied
to the in-memory map. On startup, replay the log to rebuild state.

**Done when:** you can kill `-9` the process mid-run, restart it, and your
data is still there.

### Stage 3 — Raft consensus (`src/raft.rs`, `src/rpc.rs`)
The main event. Implement:
- Leader election (randomized timeouts, RequestVote RPC)
- Log replication (AppendEntries RPC, majority commit)
- The safety rules that make it actually correct, not just "looks like it works"

**Done when:** you can run 3–5 node processes on your machine, kill the
leader, and watch a new one get elected automatically — with no data loss
for committed writes.

**Read first:** [In Search of an Understandable Consensus Algorithm (the
Raft paper)](https://raft.github.io/raft.pdf) — read Section 5 closely, it's
short and the figures (especially Figure 2) are basically your spec.
[The Raft visualization](https://raft.github.io/) is also great for building
intuition before you write a line of code.

### Stage 4 — Client CLI (`src/bin/cli.rs`)
A small binary to `set` / `get` / `delete` against the cluster. Needs to
handle "this node isn't the leader, redirect me" — that's normal Raft
client behavior, not an edge case.

**Done when:** you can drive the whole cluster from the command line.

### Stage 5 — Snapshotting (bonus, extends `wal.rs` + `raft.rs`)
Without this, your WAL grows forever. Periodically dump the full state to
a snapshot file and truncate the log before that point.

**Done when:** a node that's way behind can catch up via a snapshot instead
of replaying the entire log history.

## Suggested commit cadence

Commit at the end of each stage (or sub-step within a stage) rather than
one giant commit at the end — this project is a great portfolio piece
specifically *because* the commit history shows the system being built up
incrementally.

## Notes

- Every module currently has `todo!()` stubs with comments describing what
  to implement and in what order — start with `store.rs`.
- Dependency suggestions for each stage are commented out in `Cargo.toml`;
  uncomment as you reach that stage instead of pulling everything in at once.
