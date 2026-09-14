# raft-kv

![Rust](https://img.shields.io/badge/rust-1.80%2B-blue)

A distributed key-value store powered by the **Raft Consensus Algorithm**, built completely from scratch in Rust.

This project was built to understand the inner workings of distributed systems and how industry-standard databases like etcd, CockroachDB, and TiKV keep data safe, consistent, and highly available across multiple machines.

## Features

*   **Raft Consensus Protocol**: Full implementation of the Raft algorithm including leader election, log replication, and safety guarantees (based on the original [Raft paper](https://raft.github.io/raft.pdf)).
*   **Write-Ahead Logging (WAL)**: Ensures data durability and full crash recovery. The complete Raft state (Term, VotedFor, Log) is serialized, appended, and `fsync`-ed to disk before responding to any RPC.
*   **Strict Linearizability**: Client requests are safely blocked until the write is successfully replicated to a majority of nodes and committed to the state machine, preventing dirty reads or lost updates.
*   **Log Compaction (Snapshotting)**: Prevents the log from growing indefinitely. The system automatically creates snapshots and sends `InstallSnapshot` RPCs to lagging followers.
*   **Asynchronous Event Loop**: Built on top of `tokio` for efficient networking and non-blocking I/O multiplexing.
*   **Custom RPC Layer**: A lightweight, TCP-based JSON RPC protocol with a 4-byte length prefix framing.
*   **Interactive CLI Client**: A robust command-line client that handles transparent cluster routing (automatically redirects to the current leader if a follower is queried).

## Architecture

The project is structured in progressive components, separating the consensus logic from the transport and state machine layers:

1.  `store.rs`: The core in-memory Key-Value state machine (`HashMap`).
2.  `wal.rs`: Disk persistence using append-only logs for crash recovery.
3.  `raft.rs`: A **pure state machine** implementation of Raft. It does no I/O, making it 100% deterministic and incredibly easy to unit test.
4.  `rpc.rs`: The `tokio` TCP networking layer that powers node-to-node communication and client routing.
5.  `main.rs` & `cli.rs`: The asynchronous daemon and the user-facing command line interface.

## Getting Started

### Prerequisites
*   [Rust & Cargo](https://rustup.rs/) (stable)

### Running a Cluster locally

Open three separate terminals to start a 3-node cluster. They will automatically communicate, elect a leader, and start replicating data.

**Node 0:**
```bash
cargo run --bin raft-kv-node -- --id 0 --addr 127.0.0.1:8001 --peers 1=127.0.0.1:8002,2=127.0.0.1:8003
```

**Node 1:**
```bash
cargo run --bin raft-kv-node -- --id 1 --addr 127.0.0.1:8002 --peers 0=127.0.0.1:8001,2=127.0.0.1:8003
```

**Node 2:**
```bash
cargo run --bin raft-kv-node -- --id 2 --addr 127.0.0.1:8003 --peers 0=127.0.0.1:8001,1=127.0.0.1:8002
```

### Using the CLI

In a fourth terminal, use the CLI client to interact with the cluster. By default, it knows the addresses of all 3 nodes and will automatically find the leader.

**Write data:**
```bash
cargo run --bin raft-kv-cli -- set user:1 "Alice"
cargo run --bin raft-kv-cli -- set user:2 "Bob"
```

**Read data:**
```bash
cargo run --bin raft-kv-cli -- get user:1
# Output: Success: Alice
```

**Delete data:**
```bash
cargo run --bin raft-kv-cli -- delete user:2
```

### Simulating Failures

You can safely `Ctrl+C` the leader node. If you watch the logs on the remaining two nodes, you will see an election timeout fire, followed by a new leader being elected. The cluster will continue to accept writes and reads as long as a majority (2 out of 3 nodes) is alive.

## Testing

The pure state machine design of the core consensus layer allows for rigorous unit testing without network mocks.

```bash
cargo test
```

## Acknowledgements
*   [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout.
