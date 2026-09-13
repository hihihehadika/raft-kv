use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::time;

use raft_kv::raft::{Command, OutboundMsg, RaftNode};
use raft_kv::rpc::{ClientReply, ClientRequest, Inbound, RpcClient, RpcMessage, RpcServer, to_wire};
use raft_kv::store::KvStore;

#[derive(Parser)]
#[command(name = "raft-kv-node")]
#[command(about = "A single node in the raft-kv cluster", long_about = None)]
struct Cli {
    /// The ID of this node (e.g., 0, 1, or 2)
    #[arg(long)]
    id: u64,

    /// The address this node listens on (e.g., 127.0.0.1:8001)
    #[arg(long)]
    addr: SocketAddr,

    /// Comma-separated list of peer ID=Address (e.g., 1=127.0.0.1:8002,2=127.0.0.1:8003)
    #[arg(long, default_value = "")]
    peers: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Parse peers
    let mut peers_map = HashMap::new();
    let mut peer_ids = Vec::new();
    if !cli.peers.is_empty() {
        for pair in cli.peers.split(',') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() == 2 {
                let pid: u64 = parts[0].parse()?;
                let paddr: SocketAddr = parts[1].parse()?;
                peers_map.insert(pid, paddr);
                peer_ids.push(pid);
            }
        }
    }

    use raft_kv::wal::{Wal, DiskRecord};
    
    println!("[node {}] Starting up on {}...", cli.id, cli.addr);
    
    // Initialize Raft node and KV Store
    let mut node = RaftNode::new(cli.id, peer_ids);
    let mut store = KvStore::new();

    // -----------------------------------------------------------------------
    // Crash Recovery (WAL)
    // -----------------------------------------------------------------------
    let wal_path = format!("node_{}.wal", cli.id);
    let mut wal = Wal::open(&wal_path)?;
    
    match wal.replay() {
        Ok(records) => {
            let count = records.len();
            for rec in records {
                match rec {
                    DiskRecord::State { current_term, voted_for } => {
                        node.current_term = current_term;
                        node.voted_for = voted_for;
                    }
                    DiskRecord::Append { entry } => {
                        node.log.push(entry);
                    }
                    DiskRecord::Truncate { physical_index } => {
                        node.log.truncate(physical_index);
                    }
                    DiskRecord::Snapshot { last_included_index, last_included_term, store_data } => {
                        node.last_included_index = last_included_index;
                        node.last_included_term = last_included_term;
                        // Sentinel
                        node.log.clear();
                        node.log.push(raft_kv::raft::LogEntry {
                            term: last_included_term,
                            command: raft_kv::raft::Command::Set { key: String::new(), value: String::new() },
                        });
                        node.commit_index = last_included_index as usize;
                        node.last_applied = last_included_index as usize;
                        store.load(store_data);
                    }
                }
            }
            if count > 0 {
                println!("[node {}] Recovered {} records from WAL. Term: {}, Log size: {}", cli.id, count, node.current_term, node.log.len());
            }
        }
        Err(e) => {
            eprintln!("[node {}] Failed to replay WAL: {}. Starting fresh.", cli.id, e);
        }
    }

    // -----------------------------------------------------------------------
    // Network layer setup
    // -----------------------------------------------------------------------
    let rpc_client = RpcClient::new(peers_map);
    let (inbox_tx, mut inbox_rx) = mpsc::channel::<Inbound>(100);

    let server = RpcServer {
        addr: cli.addr,
        node_id: cli.id,
        inbox_tx,
    };
    
    // Run the server in the background
    tokio::spawn(server.run());

    // -----------------------------------------------------------------------
    // Timers
    // -----------------------------------------------------------------------
    // 10ms tick for all time-based events
    let mut ticker = time::interval(Duration::from_millis(10));
    
    let mut election_timeout = gen_election_timeout();
    let mut election_elapsed = 0;
    
    let heartbeat_timeout = 50; // 50ms
    let mut heartbeat_elapsed = 0;

    // Helper closure to dispatch outbound messages
    let dispatch = |msgs: Vec<OutboundMsg>, client: &RpcClient| {
        for m in msgs {
            let (to, rpc_msg) = to_wire(cli.id, m);
            let client = client.clone();
            // Fire and forget
            tokio::spawn(async move {
                client.send(to, &rpc_msg).await;
            });
        }
    };

    let mut pending_clients: HashMap<u64, tokio::sync::oneshot::Sender<RpcMessage>> = HashMap::new();

    println!("[node {}] Cluster ready. Waiting for events...", cli.id);

    // -----------------------------------------------------------------------
    // The Main Event Loop
    // -----------------------------------------------------------------------
    loop {
        let old_term = node.current_term;
        let old_voted_for = node.voted_for;
        let old_last_included_index = node.last_included_index;
        let old_log = node.log.clone();

        tokio::select! {
            _ = ticker.tick() => {
                // Timer logic
                if node.state == raft_kv::raft::NodeState::Leader {
                    heartbeat_elapsed += 10;
                    if heartbeat_elapsed >= heartbeat_timeout {
                        heartbeat_elapsed = 0;
                        let msgs = node.send_heartbeats();
                        dispatch(msgs, &rpc_client);
                    }
                } else {
                    election_elapsed += 10;
                    if election_elapsed >= election_timeout {
                        println!("[node {}] Election timeout! Starting election for term {}...", cli.id, node.current_term + 1);
                        election_elapsed = 0;
                        election_timeout = gen_election_timeout(); // randomize next
                        let msgs = node.start_election();
                        dispatch(msgs, &rpc_client);
                    }
                }
            }

            Some(inbound) = inbox_rx.recv() => {
                let old_state = node.state.clone();
                
                // Process incoming network message
                let mut reset_election = false;

                match inbound.msg {
                    RpcMessage::RequestVote(env) => {
                        let (reply, msgs) = node.handle_request_vote(env.payload);
                        if reply.vote_granted {
                            reset_election = true;
                        }
                        dispatch(msgs, &rpc_client);
                        // Send reply
                        rpc_client.send(env.from, &RpcMessage::RequestVoteReply(raft_kv::rpc::Envelope {
                            from: cli.id,
                            to: env.from,
                            payload: reply,
                        })).await;
                    }
                    RpcMessage::RequestVoteReply(env) => {
                        let msgs = node.handle_request_vote_reply(env.from, env.payload);
                        dispatch(msgs, &rpc_client);
                    }
                    RpcMessage::AppendEntries(env) => {
                        reset_election = true; // Any AppendEntries resets election timeout
                        let (reply, msgs) = node.handle_append_entries(env.payload);
                        dispatch(msgs, &rpc_client);
                        rpc_client.send(env.from, &RpcMessage::AppendEntriesReply(raft_kv::rpc::Envelope {
                            from: cli.id,
                            to: env.from,
                            payload: reply,
                        })).await;
                    }
                    RpcMessage::AppendEntriesReply(env) => {
                        let msgs = node.handle_append_entries_reply(env.from, env.payload);
                        dispatch(msgs, &rpc_client);
                    }
                    RpcMessage::InstallSnapshot(env) => {
                        reset_election = true;
                        let (reply, msgs) = node.handle_install_snapshot(env.payload);
                        dispatch(msgs, &rpc_client);
                        rpc_client.send(env.from, &RpcMessage::InstallSnapshotReply(raft_kv::rpc::Envelope {
                            from: cli.id,
                            to: env.from,
                            payload: reply,
                        })).await;
                    }
                    RpcMessage::InstallSnapshotReply(env) => {
                        let msgs = node.handle_install_snapshot_reply(env.from, env.payload);
                        dispatch(msgs, &rpc_client);
                    }
                    RpcMessage::ClientRequest(req) => {
                        // Handle request from the CLI client
                        match req {
                            ClientRequest::Get { key } => {
                                // Simple read (stale read possible, but okay for this demo)
                                let reply = ClientReply::Success { value: store.get(&key).cloned() };
                                if let Some(tx) = inbound.reply_tx {
                                    let _ = tx.send(RpcMessage::ClientReply(reply));
                                }
                            }
                            ClientRequest::Set { key, value } => {
                                match node.propose(Command::Set { key, value }) {
                                    Ok((log_index, msgs)) => {
                                        dispatch(msgs, &rpc_client);
                                        if let Some(tx) = inbound.reply_tx {
                                            pending_clients.insert(log_index, tx);
                                        }
                                    }
                                    Err(_) => {
                                        let reply = ClientReply::NotLeader { leader_id: node.leader_id };
                                        if let Some(tx) = inbound.reply_tx {
                                            let _ = tx.send(RpcMessage::ClientReply(reply));
                                        }
                                    }
                                }
                            }
                            ClientRequest::Delete { key } => {
                                match node.propose(Command::Delete { key }) {
                                    Ok((log_index, msgs)) => {
                                        dispatch(msgs, &rpc_client);
                                        if let Some(tx) = inbound.reply_tx {
                                            pending_clients.insert(log_index, tx);
                                        }
                                    }
                                    Err(_) => {
                                        let reply = ClientReply::NotLeader { leader_id: node.leader_id };
                                        if let Some(tx) = inbound.reply_tx {
                                            let _ = tx.send(RpcMessage::ClientReply(reply));
                                        }
                                    }
                                }
                            }
                        };
                    }
                    RpcMessage::ClientReply(_) => {
                        // Nodes don't receive client replies
                    }
                }

                // If state changed to leader, reset heartbeat timer immediately
                if old_state != node.state && node.state == raft_kv::raft::NodeState::Leader {
                    println!("[node {}] Became LEADER for term {}!", cli.id, node.current_term);
                    heartbeat_elapsed = heartbeat_timeout; // Trigger immediately on next tick
                }

                if reset_election {
                    election_elapsed = 0;
                }

                // Apply any newly committed entries to the state machine
                let applied = node.apply_committed(&mut store);
                if applied > 0 {
                    println!("[node {}] Applied {} entries to state machine. Commit index: {}", cli.id, applied, node.commit_index);
                }

                // Reply to any clients whose requests just committed
                let mut completed_indexes = Vec::new();
                for (&log_index, _) in pending_clients.iter() {
                    if log_index <= node.commit_index as u64 {
                        completed_indexes.push(log_index);
                    }
                }
                for idx in completed_indexes {
                    if let Some(tx) = pending_clients.remove(&idx) {
                        let _ = tx.send(RpcMessage::ClientReply(ClientReply::Success { value: None }));
                    }
                }

                // Stage 5: Log Compaction
                if node.log.len() > 20 {
                    let compact_to = node.commit_index as u64;
                    println!("[node {}] Log size exceeded 20, compacting up to index {}...", cli.id, compact_to);
                    node.compact_log(compact_to);
                }
            }
        }

        // -------------------------------------------------------------------
        // WAL Persistence (diffing)
        // -------------------------------------------------------------------
        if node.current_term != old_term || node.voted_for != old_voted_for {
            let _ = wal.append(&DiskRecord::State {
                current_term: node.current_term,
                voted_for: node.voted_for,
            });
        }
        
        if node.last_included_index != old_last_included_index {
            let _ = wal.append(&DiskRecord::Snapshot {
                last_included_index: node.last_included_index,
                last_included_term: node.last_included_term,
                store_data: store.dump(),
            });
        } else {
            // Find common prefix length
            let mut matching_len = 0;
            for (a, b) in old_log.iter().zip(node.log.iter()) {
                if a.term == b.term && a.command == b.command {
                    matching_len += 1;
                } else {
                    break;
                }
            }
            if matching_len < old_log.len() {
                let _ = wal.append(&DiskRecord::Truncate { physical_index: matching_len });
            }
            for i in matching_len..node.log.len() {
                let _ = wal.append(&DiskRecord::Append { entry: node.log[i].clone() });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn gen_election_timeout() -> u32 {
    // Raft randomizes election timeouts to prevent split votes
    rand::thread_rng().gen_range(150..300)
}

