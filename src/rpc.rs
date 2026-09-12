//! Stage 3: Async TCP networking between Raft nodes.
//!
//! This module handles the *transport* layer only — it knows nothing about
//! Raft logic.  Its job is to:
//!   1. Listen for incoming messages from peers.
//!   2. Deserialize them and forward them to the RaftNode via a channel.
//!   3. Provide a client that can fire-and-forget messages to peers.
//!
//! Wire format: 4-byte big-endian length prefix followed by a JSON body.
//! Simple, human-readable, and easy to debug with `nc` / Wireshark.
//!
//! Stage 4 (CLI) will extend this with client-facing request handling.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

use crate::raft::{
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};

// ---------------------------------------------------------------------------
// Wire message types
// ---------------------------------------------------------------------------

/// Wraps every RPC payload with sender/receiver IDs.
/// This lets the receiver know who to reply to without inspecting the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub from: u64,
    pub to: u64,
    pub payload: T,
}

/// All peer-to-peer RPC messages on the wire.
/// `#[serde(tag = "type")]` adds a `"type": "..."` discriminant field so
/// the receiver knows how to deserialize the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RpcMessage {
    RequestVote(Envelope<RequestVoteArgs>),
    RequestVoteReply(Envelope<RequestVoteReply>),
    AppendEntries(Envelope<AppendEntriesArgs>),
    AppendEntriesReply(Envelope<AppendEntriesReply>),
}

// ---------------------------------------------------------------------------
// Frame codec  (4-byte big-endian length prefix + JSON body)
// ---------------------------------------------------------------------------

/// Serialize `msg` and write it as a length-prefixed frame to `stream`.
pub async fn write_frame(stream: &mut TcpStream, msg: &RpcMessage) -> io::Result<()> {
    let body = serde_json::to_vec(msg)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len = body.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}

/// Read one length-prefixed frame from `stream` and deserialize it.
pub async fn read_frame(stream: &mut TcpStream) -> io::Result<RpcMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Guard against absurdly large frames (e.g. from a buggy or hostile peer)
    if len > 64 * 1024 * 1024 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }

    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;

    serde_json::from_slice(&body)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ---------------------------------------------------------------------------
// RpcServer — listens for peer connections and forwards messages to the node
// ---------------------------------------------------------------------------

/// Listens on `addr` and forwards every received `RpcMessage` to `inbox_tx`.
/// Spawn this as a dedicated tokio task:
///
/// ```ignore
/// let server = RpcServer { addr, node_id, inbox_tx };
/// tokio::spawn(server.run());
/// ```
pub struct RpcServer {
    pub addr: SocketAddr,
    pub node_id: u64,
    /// The node's inbound message channel.
    pub inbox_tx: mpsc::Sender<RpcMessage>,
}

impl RpcServer {
    pub async fn run(self) -> io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        println!(
            "[node {}] RPC server listening on {}",
            self.node_id, self.addr
        );

        loop {
            let (mut stream, peer_addr) = listener.accept().await?;
            let tx = self.inbox_tx.clone();
            let node_id = self.node_id;

            // One tokio task per open connection; handles the full lifetime of
            // that connection (could be reused across multiple messages).
            tokio::spawn(async move {
                loop {
                    match read_frame(&mut stream).await {
                        Ok(msg) => {
                            if tx.send(msg).await.is_err() {
                                // Receiver (node) has shut down
                                break;
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                            // Peer closed connection cleanly
                            break;
                        }
                        Err(e) => {
                            eprintln!(
                                "[node {}] error reading frame from {}: {}",
                                node_id, peer_addr, e
                            );
                            break;
                        }
                    }
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// RpcClient — sends messages to peers (fire-and-forget)
// ---------------------------------------------------------------------------

/// Sends `RpcMessage`s to peers over TCP.
///
/// This is fire-and-forget: if the peer is unreachable (crashed, partitioned),
/// the error is silently swallowed.  Raft is designed to handle this — the
/// leader will retry via the next heartbeat.
pub struct RpcClient {
    /// Map of node ID → TCP address.
    pub peer_addrs: HashMap<u64, SocketAddr>,
}

impl RpcClient {
    pub fn new(peer_addrs: HashMap<u64, SocketAddr>) -> Self {
        RpcClient { peer_addrs }
    }

    /// Send `msg` to peer `to`.  Returns immediately even if the peer is down.
    pub async fn send(&self, to: u64, msg: &RpcMessage) {
        let addr = match self.peer_addrs.get(&to) {
            Some(a) => *a,
            None => {
                eprintln!("rpc: unknown peer {}", to);
                return;
            }
        };

        // A new TCP connection per message is simple and correct for Raft;
        // a real production impl would use a persistent connection pool.
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                if let Err(e) = write_frame(&mut stream, msg).await {
                    eprintln!("rpc: failed to send to node {}: {}", to, e);
                }
            }
            Err(_) => {
                // Peer unreachable — normal in a distributed system.
                // Raft will recover via the next heartbeat / retry.
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: build RpcMessages from OutboundMsg (glue between raft.rs and rpc.rs)
// ---------------------------------------------------------------------------

use crate::raft::OutboundMsg;

/// Convert a Raft state machine outbound message into a wire `RpcMessage`.
pub fn to_wire(from: u64, msg: OutboundMsg) -> (u64, RpcMessage) {
    match msg {
        OutboundMsg::RequestVote { to, args } => (
            to,
            RpcMessage::RequestVote(Envelope { from, to, payload: args }),
        ),
        OutboundMsg::AppendEntries { to, args } => (
            to,
            RpcMessage::AppendEntries(Envelope { from, to, payload: args }),
        ),
    }
}
