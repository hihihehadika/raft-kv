use std::net::SocketAddr;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::net::TcpStream;

use raft_kv::rpc::{ClientReply, ClientRequest, RpcMessage, read_frame, write_frame};

#[derive(Parser)]
#[command(name = "raft-kv-cli")]
#[command(about = "CLI client for raft-kv", long_about = None)]
struct Cli {
    /// Comma-separated list of node addresses
    #[arg(short, long, default_value = "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003")]
    nodes: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Get a value by key
    Get { key: String },
    /// Set a key-value pair
    Set { key: String, value: String },
    /// Delete a key
    Delete { key: String },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    
    // Parse node addresses
    let nodes: Vec<SocketAddr> = cli
        .nodes
        .split(',')
        .map(|s| s.parse().expect("Invalid node address format"))
        .collect();

    if nodes.is_empty() {
        eprintln!("Error: No nodes provided.");
        return;
    }

    let request = match cli.command {
        Commands::Get { key } => ClientRequest::Get { key },
        Commands::Set { key, value } => ClientRequest::Set { key, value },
        Commands::Delete { key } => ClientRequest::Delete { key },
    };

    // Try sending the request. If we hit a follower, it will reply with NotLeader.
    // If a node is completely down, we try the next one.
    let mut current_target_idx = 0;
    
    for _ in 0..nodes.len() * 2 {
        let addr = nodes[current_target_idx];
        println!("> Sending request to node {} ({})", current_target_idx, addr);

        match send_request(addr, &request).await {
            Ok(ClientReply::Success { value }) => {
                match value {
                    Some(val) => println!("Success: {}", val),
                    None => println!("Success"),
                }
                return;
            }
            Ok(ClientReply::Error { message }) => {
                eprintln!("Error from node: {}", message);
                return;
            }
            Ok(ClientReply::NotLeader { leader_id }) => {
                match leader_id {
                    Some(id) if (id as usize) < nodes.len() => {
                        println!("  Node is a follower. Redirecting to leader: Node {}", id);
                        current_target_idx = id as usize;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    _ => {
                        println!("  Node is a follower but doesn't know the leader yet. Retrying...");
                        current_target_idx = (current_target_idx + 1) % nodes.len();
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
            }
            Err(e) => {
                println!("  Failed to connect/read from {}: {}", addr, e);
                // Node might be down, try the next one
                current_target_idx = (current_target_idx + 1) % nodes.len();
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    eprintln!("Error: Could not complete request. Cluster might be down or electing.");
}

async fn send_request(addr: SocketAddr, req: &ClientRequest) -> std::io::Result<ClientReply> {
    let mut stream = TcpStream::connect(addr).await?;
    let msg = RpcMessage::ClientRequest(req.clone());
    write_frame(&mut stream, &msg).await?;
    
    let reply = read_frame(&mut stream).await?;
    if let RpcMessage::ClientReply(r) = reply {
        Ok(r)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Expected ClientReply",
        ))
    }
}
