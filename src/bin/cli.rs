//! Stage 4: Simple CLI client.
//!
//! Usage once implemented (suggested):
//!   raft-kv-cli set <key> <value>
//!   raft-kv-cli get <key>
//!   raft-kv-cli delete <key>
//!
//! TODO:
//! - [ ] Parse args (std::env::args or clap)
//! - [ ] Connect to any node in the cluster
//! - [ ] If that node isn't the leader, it should reply with a redirect
//!       to the actual leader (this is standard Raft client behavior) —
//!       follow the redirect automatically
//! - [ ] Print a clean result to stdout

fn main() {
    println!("raft-kv-cli: not implemented yet");
}
