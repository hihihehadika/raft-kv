//! Stage 3: Networking between nodes.
//!
//! Pick ONE approach and stick with it:
//!
//! Option A - Simple: raw TCP + your own tiny framed protocol
//!   (good if you also want the "wrote an HTTP/wire protocol from scratch"
//!   learning experience)
//!
//! Option B - Practical: tokio + tonic (gRPC)
//!   (faster to get working, closer to how real systems like TiKV do it)
//!
//! Either way you need at minimum two RPCs:
//! - RequestVote(term, candidate_id, last_log_index, last_log_term) -> (term, vote_granted)
//! - AppendEntries(term, leader_id, prev_log_index, prev_log_term, entries[], leader_commit)
//!     -> (term, success)
//!
//! TODO:
//! - [ ] Define the RPC request/response types (or .proto file if using tonic)
//! - [ ] Implement a server that listens and dispatches to RaftNode handlers
//! - [ ] Implement a client that can call RequestVote/AppendEntries on peers
//! - [ ] Handle timeouts / unreachable peers gracefully (a peer being down
//!       is normal, not an error to crash on)

pub struct RpcServer {
    // listener, etc.
}

pub struct RpcClient {
    // peer addresses
}
