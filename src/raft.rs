//! Stage 3: Raft consensus.
//!
//! This is the heart of the project. Read the Raft paper section 5 closely
//! before writing code here: https://raft.github.io/raft.pdf
//!
//! Core concepts to implement, roughly in this order:
//!
//! 1. Node states: Follower, Candidate, Leader
//! 2. Persistent state each node must track: currentTerm, votedFor, log[]
//! 3. Leader election:
//!    - randomized election timeout
//!    - on timeout, become Candidate, increment term, vote for self,
//!      send RequestVote RPCs to all peers
//!    - if majority votes received -> become Leader
//! 4. Log replication:
//!    - Leader appends client commands to its own log, then sends
//!      AppendEntries RPCs to followers
//!    - once a majority of nodes have replicated an entry, it's "committed"
//!      and safe to apply to the state machine (our KvStore)
//! 5. Safety rules (election restriction, log matching property) — these
//!    are what make Raft actually correct, don't skip them
//!
//! TODO:
//! - [ ] Define `NodeState` enum
//! - [ ] Define `LogEntry { term: u64, command: ... }`
//! - [ ] Implement `RequestVote` RPC handler
//! - [ ] Implement `AppendEntries` RPC handler (also used as heartbeat)
//! - [ ] Implement election timeout + heartbeat timers
//! - [ ] Wire committed entries into `store::KvStore`

#[derive(Debug, PartialEq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    pub id: u64,
    pub state: NodeState,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    // pub log: Vec<LogEntry>,
    // pub peers: Vec<PeerHandle>,
}

impl RaftNode {
    pub fn new(id: u64) -> Self {
        RaftNode {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
        }
    }

    pub fn handle_request_vote(&mut self /* , args: RequestVoteArgs */) {
        todo!("implement the RequestVote RPC logic from the paper, Fig. 2")
    }

    pub fn handle_append_entries(&mut self /* , args: AppendEntriesArgs */) {
        todo!("implement the AppendEntries RPC logic from the paper, Fig. 2")
    }
}
