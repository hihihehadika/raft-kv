//! Stage 3: Raft consensus state machine.
//!
//! This module is a *pure state machine* — it does no I/O whatsoever.
//! It receives messages, updates its internal state, and returns a list
//! of messages to send out.  All networking, timers, and disk I/O live
//! in other modules (`rpc.rs`, `wal.rs`, `main.rs`).
//!
//! The implementation follows the Raft paper Figure 2 closely.
//! Read it first: https://raft.github.io/raft.pdf  (especially §5)

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::store::KvStore;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// A command that changes the KV state machine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub enum Command {
    Set { key: String, value: String },
    Delete { key: String },
}

/// One entry in the replicated log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    /// Term when this entry was created by the leader.
    pub term: u64,
    pub command: Command,
}

/// Raft node lifecycle (Figure 4 of the paper).
#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

// ---------------------------------------------------------------------------
// RPC argument / reply types  (Figure 2 of the paper)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteArgs {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting the vote.
    pub candidate_id: u64,
    /// Index of the candidate's last log entry.
    pub last_log_index: u64,
    /// Term of the candidate's last log entry.
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteReply {
    /// Current term (so the candidate can update itself).
    pub term: u64,
    /// True if the follower voted for this candidate.
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesArgs {
    /// Leader's term.
    pub term: u64,
    /// Leader's id (so followers can redirect clients).
    pub leader_id: u64,
    /// Index of the log entry immediately preceding the new ones.
    pub prev_log_index: u64,
    /// Term of the entry at `prev_log_index`.
    pub prev_log_term: u64,
    /// New entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    /// Current term (so the leader can update itself).
    pub term: u64,
    /// True if the follower accepted the entries.
    pub success: bool,
    /// Highest index the follower has now matched.
    /// Piggybacked so the leader can update matchIndex in one round-trip.
    pub match_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    pub term: u64,
}

/// Messages the state machine wants to send to peers.
/// The caller (main / rpc layer) is responsible for actual delivery.
#[derive(Debug, Clone)]
pub enum OutboundMsg {
    RequestVote { to: u64, args: RequestVoteArgs },
    AppendEntries { to: u64, args: AppendEntriesArgs },
    InstallSnapshot { to: u64, args: InstallSnapshotArgs },
}

// ---------------------------------------------------------------------------
// The Raft node state machine
// ---------------------------------------------------------------------------

pub struct RaftNode {
    // -----------------------------------------------------------------------
    // Persistent state (must survive crashes in a real impl — stored via WAL)
    // -----------------------------------------------------------------------
    pub id: u64,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    
    // Snapshotting state
    pub last_included_index: u64,
    pub last_included_term: u64,
    
    /// log[0] is always a sentinel entry representing last_included_index.
    pub log: Vec<LogEntry>,

    // -----------------------------------------------------------------------
    // Volatile state (all nodes)
    // -----------------------------------------------------------------------
    pub state: NodeState,
    /// Index of the highest log entry known to be committed.
    pub commit_index: usize,
    /// Index of the highest log entry applied to the state machine.
    pub last_applied: usize,

    // -----------------------------------------------------------------------
    // Volatile leader state (re-initialized after each election)
    // -----------------------------------------------------------------------
    /// For each peer: next log index to send to that peer.
    pub next_index: HashMap<u64, usize>,
    /// For each peer: highest log index known to be replicated on that peer.
    pub match_index: HashMap<u64, usize>,

    // -----------------------------------------------------------------------
    // Cluster metadata
    // -----------------------------------------------------------------------
    pub peers: Vec<u64>,
    votes_received: HashSet<u64>,
    /// Cached leader id (for client redirect).
    pub leader_id: Option<u64>,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        RaftNode {
            id,
            current_term: 0,
            voted_for: None,
            last_included_index: 0,
            last_included_term: 0,
            // Index 0 is a sentinel representing last_included_index/term
            log: vec![LogEntry {
                term: 0,
                command: Command::Set { key: String::new(), value: String::new() },
            }],
            state: NodeState::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            votes_received: HashSet::new(),
            leader_id: None,
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn physical(&self, logical_index: u64) -> Option<usize> {
        if logical_index < self.last_included_index {
            None
        } else {
            Some((logical_index - self.last_included_index) as usize)
        }
    }

    fn logical(&self, physical_index: usize) -> u64 {
        self.last_included_index + physical_index as u64
    }

    fn last_log_index(&self) -> u64 {
        self.logical(self.log.len() - 1)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// §5.4.1 — Is the candidate's log at least as up-to-date as ours?
    /// Compare last-log-term first; if equal, compare index.
    fn other_log_up_to_date(&self, other_last_index: u64, other_last_term: u64) -> bool {
        if other_last_term != self.last_log_term() {
            other_last_term > self.last_log_term()
        } else {
            other_last_index >= self.last_log_index()
        }
    }

    fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// Revert to follower at `term`. Clears vote and candidate bookkeeping.
    fn become_follower(&mut self, term: u64) {
        self.state = NodeState::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
    }

    // -----------------------------------------------------------------------
    // Election timeout fired — start a new election  (§5.2)
    // -----------------------------------------------------------------------
    pub fn start_election(&mut self) -> Vec<OutboundMsg> {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id); // vote for self

        // If we're already a majority of 1, win immediately
        if self.votes_received.len() >= self.majority() {
            return self.become_leader();
        }

        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        };

        self.peers
            .iter()
            .map(|&peer| OutboundMsg::RequestVote { to: peer, args: args.clone() })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Handle incoming RequestVote RPC  (Figure 2)
    // -----------------------------------------------------------------------
    pub fn handle_request_vote(
        &mut self,
        args: RequestVoteArgs,
    ) -> (RequestVoteReply, Vec<OutboundMsg>) {
        // If we see a higher term, immediately revert to follower
        if args.term > self.current_term {
            self.become_follower(args.term);
        }

        // Grant vote only when ALL conditions hold:
        //   1. candidate's term >= ours
        //   2. we haven't voted for someone else this term
        //   3. candidate's log is at least as up-to-date (§5.4 election safety)
        let vote_granted = args.term >= self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id))
            && self.other_log_up_to_date(args.last_log_index, args.last_log_term);

        if vote_granted {
            self.voted_for = Some(args.candidate_id);
        }

        (RequestVoteReply { term: self.current_term, vote_granted }, vec![])
    }

    // -----------------------------------------------------------------------
    // Handle reply to a RequestVote we sent  (§5.2)
    // -----------------------------------------------------------------------
    pub fn handle_request_vote_reply(
        &mut self,
        from: u64,
        reply: RequestVoteReply,
    ) -> Vec<OutboundMsg> {
        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return vec![];
        }
        // Ignore stale replies or replies when we're no longer a candidate
        if self.state != NodeState::Candidate || reply.term != self.current_term {
            return vec![];
        }
        if !reply.vote_granted {
            return vec![];
        }

        self.votes_received.insert(from);
        if self.votes_received.len() >= self.majority() {
            return self.become_leader();
        }
        vec![]
    }

    // -----------------------------------------------------------------------
    // Win the election — transition to Leader  (§5.2)
    // -----------------------------------------------------------------------
    fn become_leader(&mut self) -> Vec<OutboundMsg> {
        self.state = NodeState::Leader;
        self.leader_id = Some(self.id);

        // Re-initialize nextIndex[] and matchIndex[] for all peers
        let next = self.last_log_index() + 1;
        for &peer in &self.peers {
            self.next_index.insert(peer, next as usize);
            self.match_index.insert(peer, 0);
        }

        // §5.2: send initial heartbeat immediately to assert leadership
        self.broadcast_append_entries()
    }

    // -----------------------------------------------------------------------
    // Handle incoming AppendEntries RPC  (Figure 2)
    // -----------------------------------------------------------------------
    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Vec<OutboundMsg>) {
        let reject = |term| {
            (AppendEntriesReply { term, success: false, match_index: 0 }, vec![])
        };

        // Rule 1: reject if leader's term is stale
        if args.term < self.current_term {
            return reject(self.current_term);
        }

        // Valid leader — reset our state
        if args.term > self.current_term || self.state != NodeState::Follower {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);

        let prev_logical = args.prev_log_index;
        
        // If prev_log_index is behind our snapshot, we can't safely append it directly.
        // Usually, InstallSnapshot handles this. For AppendEntries, we just reject if it's too old.
        if prev_logical < self.last_included_index {
            return reject(self.current_term);
        }

        let prev_physical = self.physical(prev_logical).unwrap();

        // Rule 2: reject if our log is missing prevLogIndex, or the term there doesn't match
        if prev_physical >= self.log.len() || self.log[prev_physical].term != args.prev_log_term {
            return reject(self.current_term);
        }

        // Rules 3 & 4: merge entries into our log
        for (offset, new_entry) in args.entries.iter().enumerate() {
            let physical_idx = prev_physical + 1 + offset;
            if physical_idx < self.log.len() {
                if self.log[physical_idx].term != new_entry.term {
                    // Conflict: truncate our log and append from here
                    self.log.truncate(physical_idx);
                    self.log.push(new_entry.clone());
                }
            } else {
                self.log.push(new_entry.clone());
            }
        }

        // Rule 5: advance commitIndex
        if args.leader_commit > self.commit_index as u64 {
            self.commit_index = (args.leader_commit as usize).min(self.logical(self.log.len() - 1) as usize);
        }

        let match_index = prev_logical + args.entries.len() as u64;
        (
            AppendEntriesReply { term: self.current_term, success: true, match_index },
            vec![],
        )
    }

    // -----------------------------------------------------------------------
    // Handle reply to AppendEntries we sent  (§5.3)
    // -----------------------------------------------------------------------
    pub fn handle_append_entries_reply(
        &mut self,
        from: u64,
        reply: AppendEntriesReply,
    ) -> Vec<OutboundMsg> {
        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return vec![];
        }
        if self.state != NodeState::Leader {
            return vec![];
        }

        if reply.success {
            let mi = reply.match_index as usize;
            self.match_index.insert(from, mi);
            self.next_index.insert(from, mi + 1);
            self.try_advance_commit_index();
            vec![]
        } else {
            // Follower rejected — back up nextIndex and retry
            let ni = self.next_index.entry(from).or_insert(1);
            if *ni > 1 { *ni -= 1; }
            let next = *self.next_index.get(&from).unwrap_or(&1);
            self.build_append_entries_for(from, next)
        }
    }

    // -----------------------------------------------------------------------
    // Handle incoming InstallSnapshot RPC
    // -----------------------------------------------------------------------
    pub fn handle_install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, Vec<OutboundMsg>) {
        if args.term < self.current_term {
            return (InstallSnapshotReply { term: self.current_term }, vec![]);
        }

        if args.term > self.current_term || self.state != NodeState::Follower {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);

        if args.last_included_index <= self.last_included_index {
            return (InstallSnapshotReply { term: self.current_term }, vec![]);
        }

        // If we have existing log entries that overlap with the snapshot, we could retain them.
        // But for simplicity (and as allowed by Raft), we just clear the log and adopt the snapshot.
        self.last_included_index = args.last_included_index;
        self.last_included_term = args.last_included_term;
        self.log.clear();
        self.log.push(LogEntry {
            term: self.last_included_term,
            command: Command::Set { key: String::new(), value: String::new() }, // sentinel
        });

        self.commit_index = self.last_included_index as usize;
        // The store application logic will jump `last_applied` forward automatically.

        (InstallSnapshotReply { term: self.current_term }, vec![])
    }

    // -----------------------------------------------------------------------
    // Handle reply to InstallSnapshot we sent
    // -----------------------------------------------------------------------
    pub fn handle_install_snapshot_reply(
        &mut self,
        from: u64,
        reply: InstallSnapshotReply,
    ) -> Vec<OutboundMsg> {
        if reply.term > self.current_term {
            self.become_follower(reply.term);
            return vec![];
        }
        if self.state != NodeState::Leader {
            return vec![];
        }

        // The follower has successfully installed the snapshot!
        // We can update its matchIndex and nextIndex to the snapshot's end.
        self.match_index.insert(from, self.last_included_index as usize);
        self.next_index.insert(from, (self.last_included_index + 1) as usize);

        self.try_advance_commit_index();
        vec![]
    }

    // -----------------------------------------------------------------------
    // Log Compaction / Snapshotting (Stage 5)
    // -----------------------------------------------------------------------
    pub fn compact_log(&mut self, up_to_index: u64) {
        if up_to_index <= self.last_included_index || up_to_index > (self.commit_index as u64) {
            return; // invalid index to compact
        }

        let physical_idx = self.physical(up_to_index).unwrap();
        let term = self.log[physical_idx].term;

        // Retain entries from `physical_idx` onwards
        let retained = self.log[physical_idx..].to_vec();
        
        self.log = retained;
        // Overwrite the first retained element to be the sentinel
        self.log[0] = LogEntry {
            term,
            command: Command::Set { key: String::new(), value: String::new() },
        };

        self.last_included_index = up_to_index;
        self.last_included_term = term;
    }

    // -----------------------------------------------------------------------
    // Leader: try to advance commitIndex  (§5.3 — leader commit rule)
    //
    // Find the highest N such that:
    //   • N > commitIndex
    //   • log[N].term == currentTerm   ← CRITICAL safety rule (§5.4.2)
    //   • a majority of nodes have matchIndex >= N
    // -----------------------------------------------------------------------
    fn try_advance_commit_index(&mut self) {
        let last_logical = self.last_log_index();
        let start = (self.commit_index as u64 + 1).max(self.last_included_index + 1);
        
        for n in (start..=last_logical).rev() {
            let physical_idx = self.physical(n).unwrap();
            if self.log[physical_idx].term != self.current_term {
                continue; // never commit entries from previous terms directly
            }
            let replicated = self.match_index.values().filter(|&&mi| mi >= n as usize).count() + 1; // +1 = self
            if replicated >= self.majority() {
                self.commit_index = n as usize;
                break;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Leader: accept a client command and start replication
    // -----------------------------------------------------------------------
    pub fn propose(&mut self, command: Command) -> Result<(u64, Vec<OutboundMsg>), &'static str> {
        if self.state != NodeState::Leader {
            return Err("not leader");
        }
        self.log.push(LogEntry { term: self.current_term, command });
        let logical_index = self.last_log_index();
        let msgs = self.broadcast_append_entries();
        self.try_advance_commit_index(); // Advance immediately if we are a 1-node cluster
        Ok((logical_index, msgs))
    }

    // -----------------------------------------------------------------------
    // Leader: heartbeat tick (called periodically by the timer)
    // -----------------------------------------------------------------------
    pub fn send_heartbeats(&self) -> Vec<OutboundMsg> {
        if self.state != NodeState::Leader {
            return vec![];
        }
        self.broadcast_append_entries()
    }

    // -----------------------------------------------------------------------
    // Apply all newly committed entries to the KV state machine.
    // Returns how many entries were applied.
    // -----------------------------------------------------------------------
    pub fn apply_committed(&mut self, store: &mut KvStore) -> usize {
        // If a snapshot advanced our state past what we applied, jump forward.
        if (self.last_applied as u64) < self.last_included_index {
            self.last_applied = self.last_included_index as usize;
        }

        let mut count = 0;
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let physical_idx = self.physical(self.last_applied as u64).unwrap();
            match self.log[physical_idx].command.clone() {
                Command::Set { key, value } => store.set(key, value),
                Command::Delete { key } => { store.delete(&key); }
            }
            count += 1;
        }
        count
    }

    // -----------------------------------------------------------------------
    // Internal helpers for building outbound AppendEntries messages
    // -----------------------------------------------------------------------

    /// Build AppendEntries for all peers (used for heartbeats and after election).
    fn broadcast_append_entries(&self) -> Vec<OutboundMsg> {
        self.peers
            .iter()
            .flat_map(|&peer| {
                let next = *self.next_index.get(&peer).unwrap_or(&1);
                self.build_append_entries_for(peer, next)
            })
            .collect()
    }

    fn build_append_entries_for(&self, peer: u64, next: usize) -> Vec<OutboundMsg> {
        let next_logical = next as u64;

        if next_logical <= self.last_included_index {
            // Peer is too far behind, must send InstallSnapshot.
            // In this demo, we assume the snapshot data is already handled or we send empty data
            // (since the real implementation would pass the serialized store).
            // To make this fully complete, we'd need access to KvStore here, but this is sufficient
            // for the Raft logic part (the actual data transfer could be out-of-band).
            return vec![OutboundMsg::InstallSnapshot {
                to: peer,
                args: InstallSnapshotArgs {
                    term: self.current_term,
                    leader_id: self.id,
                    last_included_index: self.last_included_index,
                    last_included_term: self.last_included_term,
                    data: vec![], // placeholder
                },
            }];
        }

        let prev_logical = next_logical.saturating_sub(1);
        let prev_physical = self.physical(prev_logical).unwrap();
        
        let prev_log_term = self.log[prev_physical].term;
        let next_physical = self.physical(next_logical).unwrap_or(self.log.len());
        
        let entries = if next_physical < self.log.len() {
            self.log[next_physical..].to_vec()
        } else {
            vec![]
        };

        vec![OutboundMsg::AppendEntries {
            to: peer,
            args: AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_logical,
                prev_log_term,
                entries,
                leader_commit: self.commit_index as u64,
            },
        }]
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::KvStore;

    fn cluster3() -> [RaftNode; 3] {
        [
            RaftNode::new(0, vec![1, 2]),
            RaftNode::new(1, vec![0, 2]),
            RaftNode::new(2, vec![0, 1]),
        ]
    }

    /// Drive all outbound messages through the cluster until the queue drains.
    /// This simulates instant, reliable message delivery — perfect for unit tests.
    fn flush(nodes: &mut [RaftNode; 3], initial: Vec<OutboundMsg>) {
        let mut queue = initial;
        while !queue.is_empty() {
            let mut next: Vec<OutboundMsg> = vec![];
            for msg in queue.drain(..) {
                match msg {
                    OutboundMsg::RequestVote { to, args } => {
                        let from = args.candidate_id;
                        let (reply, out) = nodes[to as usize].handle_request_vote(args);
                        next.extend(out);
                        next.extend(nodes[from as usize].handle_request_vote_reply(to, reply));
                    }
                    OutboundMsg::AppendEntries { to, args } => {
                        let leader = args.leader_id;
                        let (reply, out) = nodes[to as usize].handle_append_entries(args);
                        next.extend(out);
                        next.extend(nodes[leader as usize].handle_append_entries_reply(to, reply));
                    }
                    OutboundMsg::InstallSnapshot { to, args } => {
                        let leader = args.leader_id;
                        let (reply, out) = nodes[to as usize].handle_install_snapshot(args);
                        next.extend(out);
                        next.extend(nodes[leader as usize].handle_install_snapshot_reply(to, reply));
                    }
                }
            }
            queue = next;
        }
    }

    // -----------------------------------------------------------------------
    // Election tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_single_node_wins_election_immediately() {
        // With no peers, a node already has majority (1 of 1)
        let mut node = RaftNode::new(0, vec![]);
        let msgs = node.start_election();
        assert!(msgs.is_empty(), "no peers → no messages to send");
        assert_eq!(node.state, NodeState::Leader);
        assert_eq!(node.current_term, 1);
    }

    #[test]
    fn test_three_node_election_produces_one_leader() {
        let mut nodes = cluster3();
        // Node 0's election timer fires first
        let msgs = nodes[0].start_election();
        flush(&mut nodes, msgs);

        assert_eq!(nodes[0].state, NodeState::Leader);
        assert_eq!(nodes[0].current_term, 1);
        // Followers saw the heartbeat and adopted the leader's term
        assert_eq!(nodes[1].current_term, 1);
        assert_eq!(nodes[2].current_term, 1);
    }

    #[test]
    fn test_vote_denied_when_candidate_term_is_stale() {
        let mut follower = RaftNode::new(1, vec![0, 2]);
        follower.current_term = 5; // already on term 5

        let (reply, _) = follower.handle_request_vote(RequestVoteArgs {
            term: 3, // stale!
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(!reply.vote_granted);
        assert_eq!(reply.term, 5);
    }

    #[test]
    fn test_vote_denied_when_candidate_log_is_behind() {
        let mut follower = RaftNode::new(1, vec![0, 2]);
        // Give follower a log entry at term 2
        follower.log.push(LogEntry {
            term: 2,
            command: Command::Set { key: "x".into(), value: "1".into() },
        });
        follower.current_term = 2;

        // Candidate only has the sentinel (last_log_term=0, last_log_index=0)
        let (reply, _) = follower.handle_request_vote(RequestVoteArgs {
            term: 2,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(!reply.vote_granted);
    }

    #[test]
    fn test_higher_term_in_ae_converts_candidate_to_follower() {
        let mut nodes = cluster3();
        // Node 0 starts an election, becomes candidate in term 1
        let _ = nodes[0].start_election();
        assert_eq!(nodes[0].state, NodeState::Candidate);

        // Node 0 receives AppendEntries from a valid leader in term 2
        let (reply, _) = nodes[0].handle_append_entries(AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });

        assert!(reply.success);
        assert_eq!(nodes[0].state, NodeState::Follower);
        assert_eq!(nodes[0].current_term, 2);
        assert_eq!(nodes[0].leader_id, Some(1));
    }

    // -----------------------------------------------------------------------
    // Log replication & commit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_leader_replicates_and_commits_to_majority() {
        let mut nodes = cluster3();
        let mut stores = [KvStore::new(), KvStore::new(), KvStore::new()];

        // Elect node 0
        let msgs = nodes[0].start_election();
        flush(&mut nodes, msgs);
        assert_eq!(nodes[0].state, NodeState::Leader);

        // Propose a write
        let (_, msgs) = nodes[0]
            .propose(Command::Set { key: "hello".into(), value: "raft".into() })
            .unwrap();
        flush(&mut nodes, msgs);

        // Leader should have committed index 1
        assert_eq!(nodes[0].commit_index, 1);

        // Apply to state machine
        nodes[0].apply_committed(&mut stores[0]);
        assert_eq!(stores[0].get("hello"), Some(&"raft".to_string()));
    }

    #[test]
    fn test_followers_replicate_log_entries() {
        let mut nodes = cluster3();

        let msgs = nodes[0].start_election();
        flush(&mut nodes, msgs);

        let (_, msgs) = nodes[0]
            .propose(Command::Set { key: "k".into(), value: "v".into() })
            .unwrap();
        flush(&mut nodes, msgs);

        // All nodes should have the entry in their log
        for node in &nodes {
            assert!(node.log.len() > 1, "node {} log is empty", node.id);
            let entry = &node.log[1];
            assert_eq!(
                entry.command,
                Command::Set { key: "k".into(), value: "v".into() }
            );
        }
    }

    #[test]
    fn test_append_entries_rejected_when_term_is_stale() {
        let mut follower = RaftNode::new(1, vec![0, 2]);
        follower.current_term = 4;

        let (reply, _) = follower.handle_append_entries(AppendEntriesArgs {
            term: 2, // stale leader
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        });

        assert!(!reply.success);
        assert_eq!(reply.term, 4);
    }

    #[test]
    fn test_multiple_proposals_all_committed() {
        let mut nodes = cluster3();
        let mut store = KvStore::new();

        let msgs = nodes[0].start_election();
        flush(&mut nodes, msgs);

        let writes = vec![
            Command::Set { key: "a".into(), value: "1".into() },
            Command::Set { key: "b".into(), value: "2".into() },
            Command::Delete { key: "a".into() },
        ];
        for cmd in writes {
            let (_, msgs) = nodes[0].propose(cmd).unwrap();
            flush(&mut nodes, msgs);
        }

        // Leader's log should have sentinel + 3 entries
        assert_eq!(nodes[0].log.len(), 4);
        assert_eq!(nodes[0].commit_index, 3);

        nodes[0].apply_committed(&mut store);
        assert_eq!(store.get("a"), None);   // was deleted
        assert_eq!(store.get("b"), Some(&"2".to_string()));
    }

    #[test]
    fn test_non_leader_propose_returns_error() {
        let mut node = RaftNode::new(0, vec![1, 2]);
        // node is Follower by default
        let result = node.propose(Command::Set { key: "x".into(), value: "y".into() });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "not leader");
    }

    #[test]
    fn test_new_election_after_leader_drops() {
        let mut nodes = cluster3();

        // First election: node 0 becomes leader
        let msgs = nodes[0].start_election();
        flush(&mut nodes, msgs);
        assert_eq!(nodes[0].state, NodeState::Leader);

        // Simulate node 0 crashing: node 1 starts a new election
        let msgs = nodes[1].start_election();
        flush(&mut nodes, msgs);

        // Node 1 should win (node 0 sees higher term and becomes follower)
        assert_eq!(nodes[1].state, NodeState::Leader);
        assert_eq!(nodes[1].current_term, 2);
        assert_eq!(nodes[0].state, NodeState::Follower); // demoted
    }
}
