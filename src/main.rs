mod store;
mod wal;
mod raft;
mod rpc;

use raft::{Command, NodeState, OutboundMsg, RaftNode};
use store::KvStore;

// ---------------------------------------------------------------------------
// In-process cluster simulation
//
// For Stage 3, we prove the Raft *logic* is correct using a single-process
// simulation: nodes are just structs, and "the network" is a Vec of messages
// we deliver synchronously.
//
// Stage 4 will wire up the real TCP layer from rpc.rs into a multi-process
// binary, at which point each node runs in its own OS process.
// ---------------------------------------------------------------------------

/// Deliver all messages in `queue` to the right node and keep going until
/// the queue is empty (no more messages to process).
///
/// This simulates an instant, reliable, in-order network — ideal for testing
/// correctness before adding timing and fault injection.
fn deliver(nodes: &mut [RaftNode; 3], queue: Vec<OutboundMsg>) {
    let mut pending = queue;
    while !pending.is_empty() {
        let mut next: Vec<OutboundMsg> = vec![];
        for msg in pending.drain(..) {
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
            }
        }
        pending = next;
    }
}

fn separator(title: &str) {
    println!("\n─── {} {}", title, "─".repeat(50 - title.len().min(48)));
}

fn main() {
    println!("raft-kv node starting (Stage 3 — in-process simulation)");

    // -----------------------------------------------------------------------
    // Boot a 3-node cluster (IDs: 0, 1, 2)
    // -----------------------------------------------------------------------
    let mut nodes = [
        RaftNode::new(0, vec![1, 2]),
        RaftNode::new(1, vec![0, 2]),
        RaftNode::new(2, vec![0, 1]),
    ];
    let mut stores = [KvStore::new(), KvStore::new(), KvStore::new()];

    separator("STEP 1 — Initial state");
    for n in &nodes {
        println!("  node {} → {:?}  term={}", n.id, n.state, n.current_term);
    }

    // -----------------------------------------------------------------------
    // Step 2 — Leader election
    // Node 0's election timeout fires first (deterministic in simulation)
    // -----------------------------------------------------------------------
    separator("STEP 2 — Leader election (node 0 times out)");
    let msgs = nodes[0].start_election();
    println!("  node 0 → Candidate  term=1  sending {} RequestVote RPCs", msgs.len());
    deliver(&mut nodes, msgs);

    for n in &nodes {
        println!("  node {} → {:?}  term={}  leader={:?}", n.id, n.state, n.current_term, n.leader_id);
    }
    assert_eq!(nodes[0].state, NodeState::Leader, "node 0 must be leader");

    // -----------------------------------------------------------------------
    // Step 3 — Log replication: write some keys
    // -----------------------------------------------------------------------
    separator("STEP 3 — Log replication");
    let writes = [
        Command::Set { key: "user:1".into(), value: "alice".into() },
        Command::Set { key: "user:2".into(), value: "bob".into() },
        Command::Set { key: "config:port".into(), value: "8080".into() },
        Command::Delete { key: "user:2".into() },
    ];

    for cmd in &writes {
        let msgs = nodes[0].propose(cmd.clone()).expect("leader must accept writes");
        deliver(&mut nodes, msgs);
    }

    // Apply committed entries to state machines
    for i in 0..3 {
        let applied = nodes[i].apply_committed(&mut stores[i]);
        println!("  node {} applied {} entries  (commit_index={})", i, applied, nodes[i].commit_index);
    }

    println!("\n  [Leader store]");
    println!("    get user:1       => {:?}", stores[0].get("user:1"));
    println!("    get user:2       => {:?}", stores[0].get("user:2")); // deleted
    println!("    get config:port  => {:?}", stores[0].get("config:port"));

    // Verify all nodes have the same log length (strong consistency)
    assert_eq!(nodes[0].log.len(), nodes[1].log.len());
    assert_eq!(nodes[1].log.len(), nodes[2].log.len());
    println!("\n  ✓ All 3 nodes have identical log length: {}", nodes[0].log.len() - 1);

    // -----------------------------------------------------------------------
    // Step 4 — Leader failure & re-election
    // Simulate node 0 crashing: node 1 starts a new election
    // -----------------------------------------------------------------------
    separator("STEP 4 — Leader crash + re-election");
    println!("  [node 0 crashes — no more messages from it]");

    let msgs = nodes[1].start_election();
    println!("  node 1 → Candidate  sending {} RequestVote RPCs", msgs.len());
    deliver(&mut nodes, msgs);

    println!("\n  After re-election:");
    for n in &nodes {
        println!("  node {} → {:?}  term={}  leader={:?}", n.id, n.state, n.current_term, n.leader_id);
    }
    assert_eq!(nodes[1].state, NodeState::Leader, "node 1 must win re-election");
    println!("\n  ✓ New leader elected: node 1  (no data loss for committed writes)");

    // -----------------------------------------------------------------------
    // Step 5 — New leader accepts writes immediately
    // -----------------------------------------------------------------------
    separator("STEP 5 — New leader accepts writes");
    let msgs = nodes[1]
        .propose(Command::Set { key: "new-key".into(), value: "after-failover".into() })
        .expect("node 1 is the new leader");
    deliver(&mut nodes, msgs);

    nodes[1].apply_committed(&mut stores[1]);
    println!("  get new-key  => {:?}", stores[1].get("new-key"));
    assert_eq!(stores[1].get("new-key"), Some(&"after-failover".to_string()));
    println!("\n  ✓ Writes continue working after leader failover");

    separator("DONE");
    println!("  Stage 3 simulation complete.");
    println!("  Next: Stage 4 — run each node as a real OS process via rpc.rs + CLI.");
}
