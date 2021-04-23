use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use log::info;
use math::round;
use rand::Rng;

use crate::constants::{MIN_QUORUM, NUM_SERVERS};
use crate::raft::types::{RaftNode, Role};
use crate::rpc::types::{RpcClient, VoteRequest};

pub(crate) fn background_task(raft_node: Arc<Mutex<RaftNode>>, rpc_client: &RpcClient) {
    loop {
        broadcast_heartbeat(Arc::clone(&raft_node), rpc_client);
        handle_timeout(Arc::clone(&raft_node), rpc_client);
    }
}

fn broadcast_heartbeat(raft_node: Arc<Mutex<RaftNode>>, rpc_client: &RpcClient) {
    let is_leader = raft_node.lock().unwrap().role == Role::Leader;

    if is_leader {
        let current_term = raft_node.lock().unwrap().state.current_term;
        let id = raft_node.lock().unwrap().id;

        rpc_client.broadcast_heartbeat(current_term, id);
        // A touch of randomness, so that we can get the chance
        // to have other leader elections.
        let mut rng = rand::thread_rng();
        thread::sleep(Duration::new(rng.gen_range(1..7), 0));
    }
}

fn handle_timeout(raft_node: Arc<Mutex<RaftNode>>, rpc_client: &RpcClient) {
    let node_id = raft_node.lock().unwrap().id;
    let has_timed_out = raft_node.lock().unwrap().has_timed_out();
    let role = raft_node.lock().unwrap().role;

    if has_timed_out && role != Role::Leader {
        info!("Server {} has timed out and has started election.", node_id);
        let term = raft_node.lock().unwrap().prepare_for_election();
        let candidate_id = raft_node.lock().unwrap().id;
        // TODO
        let last_log_index = 0;
        let last_log_term = 0;

        // run the election
        let mut won_election = false;
        {
            let votes = rpc_client.request_votes(term, candidate_id, last_log_index, last_log_term);
            let votes_for = votes.iter().filter(|r| r.vote_granted).count();
            won_election = (votes_for + 1) > *MIN_QUORUM
                && Role::Candidate == raft_node.lock().unwrap().role
                && !raft_node.lock().unwrap().has_timed_out();
        }

        if won_election {
            raft_node.lock().unwrap().become_leader();
            let current_term = raft_node.lock().unwrap().state.current_term;
            let node_id = raft_node.lock().unwrap().id;
            rpc_client.broadcast_heartbeat(current_term, node_id);
        }
    }
}
