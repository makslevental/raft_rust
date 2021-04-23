use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use log::info;
use rand::prelude::ThreadRng;
use rand::Rng;

use crate::constants::{MAX_TIMEOUT, MIN_TIMEOUT, NUM_SERVERS};
use crate::raft::types::{LogIndex, NodeId, PersistentState, RaftNode, Role, Term};
use crate::rpc::types::{
    AppendEntriesRequest, AppendEntriesResponse, Heartbeat, HeartbeatResponse, Peer, RpcMessage,
    VoteRequest, VoteRequestResponse,
};

impl RaftNode {
    pub fn new(id: NodeId) -> Self {
        RaftNode {
            state: PersistentState {
                current_term: 0,
                voted_for: None,
                log: vec![],
            },
            commit_index: vec![],
            last_applied: vec![],
            next_index: [0; NUM_SERVERS],
            match_index: [0; NUM_SERVERS],
            id,
            current_leader: None,
            role: Role::Follower,
            next_timeout: None,
        }
    }

    pub fn refresh_timeout(self: &mut Self) {
        let mut rng = rand::thread_rng();
        self.next_timeout = Option::from(
            Instant::now()
                + Duration::new(
                    rng.gen_range(MIN_TIMEOUT..MAX_TIMEOUT).try_into().unwrap(),
                    0,
                ),
        );
    }

    pub fn become_leader(self: &mut Self) {
        if self.role == Role::Candidate {
            info!(
                "Server {} has won the election! The new term is: {}",
                self.id, self.state.current_term
            );
            self.role = Role::Leader;
            self.next_timeout = None;
        }
    }

    pub fn has_timed_out(self: &mut Self) -> bool {
        match self.next_timeout {
            Some(t) => Instant::now() > t,
            None => false,
        }
    }

    pub fn handle_vote_request(self: &mut Self, term: Term, candidate_id: NodeId) -> (Term, bool) {
        // match self.state.voted_for {
        //     Some(_) => (term, false),
        //     None => {
        //         if term > self.state.current_term {
        //             self.state.voted_for = Some(candidate_id);
        //             (term, true)
        //         } else {
        //             (term, false)
        //         }
        //     }
        // }
        todo!()
    }

    pub fn handle_heartbeat(self: &mut Self, term: Term, node_id: NodeId) -> (bool, NodeId) {
        info!(
            "Server {} with term {}, received heartbeat from {} with term {}",
            self.id, self.state.current_term, node_id, term
        );

        todo!()
        // self.refresh_timeout();
        //
        // if term > self.state.current_term {
        //     info!(
        //         "Server {} becoming follower. The new leader is: {}",
        //         self.id, node_id
        //     );
        //
        //     (term, true)
        // } else {
        //     (self.state.current_term, false)
        // }
    }

    pub fn prepare_for_election(self: &mut Self) -> Term {
        self.role = Role::Candidate;
        self.state.current_term += 1;
        self.refresh_timeout();
        self.state.voted_for = Some(self.id);
        self.state.current_term
    }
}
