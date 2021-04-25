use std::cmp::min;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use log::info;
use rand::Rng;

use crate::constants::{MAX_TIMEOUT, MESSAGE_LENGTH, MIN_QUORUM, MIN_TIMEOUT};
use crate::raft::types::{
    LogEntry, LogIndex, LogTerm, Maintenance, Message, NodeId, Peer, PersistentState, RaftNode,
    Role, VolatileState,
};

impl RaftNode {
    pub fn new(id: NodeId, address: SocketAddrV4, peers: &Vec<Peer>) -> Self {
        RaftNode {
            persistent_state: PersistentState {
                current_term: 0,
                voted_for: None,
                log: vec![],
            },
            volatile_state: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            leader_state: None,
            maintenance: Maintenance {
                current_leader: None,
                role: Role::Follower,
                next_timeout: None,
                peer_nodes: peers
                    .iter()
                    .map(|p| (p.id.to_string(), TcpStream::connect(&p.address).unwrap()))
                    .collect(),
            },
            id,
            address,
        }
    }

    pub fn start(this: Arc<Mutex<Self>>) {
        let address;
        {
            address = Arc::clone(&this).lock().unwrap().address;
        }
        info!("Starting server at: {}...", address);
        let listener = TcpListener::bind(address).unwrap();
        for stream in listener.incoming() {
            let this_clone = Arc::clone(&this);
            match stream {
                Ok(stream) => {
                    thread::spawn(move || this_clone.lock().unwrap().handle_connection(stream));
                }
                Err(e) => {
                    info!("Error while listening to client: {}", e);
                }
            }
        }

        // background tasks
        let this_clone = Arc::clone(&this);
        thread::spawn(move || loop {
            let mut this_node = this_clone.lock().unwrap();

            // broadcast heartbeat if leader
            if this_node.maintenance.role == Role::Leader {
                this_node.broadcast_heartbeat();
            }

            // not leader therefore check time out and run election
            if this_node.has_timed_out() {
                this_node.run_election();
            }

            thread::sleep(Duration::from_millis(1));
        });
    }

    pub fn run_election(&mut self) {
        // prepare for election
        self.maintenance.role = Role::Candidate;
        self.persistent_state.current_term += 1;
        self.refresh_timeout();
        self.persistent_state.voted_for = Some(self.id);

        // request votes
        let votes = self.request_votes(
            self.persistent_state.current_term,
            self.id,
            // TODO: what's the right thing here
            0,
            0,
        );

        // count votes
        let votes_for = votes
            .iter()
            .map(|v| match *v {
                Message::VoteRequestResponse { vote_granted, .. } => vote_granted,
                _ => panic!(),
            })
            .filter(|v| *v)
            .count();

        // if election won
        if (votes_for + 1) > *MIN_QUORUM {
            info!(
                "Server {} has won the election! The new term is: {}",
                self.id, self.persistent_state.current_term
            );
            self.maintenance.role = Role::Leader;
            self.maintenance.next_timeout = None;
            self.broadcast_heartbeat();
        }
    }

    pub fn refresh_timeout(self: &mut Self) {
        let mut rng = rand::thread_rng();
        self.maintenance.next_timeout = Option::from(
            Instant::now()
                + Duration::new(
                    rng.gen_range(MIN_TIMEOUT..MAX_TIMEOUT).try_into().unwrap(),
                    0,
                ),
        );
    }

    pub fn has_timed_out(self: &mut Self) -> bool {
        match self.maintenance.next_timeout {
            Some(t) => Instant::now() > t,
            None => false,
        }
    }

    pub fn handle_vote_request(
        self: &mut Self,
        _term: LogTerm,
        _candidate_id: NodeId,
        _last_log_index: LogIndex,
        _last_log_term: LogTerm,
    ) -> (LogTerm, bool) {
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

    pub fn handele_append_entries(
        self: &mut Self,
        term: LogTerm,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: LogTerm,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> (LogTerm, bool) {
        if term < self.persistent_state.current_term {
            return (term, false);
        }

        if self.persistent_state.log.get(prev_log_index).is_none()
            || self.persistent_state.log.get(prev_log_index).unwrap().term != prev_log_term
        {
            return (term, false);
        }

        let matching_entries: Vec<LogEntry> = self.persistent_state.log[(prev_log_index + 1)..]
            .iter()
            .zip(entries.iter())
            .filter_map(|(recorded_entry, new_entry)| {
                recorded_entry.eq(new_entry).then(|| recorded_entry.clone())
            })
            .collect();
        let num_new_entries = entries.len() - matching_entries.len();
        self.persistent_state.log.truncate(prev_log_index + 1);
        self.persistent_state.log.extend(matching_entries);

        if leader_commit > self.volatile_state.commit_index {
            self.volatile_state.commit_index = min(leader_commit, prev_log_index + num_new_entries);
        }

        self.maintenance.current_leader = Some(leader_id);
        (self.persistent_state.current_term, true)
    }

    pub fn handle_heartbeat(self: &mut Self, term: LogTerm, node_id: NodeId) -> (bool, NodeId) {
        info!(
            "Server {} with term {}, received heartbeat from {} with term {}",
            self.id, self.persistent_state.current_term, node_id, term
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

    pub(crate) fn request_votes(
        &self,
        term: LogTerm,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: LogTerm,
    ) -> Vec<Message> {
        let message = Message::VoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };
        self.send(message)
    }

    fn broadcast_heartbeat(&self) {
        let rpc_message = Message::Heartbeat {
            term: self.persistent_state.current_term,
            node_id: self.id,
        };
        let responses = self.send(rpc_message);
        responses.into_iter().map(|x| match x {
            Message::HeartbeatResponse { .. } => x,
            _ => panic!(),
        });

        // A touch of randomness, so that we can get the chance
        // to have other leader elections.
        let mut rng = rand::thread_rng();
        thread::sleep(Duration::new(rng.gen_range(1..7), 0));
    }

    fn send_append_entries(&self, entries: Vec<LogEntry>) -> Vec<Message> {
        let rpc_message = Message::AppendEntriesRequest {
            term: self.persistent_state.current_term,
            leader_id: self.id,
            // TODO: is this the right index and term???
            // This is definitely wrong
            prev_log_index: self.volatile_state.commit_index,
            prev_log_term: self.persistent_state.current_term,
            entries,
            leader_commit: self.volatile_state.commit_index,
        };
        let rpc_responses = self.send(rpc_message);
        rpc_responses
            .into_iter()
            .map(|x| match x {
                Message::AppendEntriesResponse { .. } => x,
                _ => panic!(),
            })
            .collect()
    }
}

impl RaftNode {
    fn handle_connection(&mut self, mut stream: TcpStream) {
        loop {
            let mut buffer = [0; MESSAGE_LENGTH];
            stream.read(&mut buffer).unwrap();

            let d: Message = bincode::deserialize(&buffer).unwrap();

            let response = match d {
                Message::VoteRequest {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                } => {
                    let (term, vote_granted) =
                        self.handle_vote_request(term, candidate_id, last_log_index, last_log_term);
                    bincode::serialize(&Message::VoteRequestResponse { term, vote_granted })
                        .unwrap()
                }
                Message::AppendEntriesRequest {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => {
                    let (term, success) = self.handele_append_entries(
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                    );
                    bincode::serialize(&Message::AppendEntriesResponse { term, success }).unwrap()
                }
                Message::Heartbeat { term, node_id } => {
                    let (success, node_id) = self.handle_heartbeat(term, node_id);
                    bincode::serialize(&Message::HeartbeatResponse { success, node_id }).unwrap()
                }
                _ => todo!(), // Response messages;
            };

            stream.write(&response).unwrap();
            stream.flush().unwrap();
        }
    }

    fn send(&self, rpc_message: Message) -> Vec<Message> {
        let request_vote_bin = bincode::serialize(&rpc_message).unwrap();
        let mut rpc_responses: Vec<Message> = Vec::new();

        for mut stream in self.maintenance.peer_nodes.values() {
            stream.write(&request_vote_bin).unwrap();

            let mut buffer = [0; MESSAGE_LENGTH];
            stream.read(&mut buffer).unwrap();
            rpc_responses.push(bincode::deserialize(&buffer).unwrap());
        }
        rpc_responses
    }
}
