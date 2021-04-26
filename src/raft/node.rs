use std::cmp::min;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::constants::{MAX_TIMEOUT, MESSAGE_LENGTH, MIN_QUORUM, MIN_TIMEOUT, NUM_SERVERS};
use crate::raft::types::{
    LogEntry, LogIndex, LogTerm, Maintenance, Message, NodeId, Peer, PersistentState, RaftNode,
    Role, VolatileState,
};
use std::any::Any;

impl RaftNode {
    pub fn new(id: NodeId, address: SocketAddrV4) -> Self {
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
                peer_nodes: None,
            },
            id,
            address,
        }
    }

    pub fn start_background_tasks(this: Arc<Mutex<Self>>) {
        let this_clone = Arc::clone(&this);
        this_clone.lock().unwrap().refresh_timeout();
        let handle = thread::spawn(move || loop {
            let mut this_node = this_clone.lock().unwrap();

            // broadcast heartbeat if leader
            if this_node.maintenance.role == Role::Leader {
                this_node.send_heartbeat();
            }

            // not leader therefore check time out and run election
            if this_node.has_timed_out() {
                this_node.run_election();
            }

            thread::sleep(Duration::from_millis(1));
        });

        handle.join().unwrap();
    }

    pub fn start_server(this: Arc<Mutex<Self>>) {
        let address;
        {
            address = Arc::clone(&this).lock().unwrap().address;
        }
        println!("Starting server at: {}...", address);
        let listener = TcpListener::bind(address).unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let this = this.clone();
                match stream {
                    Ok(stream) => {
                        thread::spawn(move || Self::handle_connection(this, stream));
                    }
                    Err(e) => {
                        println!("Error while listening to client: {}", e);
                    }
                }
            }
        });
    }

    pub fn set_peers(&mut self, peers: &Vec<Peer>) {
        self.maintenance.peer_nodes = Some(
            peers
                .iter()
                .map(|p| (p.id, TcpStream::connect(&p.address).unwrap()))
                .collect(),
        );
    }

    pub fn run_election(&mut self) {
        // Each candidate restarts its randomized election timeout at the start of an election,
        // and it waits for that timeout to elapse before starting the next election;
        // this reduces the likelihood of another split vote in the new election.
        self.refresh_timeout();

        // To begin an election, a follower increments its current term and transitions to candidate state.
        self.persistent_state.current_term += 1;
        // ...for the same term...
        let election_term = self.persistent_state.current_term;
        self.maintenance.role = Role::Candidate;
        // It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
        self.persistent_state.voted_for = Some(self.id);
        let votes = self.send_vote_requests(
            election_term,
            self.id,
            // TODO(max) is this the right way to compute last valid log index
            // or should it be "last commit index"???
            self.persistent_state.log.len(),
            self.persistent_state
                .log
                .last()
                .unwrap_or(&LogEntry {
                    message: "".to_string(),
                    term: 0,
                })
                .term,
        );

        // A candidate wins an election if it receives votes from a majority of the servers in the full cluster for the same term.
        let votes_for = votes
            .into_iter()
            .filter(|(term, vote)| *vote && *term == election_term)
            .count();
        if (votes_for + 1) > *MIN_QUORUM {
            println!(
                "Server {} has won the election! The new term is: {}",
                self.id, election_term
            );
            // Once a candidate wins an election, it becomes leader.
            self.maintenance.role = Role::Leader;
            self.maintenance.next_timeout = None;
            // It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
            self.send_heartbeat();
            // When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
            self.leader_state.as_mut().unwrap().next_index =
                [self.persistent_state.log.len(); NUM_SERVERS];
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
}

impl RaftNode {
    pub fn service_client_request(&mut self, request: Message) -> Message {
        let res = Message::ClientResponse {
            success: false,
            leader_id: self.maintenance.current_leader.unwrap(),
        };
        if self.maintenance.role != Role::Leader {
            return res;
        }
        if let Message::ClientRequest { message } = request {
            let l = LogEntry {
                message,
                term: self.persistent_state.current_term,
            };
            self.persistent_state.log.push(l.clone());
            self.send_append_entries();
            return Message::ClientResponse {
                success: true,
                leader_id: self.maintenance.current_leader.unwrap(),
            };
        }
        res
    }

    pub fn send_vote_requests(
        &self,
        term: LogTerm,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: LogTerm,
    ) -> Vec<(LogTerm, bool)> {
        let message = Message::VoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };
        let responses = self.send(message);
        // count votes
        responses
            .iter()
            .map(|v| match *v {
                Message::VoteRequestResponse { term, vote_granted } => (term, vote_granted),
                _ => panic!(),
            })
            .collect()
    }

    pub fn handle_vote_request(
        self: &mut Self,
        term: LogTerm,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: LogTerm,
    ) -> (LogTerm, bool) {
        if term < self.persistent_state.current_term {
            return (self.persistent_state.current_term, false);
        }
        if self.persistent_state.voted_for.is_none()
            || self.persistent_state.voted_for.unwrap() == candidate_id
        {
            if let Some(last_log_entry) = self.persistent_state.log.last() {
                if last_log_entry.term == last_log_term
                    && self.persistent_state.log.len() - 1 == last_log_index
                {
                    return (self.persistent_state.current_term, true);
                }
            }
        }
        return (self.persistent_state.current_term, false);
    }

    fn send_heartbeat(&self) {
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

    pub fn handle_heartbeat(self: &mut Self, term: LogTerm, node_id: NodeId) -> (bool, NodeId) {
        println!(
            "Server {} with term {}, received heartbeat from {} with term {}",
            self.id, self.persistent_state.current_term, node_id, term
        );

        todo!()
        // self.refresh_timeout();
        //
        // if term > self.state.current_term {
        //     println!(
        //         "Server {} becoming follower. The new leader is: {}",
        //         self.id, node_id
        //     );
        //
        //     (term, true)
        // } else {
        //     (self.state.current_term, false)
        // }
    }

    // TODO retries here if nodes go down?
    fn send_append_entries(&mut self) {
        let mut term = self.persistent_state.current_term;
        for (id, mut stream) in self.maintenance.peer_nodes.as_ref().unwrap() {
            let mut next_indicies = self.leader_state.as_mut().unwrap().next_index;
            'inner: while let Some(&prev_log_index) = next_indicies.get(*id as usize) {
                let prev_log_term = self.persistent_state.log[prev_log_index].term;
                let entries = self.persistent_state.log[prev_log_index + 1..].to_vec();
                let rpc_message = Message::AppendEntriesRequest {
                    term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.volatile_state.commit_index,
                };
                stream
                    .write(&bincode::serialize(&rpc_message).unwrap())
                    .unwrap();

                let mut buffer = [0; MESSAGE_LENGTH];
                stream.read(&mut buffer).unwrap();
                match bincode::deserialize(&buffer).unwrap() {
                    // consistency check passes -> break while
                    Message::AppendEntriesResponse { success, .. } if success => break 'inner,
                    // consistency check passes -> decrement prev_log_index
                    Message::AppendEntriesResponse { success, .. } if !success => {
                        // After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
                        // Eventually nextIndex will reach a point where the leader and follower logs match.
                        if prev_log_index == 0 {
                            panic!()
                        }
                        next_indicies[*id as usize] -= 1;
                    }
                    _ => panic!(),
                };
            }
        }
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
            return (self.persistent_state.current_term, false);
        }

        if self.persistent_state.log.get(prev_log_index).is_none()
            || self.persistent_state.log.get(prev_log_index).unwrap().term != prev_log_term
        {
            return (self.persistent_state.current_term, false);
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

        // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
        // then the candidate recognizes the leader as legitimate and returns to follower state.
        self.maintenance.current_leader = Some(leader_id);
        self.maintenance.role = Role::Follower;
        (self.persistent_state.current_term, true)
    }
}

impl RaftNode {
    fn handle_connection(this: Arc<Mutex<Self>>, mut stream: TcpStream) {
        let this = Arc::clone(&this);
        loop {
            let mut buffer = [0; MESSAGE_LENGTH];
            stream.read(&mut buffer).unwrap();

            let d: Message = bincode::deserialize(&buffer).unwrap();

            let mut this = this.lock().unwrap();
            let response = match d {
                Message::VoteRequest {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                } => {
                    let (term, vote_granted) =
                        this.handle_vote_request(term, candidate_id, last_log_index, last_log_term);
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
                    let (term, success) = this.handele_append_entries(
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
                    let (success, node_id) = this.handle_heartbeat(term, node_id);
                    bincode::serialize(&Message::HeartbeatResponse { success, node_id }).unwrap()
                }
                _ => panic!(), // Response messages;
            };

            stream.write(&response).unwrap();
            stream.flush().unwrap();
        }
    }

    // TODO: this should be done in parallel but i'm running into self being captured
    fn send(&self, rpc_message: Message) -> Vec<Message> {
        let rpc_message_bin = bincode::serialize(&rpc_message).unwrap();
        let mut rpc_responses: Vec<Message> = Vec::new();

        for mut stream in self.maintenance.peer_nodes.as_ref().unwrap().values() {
            stream.write(&rpc_message_bin).unwrap();

            let mut buffer = [0; MESSAGE_LENGTH];
            stream.read(&mut buffer).unwrap();
            rpc_responses.push(bincode::deserialize(&buffer).unwrap());
        }
        rpc_responses
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn create_nodes() {
        let node_ids = 0..3;
        let peers: Vec<Peer> = node_ids
            .clone()
            .map(|i| Peer {
                id: i as u64,
                address: SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16),
            })
            .collect();

        let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
            .clone()
            .map(|i| {
                Arc::new(Mutex::new(RaftNode::new(
                    i as u64,
                    peers[i as usize].address,
                )))
            })
            .collect();

        raft_nodes
            .iter()
            .for_each(|r| RaftNode::start_server(r.clone()));

        raft_nodes.iter().for_each(|r| {
            let rid = r.clone().lock().unwrap().id;
            r.clone().lock().unwrap().set_peers(
                &peers
                    .clone()
                    .into_iter()
                    .filter(|p| p.id != rid as u64)
                    .collect(),
            )
        });

        let mut connections: HashMap<String, HashSet<String>> = HashMap::new();
        for raft_node in &raft_nodes {
            let address = raft_node.lock().as_ref().unwrap().address;
            for (_ip_address, connection) in raft_node
                .lock()
                .as_ref()
                .unwrap()
                .maintenance
                .peer_nodes
                .as_ref()
                .unwrap()
            {
                let k = address.to_string();
                let v = connection.peer_addr().unwrap().to_string();
                if !connections.contains_key(&k) {
                    connections.insert(k.clone(), HashSet::new());
                }
                connections.get_mut(&k).unwrap().insert(v);
            }
        }
        assert_eq!(
            connections[&"127.0.0.1:3300".to_string()],
            ["127.0.0.1:3301", "127.0.0.1:3302"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
        assert_eq!(
            connections[&"127.0.0.1:3301".to_string()],
            ["127.0.0.1:3300", "127.0.0.1:3302"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
        assert_eq!(
            connections[&"127.0.0.1:3302".to_string()],
            ["127.0.0.1:3300", "127.0.0.1:3301"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );

        // raft_nodes
        //     .iter()
        //     .for_each(|r| RaftNode::start_background_tasks(r.clone()));

        // loop {}
    }

    #[test]
    fn check_bg_tasks() {
        let node_ids = 0..3;
        let peers: Vec<Peer> = node_ids
            .clone()
            .map(|i| Peer {
                id: i as u64,
                address: SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16),
            })
            .collect();

        let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
            .clone()
            .map(|i| {
                Arc::new(Mutex::new(RaftNode::new(
                    i as u64,
                    peers[i as usize].address,
                )))
            })
            .collect();

        raft_nodes
            .iter()
            .for_each(|r| RaftNode::start_server(r.clone()));

        raft_nodes.iter().for_each(|r| {
            let rid = r.clone().lock().unwrap().id;
            r.clone().lock().unwrap().set_peers(
                &peers
                    .clone()
                    .into_iter()
                    .filter(|p| p.id != rid as u64)
                    .collect(),
            )
        });

        raft_nodes
            .iter()
            .for_each(|r| RaftNode::start_background_tasks(r.clone()));

        // loop {}
    }
}
