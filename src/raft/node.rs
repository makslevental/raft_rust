use crossbeam_utils::thread as crossbeam_thread;
use velcro::{hash_map, iter, vec};

use std::convert::TryInto;
use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::constants::{MAX_TIMEOUT, MESSAGE_LENGTH, MIN_QUORUM, MIN_TIMEOUT, NUM_SERVERS};
use crate::raft::types::{
    AppendEntriesRequest, Heartbeat, LeaderState, LogEntry, LogIndex, LogTerm, Message, NodeId,
    Peer, PersistentState, Ping, PingResponse, RaftNode, Role, VolatileState, VoteRequest,
    VoteRequestResponse,
};

use serde::Serialize;

use std::cmp::min;
use std::collections::HashMap;
use std::fmt::Debug;
use std::iter::FromIterator;
use std::thread::JoinHandle;

impl RaftNode {
    pub fn new(id: NodeId, address: SocketAddrV4, peers: Vec<(NodeId, SocketAddrV4)>) -> Self {
        RaftNode {
            persistent_state: PersistentState {
                current_term: 0,
                voted_for: None,
                log: vec![LogEntry {
                    // initialize so that subtracting to get prev_index gives 0 instead of -1
                    message: "NULL".to_string(),
                    term: 0,
                }],
            },
            volatile_state: VolatileState {
                commit_index: 0,
                last_applied: 0,
            },
            leader_state: None,
            //
            current_leader: None,
            role: Role::Follower,
            next_timeout: None,
            peers: peers
                .into_iter()
                .map(|(id, address)| {
                    (
                        id,
                        Peer {
                            id,
                            address,
                            connection: None,
                        },
                    )
                })
                .collect(),
            id,
            address,
            start_time: Instant::now(),
        }
    }

    pub fn start_background_tasks(this: Arc<Mutex<Self>>) -> JoinHandle<()> {
        let this_clone = Arc::clone(&this);
        this_clone.lock().unwrap().refresh_timeout();
        thread::spawn(move || loop {
            let mut this_node = this_clone.lock().unwrap();
            // broadcast heartbeat if leader
            if this_node.role == Role::Leader {
                this_node.send_heartbeat();
            } else if this_node.has_timed_out() {
                // not leader therefore check time out and run election
                this_node.start_election();
            }

            thread::sleep(Duration::from_nanos(5));

            // TODO: remove and use channels instead
            if Instant::now().duration_since(this_node.start_time) > Duration::from_secs(5) {
                break;
            }
        })
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
                println!("{:?} connected to {:?}", this.lock().unwrap().id, stream);
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

    pub fn connect_to_peers(&mut self) {
        println!("{:?} connecting to peers {:?}", self.id, self.peers);
        for (_pid, mut peer) in self.peers.iter_mut() {
            peer.connection = Some(TcpStream::connect(peer.address).unwrap());
        }
    }

    pub fn start_election(&mut self) {
        println!("{:?} starting election", self.id);
        // Each candidate restarts its randomized election timeout at the start of an election,
        // and it waits for that timeout to elapse before starting the next election;
        // this reduces the likelihood of another split vote in the new election.

        self.refresh_timeout();

        // To begin an election, a follower increments its current term and transitions to candidate state.
        self.persistent_state.current_term += 1;
        // ...for the same term...
        let election_term = self.persistent_state.current_term;
        self.role = Role::Candidate;
        self.leader_state = Option::from(LeaderState {
            next_index: Default::default(),
            match_index: Default::default(),
            votes: hash_map! {
                self.id: VoteRequestResponse{
                            node_id: self.id,
                            term: election_term,
                            vote_granted: true
                        }
            },
        });
        // It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
        self.persistent_state.voted_for = Some(self.id);
        self.send_vote_requests(election_term);
    }

    pub fn refresh_timeout(&mut self) {
        let mut rng = rand::thread_rng();
        self.next_timeout = Option::from(
            Instant::now()
                + Duration::from_millis(
                    rng.gen_range(MIN_TIMEOUT..MAX_TIMEOUT).try_into().unwrap(),
                ),
        );
    }

    pub fn has_timed_out(&mut self) -> bool {
        match self.next_timeout {
            Some(t) => Instant::now() > t,
            None => false,
        }
    }
}

impl RaftNode {
    pub fn send_vote_requests(&mut self, term: LogTerm) {
        let message = Message::V(VoteRequest {
            term,
            candidate_id: self.id,
            last_log_index: self.get_last_log_index(),
            last_log_term: self.get_last_log_term(),
        });
        self.send_to_all_peers(message)
    }

    pub fn handle_vote_request(&mut self, vr: VoteRequest) -> VoteRequestResponse {
        println!("{:?} received vote request {:?}", self.id, vr);
        if self.persistent_state.current_term < vr.term {
            self.convert_to_follower(vr.term);
        }

        if vr.term < self.persistent_state.current_term {
            return VoteRequestResponse {
                node_id: self.id,
                term: self.persistent_state.current_term,
                vote_granted: false,
            };
        }
        if self.persistent_state.voted_for.is_none()
            || self.persistent_state.voted_for.unwrap() == vr.candidate_id
        {
            if self.get_last_log_term() == vr.last_log_term
                && self.get_last_log_index() == vr.last_log_index
            {
                self.persistent_state.voted_for = Some(vr.candidate_id);
                return VoteRequestResponse {
                    node_id: self.id,
                    term: self.persistent_state.current_term,
                    vote_granted: true,
                };
            }
        }
        return VoteRequestResponse {
            node_id: self.id,
            term: self.persistent_state.current_term,
            vote_granted: false,
        };
    }

    pub fn handle_vote_request_response(&mut self, vrr: VoteRequestResponse) {
        println!("{:?} received vote request response {:?}", self.id, vrr);
        // // A candidate wins an election if it receives votes from a majority of the servers in the full cluster for the same term.
        let current_term = self.persistent_state.current_term;
        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if current_term < vrr.term {
            self.convert_to_follower(vrr.term);
            return;
        }

        // TODO: not sure how this happens but i'm getting spurious
        // vote requests and vote request responses being received (but never sent)
        // i suspect serialization bug
        if self.leader_state.is_none() {
            return;
        }

        self.leader_state
            .as_mut()
            .unwrap()
            .votes
            .insert(vrr.node_id, vrr);
        let votes_for = self
            .leader_state
            .as_mut()
            .unwrap()
            .votes
            .iter()
            .filter(|(_, v)| v.vote_granted && v.term == current_term)
            .count();
        println!(
            "{:?} got {:?} votes for; min votes {:?}",
            self.id, votes_for, *MIN_QUORUM
        );
        // no +1 since we included the vote for ourselves in the votes map
        if (votes_for) > *MIN_QUORUM {
            println!(
                "Server {} has won the election! The new term is: {}",
                self.id, current_term
            );
            // Once a candidate wins an election, it becomes leader.
            self.role = Role::Leader;
            self.next_timeout = None;
            // It then sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
            self.send_heartbeat();
            // When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
            self.leader_state.as_mut().unwrap().next_index = HashMap::from_iter(
                self.peers
                    .iter()
                    .map(|(&pid, _)| (pid, self.get_last_log_index() + 1)),
            )
        }
    }
    // pub fn service_client_request(&mut self, request: Message) -> Message {
    //     let res = Message::ClientResponse {
    //         success: false,
    //         leader_id: self.current_leader.unwrap(),
    //     };
    //     if self.role != Role::Leader {
    //         return res;
    //     }
    //     if let Message::ClientRequest { message } = request {
    //         let l = LogEntry {
    //             message,
    //             term: self.persistent_state.current_term,
    //         };
    //         self.persistent_state.log.push(l.clone());
    //         self.send_append_entries();
    //         return Message::ClientResponse {
    //             success: true,
    //             leader_id: self.current_leader.unwrap(),
    //         };
    //     }
    //     res
    // }
    //
    fn send_heartbeat(&mut self) {
        let rpc_message = Message::H(Heartbeat {
            term: self.persistent_state.current_term,
            node_id: self.id,
            leader_commit: self.volatile_state.commit_index,
        });
        self.send_to_all_peers(rpc_message);

        // A touch of randomness, so that we can get the chance
        // to have other leader elections.
        // let mut rng = rand::thread_rng();
        // thread::sleep(Duration::new(rng.gen_range(1..7), 0));
    }

    pub fn handle_heartbeat(&mut self, heartbeat: Heartbeat) {
        println!(
            "Server {} with term {}, received heartbeat from {} with term {}",
            self.id, self.persistent_state.current_term, heartbeat.node_id, heartbeat.term
        );

        // is it geq?
        if heartbeat.term >= self.persistent_state.current_term {
            println!(
                "Server {} becoming follower. The new leader is: {}",
                self.id, heartbeat.node_id
            );
            self.convert_to_follower(heartbeat.term);
            self.refresh_timeout();

            // If leaderCommit > commitIndex, set commitIndex =
            // min(leaderCommit, index of last new entry)
            if heartbeat.leader_commit > self.volatile_state.commit_index {
                self.volatile_state.commit_index =
                    min(heartbeat.leader_commit, self.get_last_log_index())
            }

            // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
            // then the candidate recognizes the leader as legitimate and returns to follower state.
            self.current_leader = Some(heartbeat.node_id);
        }
    }
    //
    // // TODO retries here if nodes go down?
    // fn send_append_entries(&mut self) {
    //     let term = self.persistent_state.current_term;
    //     for (pid, peer) in self.peers.iter_mut() {
    //         let (pid, next_indicies) = &self.leader_state.as_mut().unwrap().next_index;
    //         while let Some(&next_log_index) = next_indicies.get(&peer.id) {
    //             let prev_log_index = next_log_index - 1;
    //             let prev_log_term = self.persistent_state.log[prev_log_index].term;
    //             let entries = self.persistent_state.log[next_log_index..].to_vec();
    //             let rpc_message = Message::A(AppendEntriesRequest {
    //                 term,
    //                 leader_id: self.id,
    //                 prev_log_index,
    //                 prev_log_term,
    //                 entries,
    //                 leader_commit: self.volatile_state.commit_index,
    //             });
    //             peer.connection
    //                 .as_mut()
    //                 .unwrap()
    //                 .write(&bincode::serialize(&rpc_message).unwrap())
    //                 .unwrap();
    //         }
    //     }
    // }
    //
    // pub fn handele_append_entries(&mut self, a: AppendEntriesRequest) -> (NodeId, LogTerm, bool) {
    //     // Reply false if term < currentTerm
    //     if a.term < self.persistent_state.current_term {
    //         return (self.id, self.persistent_state.current_term, false);
    //     }
    //
    //     // Reply false if log doesn’t contain an entry at prevLogIndex
    //     // whose term matches prevLogTerm
    //     if self.persistent_state.log.get(a.prev_log_index).is_none()
    //         || self
    //             .persistent_state
    //             .log
    //             .get(a.prev_log_index)
    //             .unwrap()
    //             .term
    //             != a.prev_log_term
    //     {
    //         return (self.id, self.persistent_state.current_term, false);
    //     }
    //
    //     // If an existing entry conflicts with a new one (same index
    //     // but different terms), delete the existing entry and all that
    //     // follow it
    //     let matching_entries: Vec<LogEntry> = self.persistent_state.log[(a.prev_log_index + 1)..]
    //         .iter()
    //         .zip(a.entries.iter())
    //         .filter_map(|(recorded_entry, new_entry)| {
    //             recorded_entry.eq(new_entry).then(|| recorded_entry.clone())
    //         })
    //         .collect();
    //     self.persistent_state.log.truncate(a.prev_log_index + 1);
    //     // Append any new entries not already in the log
    //     self.persistent_state.log.extend(matching_entries);
    //
    //     // If leaderCommit > commitIndex, set commitIndex =
    //     // min(leaderCommit, index of last new entry)
    //     if a.leader_commit > self.volatile_state.commit_index {
    //         self.volatile_state.commit_index =
    //             min(a.leader_commit, self.persistent_state.log.len() - 1);
    //     }
    //
    //     // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
    //     // then the candidate recognizes the leader as legitimate and returns to follower state.
    //     self.current_leader = Some(a.leader_id);
    //     self.role = Role::Follower;
    //     (self.id, self.persistent_state.current_term, true)
    // }

    // pub fn handle_append_entries_response(&self) {
    //     let mut buffer = [0; MESSAGE_LENGTH];
    //     peer.connection.as_mut().unwrap().read(&mut buffer).unwrap();
    //     match bincode::deserialize(&buffer).unwrap() {
    //         // consistency check passes -> break while
    //         Message::AR(AppendEntriesResponse {
    //             success, peer_id, ..
    //         }) if success => {
    //             next_indicies[peer_id] = self.get_last_log_index() + 1;
    //         }
    //         // consistency check passes -> decrement prev_log_index
    //         Message::AR(AppendEntriesResponse {
    //             success, peer_id, ..
    //         }) if !success => {
    //             // After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
    //             // Eventually nextIndex will reach a point where the leader and follower logs match.
    //             if next_log_index == 0 {
    //                 panic!()
    //             }
    //             next_indicies[peer_id] -= 1;
    //         }
    //         _ => panic!(),
    //     };
    // }
    pub fn ping_all_peers(&mut self) {
        println!("{:?} sending ping to all peers", self.id);
        self.send_to_all_peers(Message::P(Ping { pinger_id: self.id }))
    }

    pub fn handle_ping(&mut self, p: Ping) -> PingResponse {
        println!("{:?} handling ping {:?}", self.id, p);
        PingResponse {
            pinger_id: p.pinger_id,
            ponger_id: self.id,
        }
    }

    pub fn handle_ping_response(&mut self, pr: PingResponse) {
        println!("node {:?} got ping response {:?}", self.id, pr);
        assert_eq!(pr.pinger_id, self.id);
        self.persistent_state.log.push(LogEntry {
            message: format!("{}", pr.ponger_id),
            term: 0,
        })
    }
}

impl RaftNode {
    fn handle_connection(this: Arc<Mutex<Self>>, mut peer_stream: TcpStream) {
        let node_id = this.lock().unwrap().id;
        println!(
            "{:?} handling connection {:?}",
            node_id,
            peer_stream.peer_addr()
        );
        loop {
            let mut buffer = [0; MESSAGE_LENGTH];
            peer_stream.read(&mut buffer).unwrap();
            let m = bincode::deserialize(&buffer).unwrap();
            println!(
                "node {:?} received message {:?} from node {:?}",
                node_id,
                m,
                peer_stream.peer_addr(),
            );
            let mut this = this.lock().unwrap();
            match m {
                Message::P(p) => {
                    let pr = Message::PR(this.handle_ping(p));
                    this.send(p.pinger_id, &pr);
                }
                Message::PR(pr) => {
                    this.handle_ping_response(pr);
                }
                Message::V(v) => {
                    let vrr = Message::VR(this.handle_vote_request(v));
                    this.send(v.candidate_id, &vrr);
                }
                Message::VR(vr) => {
                    this.handle_vote_request_response(vr);
                }
                // Message::A(a) => {
                //     let ar = this.handele_append_entries(a);
                //     bincode::serialize(&ar).unwrap();
                // }
                Message::H(h) => {
                    this.handle_heartbeat(h);
                }
                _ => {
                    println!("how did we get this {:?}", m);
                    panic!()
                }
            };

            thread::sleep(Duration::from_nanos(5));
        }
    }

    fn send<T: Serialize + Debug>(&mut self, node_id: NodeId, rpc_message: &T) {
        // TODO: not sure where this is coming from
        // but i'm getting vote requests from overflow ids
        if !self.peers.contains_key(&node_id) {
            return;
        }
        let mut peer_stream = self
            .peers
            .get_mut(&node_id)
            .unwrap()
            .connection
            .as_mut()
            .unwrap();
        println!(
            "{:?} sending message {:?} to {:?}",
            peer_stream.local_addr(),
            rpc_message,
            peer_stream.peer_addr()
        );
        let rpc_message_bin = bincode::serialize(rpc_message).unwrap();
        peer_stream.write(&rpc_message_bin).unwrap();
        peer_stream.flush().unwrap();
    }

    fn send_to_all_peers<T: Serialize + Send + Sync + Debug + Clone>(&mut self, rpc_message: T) {
        println!(
            "{:?} sending message {:?} to all peers",
            self.id, rpc_message
        );
        crossbeam_thread::scope(|scope| {
            self.peers.iter_mut().for_each(|(_, peer)| {
                let rpc_message = rpc_message.clone();
                let mut peer_stream = peer.connection.as_mut().unwrap();
                println!(
                    "{:?} sending message {:?} to {:?}",
                    peer_stream.local_addr(),
                    rpc_message,
                    peer_stream.peer_addr()
                );
                let rpc_message_bin = bincode::serialize(&rpc_message).unwrap();

                scope.spawn(move |_| {
                    peer_stream.write(&rpc_message_bin).unwrap();
                    peer_stream.flush().unwrap();
                });
            })
        })
        .unwrap();
    }
}

impl RaftNode {
    fn get_last_log_index(&self) -> LogIndex {
        self.persistent_state.log.len() - 1
    }

    fn get_last_log_term(&self) -> LogTerm {
        self.persistent_state.log[self.get_last_log_index()].term
    }

    fn convert_to_follower(&mut self, term: LogTerm) {
        self.role = Role::Follower;
        self.leader_state = None;
        self.persistent_state.current_term = term;
        // who do i follow if term T > currentTerm
        // self.current_leader = Some(leader_id);
        self.persistent_state.voted_for = None;
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::types::{LogTerm, NodeId, RaftNode, Role};
    use std::cmp::max;
    use std::collections::{HashMap, HashSet};
    use std::error::Error;
    use std::iter::FromIterator;
    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::Duration;

    const RAFT_ELECTION_GRACE_PERIOD: u64 = 1;

    fn build_raft_nodes(num_servers: usize) -> Vec<Arc<Mutex<RaftNode>>> {
        let node_ids = 0..num_servers;
        let nodes: Vec<(NodeId, SocketAddrV4)> = node_ids
            .clone()
            .map(|i| (i, SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16)))
            .collect();
        let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
            .clone()
            .map(|i| {
                Arc::new(Mutex::new(RaftNode::new(
                    i,
                    nodes[i as usize].1,
                    nodes
                        .clone()
                        .into_iter()
                        .filter(|(pid, _address)| *pid != i)
                        .collect(),
                )))
            })
            .collect();

        for raft_node in raft_nodes.iter() {
            RaftNode::start_server(raft_node.clone());
        }

        for raft_node in raft_nodes.iter() {
            raft_node.lock().unwrap().connect_to_peers();
        }
        raft_nodes
    }

    fn check_one_leader(raft_nodes: &Vec<Arc<Mutex<RaftNode>>>) -> Result<NodeId, String> {
        // try a few times in case re-elections are needed.
        for i in 0..10 {
            thread::sleep(Duration::from_millis(500));
            let mut leaders: HashMap<LogTerm, Vec<NodeId>> = Default::default();
            for r in raft_nodes {
                let r = r.lock().unwrap();
                if r.role == Role::Leader {
                    if !leaders.contains_key(&r.persistent_state.current_term) {
                        leaders.insert(r.persistent_state.current_term, vec![]);
                    }
                    leaders
                        .get_mut(&r.persistent_state.current_term)
                        .unwrap()
                        .push(r.id);
                }
            }

            let mut last_term_with_leader = 0;
            for (term, leaders) in &leaders {
                if leaders.len() > 1 {
                    return Err(format!(
                        "term {:?} has {:?} (>1) leaders",
                        term,
                        leaders.len()
                    ));
                }
                last_term_with_leader = max(last_term_with_leader, *term);
            }
            if leaders.len() > 0 && leaders.get(&last_term_with_leader).unwrap().len() == 1 {
                return Ok(*leaders
                    .get(&last_term_with_leader)
                    .unwrap()
                    .first()
                    .unwrap());
            }
        }
        return Err("expected one leader, got none".to_string());
    }

    fn check_terms(raft_nodes: &Vec<Arc<Mutex<RaftNode>>>) -> Result<LogTerm, String> {
        let terms: HashSet<LogTerm> = HashSet::from_iter(
            raft_nodes
                .iter()
                .map(|r| r.lock().unwrap().persistent_state.current_term),
        );
        if terms.len() != 1 {
            return Err("expected one term, got many".to_string());
        }
        return Ok(*terms.iter().next().unwrap());
    }

    #[test]
    fn test_ping_ping() {
        let num_servers: usize = 10;
        let raft_nodes = build_raft_nodes(num_servers);

        for raft_node in raft_nodes.iter() {
            raft_node.lock().unwrap().ping_all_peers();
        }

        thread::sleep(Duration::from_secs(5));

        for raft_node in raft_nodes.iter() {
            let ping_ponged_peers: HashSet<usize> = HashSet::from_iter(
                raft_node.lock().unwrap().persistent_state.log[1..]
                    .iter()
                    .map(|le| le.message.parse().unwrap()),
            );
            let all_peers: HashSet<usize> = HashSet::from_iter(
                raft_node
                    .lock()
                    .unwrap()
                    .peers
                    .iter()
                    .map(|(pid, peer)| *pid),
            );
            assert_eq!(ping_ponged_peers, all_peers);
        }
    }

    #[test]
    fn test_initial_election_2a() {
        let num_servers: usize = 3;
        let raft_nodes = build_raft_nodes(num_servers);

        let mut handles: Vec<JoinHandle<()>> = vec![];
        for raft_node in &raft_nodes {
            let handle = RaftNode::start_background_tasks(raft_node.clone());
            handles.push(handle);
        }

        println!("start tests");

        thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
        let leader = check_one_leader(&raft_nodes);
        assert!(leader.is_ok(), format!("{:?}", leader));
        thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
        let term1 = check_terms(&raft_nodes);
        thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
        let term2 = check_terms(&raft_nodes);
        assert!(term2.is_ok());

        assert_eq!(term1, term2);

        println!("tests successfully completed");

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
