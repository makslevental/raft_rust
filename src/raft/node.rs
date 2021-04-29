use std::cmp::min;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::Debug;
use std::io::{self, BufRead};
use std::io::{Read, Write};
use std::iter::FromIterator;
use std::net::{Shutdown, SocketAddrV4, TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_utils::thread as crossbeam_thread;
use log::{debug, info, warn};
use rand::Rng;
use serde::Serialize;
use velcro::{hash_map, iter, vec};

use crate::constants::MESSAGE_LENGTH;
use crate::raft::types::{
    AppendEntriesRequest, CRCMessage, Heartbeat, LeaderState, LogEntry, LogIndex, LogTerm, Message,
    NodeId, Peer, PersistentState, Ping, PingResponse, RaftNode, Role, VolatileState, VoteRequest,
    VoteRequestResponse,
};

impl RaftNode {
    pub fn new(
        id: NodeId,
        address: SocketAddrV4,
        peers: Vec<(NodeId, SocketAddrV4)>,
        MAJORITY: usize,
        MIN_TIMEOUT: usize,
        MAX_TIMEOUT: usize,
    ) -> Self {
        let mut r = RaftNode {
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
            MAJORITY,
            MIN_TIMEOUT,
            MAX_TIMEOUT,
        };
        r.refresh_timeout();
        r
    }

    pub fn connect_to_peers(&mut self) {
        debug!("node {:?} connecting to peers {:?}", self.id, self.peers);
        for (_pid, mut peer) in self.peers.iter_mut() {
            peer.connection = Some(TcpStream::connect(peer.address).unwrap());
        }
    }

    pub fn start_election(&mut self) {
        debug!("node {:?} starting election", self.id);
        // Each candidate restarts its randomized election timeout at the start of an election,
        // and it waits for that timeout to elapse before starting the next election;
        // this reduces the likelihood of another split vote in the new election.

        self.refresh_timeout();

        // To begin an election, a follower increments its current term and transitions to candidate state.
        self.persistent_state.current_term += 1;
        self.current_leader = None;
        // ...for the same term...
        let election_term = self.persistent_state.current_term;
        self.role = Role::Candidate;
        self.leader_state = Some(LeaderState {
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
        self.next_timeout = Some(
            Instant::now()
                + Duration::from_millis(
                    rng.gen_range(self.MIN_TIMEOUT..self.MAX_TIMEOUT)
                        .try_into()
                        .unwrap(),
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

    // Current terms are exchanged whenever servers communicate; if one server’s current term is
    // smaller than the other’s, then it updates its current term to the larger value.
    // If a candidate or leader discovers that its term is out of date, it immediately reverts to
    // fol- lower state. If a server receives a request with a stale term number, it rejects the request.

    pub fn handle_vote_request(&mut self, vr: VoteRequest) -> VoteRequestResponse {
        debug!("node {:?} received vote request {:?}", self.id, vr);

        if self.persistent_state.current_term < vr.term {
            // switch to follower but not necessarily follow this node
            self.convert_to_follower(vr.term, Some(vr.candidate_id));
        }

        if self.persistent_state.current_term > vr.term {
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
        debug!(
            "node {:?} received vote request response {:?}",
            self.id, vrr
        );

        // TODO: e.g. if some old node comes back online and hasn't gotten
        // its term updated then it might send out of date responses?
        if self.persistent_state.current_term > vrr.term {
            return;
        }

        if self.persistent_state.current_term < vrr.term {
            self.convert_to_follower(vrr.term, Some(vrr.node_id));
            return;
        }

        let current_term = self.persistent_state.current_term;

        // TODO: do i really need this? shouldn't this be an unreachable state?
        if self.role != Role::Candidate {
            // thanks but i'm already popular
            return;
        }

        // TODO: not sure how this happens but i'm getting spurious
        // vote requests and vote request responses being received (but never sent)
        // i suspect serialization bug (something as usize and u64)
        // if self.leader_state.is_none() {
        //     return;
        // }
        let self_id = self.id;
        self.leader_state
            .as_mut()
            .unwrap_or_else(|| panic!("{:?}", self_id))
            .votes
            .insert(vrr.node_id, vrr);
        let votes_for = self
            .leader_state
            .as_mut()
            .unwrap()
            .votes
            .iter()
            .filter(|(_, v)| v.vote_granted)
            .count();
        debug!(
            "node {:?} got {:?} votes for; min votes {:?}",
            self.id, votes_for, self.MAJORITY
        );
        // no +1 since we included the vote for ourselves in the votes map
        if (votes_for) >= self.MAJORITY {
            debug!(
                "node {} has won the election! The new term is: {}",
                self.id, current_term
            );
            // Once a candidate wins an election, it becomes leader.
            self.role = Role::Leader;
            self.current_leader = Some(self.id);
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
        debug!(
            "node {} with term {}, received heartbeat from {} with term {}",
            self.id, self.persistent_state.current_term, heartbeat.node_id, heartbeat.term
        );

        // old leader
        if self.persistent_state.current_term == heartbeat.term
            && self.current_leader.unwrap_or(u64::MAX) == heartbeat.node_id
        {
            self.refresh_timeout();
            return;
        }

        // new leader
        if heartbeat.term > self.persistent_state.current_term
            && self.persistent_state.voted_for.unwrap_or(u64::MAX) == heartbeat.node_id
        {
            debug!(
                "node {} becoming follower. The new leader is: {}",
                self.id, heartbeat.node_id
            );
            self.convert_to_follower(heartbeat.term, Some(heartbeat.node_id));

            // If leaderCommit > commitIndex, set commitIndex =
            // min(leaderCommit, index of last new entry)
            if heartbeat.leader_commit > self.volatile_state.commit_index {
                self.volatile_state.commit_index =
                    min(heartbeat.leader_commit, self.get_last_log_index())
            }

            // TODO
            // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
            // then the candidate recognizes the leader as legitimate and returns to follower state.
        }
    }

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
        debug!("node {:?} sending ping to all peers", self.id);
        self.send_to_all_peers(Message::P(Ping { pinger_id: self.id }))
    }

    pub fn handle_ping(&mut self, p: Ping) -> PingResponse {
        debug!("node {:?} handling ping {:?}", self.id, p);
        PingResponse {
            pinger_id: p.pinger_id,
            ponger_id: self.id,
        }
    }

    pub fn handle_ping_response(&mut self, pr: PingResponse) {
        debug!("node {:?} got ping response {:?}", self.id, pr);
        assert_eq!(pr.pinger_id, self.id);
        self.persistent_state.log.push(LogEntry {
            message: format!("{}", pr.ponger_id),
            term: 0,
        })
    }
}

impl RaftNode {
    pub fn start_background_tasks(this: Arc<Mutex<Self>>) -> (JoinHandle<()>, Sender<()>) {
        let (tx, rx) = mpsc::channel();
        let this_clone = this.clone();
        let handle = thread::spawn(move || loop {
            let mut rng = rand::thread_rng();

            // TODO: becareful about holding locks for too long!!!

            // broadcast heartbeat if leader
            {
                let mut this_node = this_clone.lock().unwrap();
                if this_node.has_timed_out() {
                    // not leader therefore check time out and run election
                    this_node.start_election();
                }
            }

            {
                // "work" -> will become append entries or service client request
                thread::sleep(Duration::from_millis(
                    rng.gen_range(15..30).try_into().unwrap(),
                ));
            }

            {
                let mut this_node = this_clone.lock().unwrap();
                if this_node.role == Role::Leader {
                    this_node.send_heartbeat();
                }
            }

            {
                let this_node = this_clone.lock().unwrap();
                match rx.try_recv() {
                    Ok(_) | Err(TryRecvError::Disconnected) => {
                        debug!("node {:?} terminating background tasks", this_node.id);
                        break;
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }
        });
        (handle, tx)
    }

    pub fn start_server(this: Arc<Mutex<Self>>) -> (JoinHandle<()>, Sender<()>) {
        let address = this.lock().unwrap().address;
        debug!("Starting server at: {}...", address);

        let listener = TcpListener::bind(address).unwrap();
        // https://stackoverflow.com/a/56693740/9045206
        listener
            .set_nonblocking(true)
            .expect("Cannot set non-blocking");

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        debug!(
                            "node {:?} connected to {:?}",
                            this.lock().unwrap().id,
                            stream
                        );
                        // listener should be nonblocking but not stream
                        stream.set_nonblocking(false);
                        let this = this.clone();
                        thread::spawn(move || Self::handle_connection(this, stream));
                    }
                    Err(err) => {
                        if err.kind() != io::ErrorKind::WouldBlock {
                            println!("leaving loop, error: {}", err);
                            break;
                        }
                    }
                }

                match rx.try_recv() {
                    Ok(_) | Err(TryRecvError::Disconnected) => {
                        debug!(
                            "node {:?} terminating handle connection",
                            this.lock().unwrap().id
                        );
                        break;
                    }
                    Err(TryRecvError::Empty) => {}
                };
                thread::sleep(Duration::from_millis(10));
            }
        });
        (handle, tx)
    }
    fn handle_connection(this: Arc<Mutex<Self>>, mut peer_stream: TcpStream) {
        let node_id = this.lock().unwrap().id;
        debug!(
            "node {:?} handling connection {:?}",
            node_id,
            peer_stream.peer_addr()
        );
        loop {
            let mut buffer = [0; MESSAGE_LENGTH];
            peer_stream.read(&mut buffer).unwrap();
            let m: CRCMessage = bincode::deserialize(&buffer).unwrap();
            if !m.check_crc() {
                debug!("crc failed for {:?}", m);
                continue;
            }
            let m = m.msg;
            debug!(
                "node {:?} received message {:?} from node {:?}",
                node_id,
                m,
                peer_stream.peer_addr(),
            );

            // TODO: moving this down from above let the other nodes catch up
            // i'm guessing continue doesn't drop the lock or something?
            let mut this = this.lock().unwrap();
            match m {
                Message::P(p) => {
                    let pr = Message::PR(this.handle_ping(p));
                    this.send(p.pinger_id, pr);
                }
                Message::PR(pr) => {
                    this.handle_ping_response(pr);
                }
                Message::V(v) => {
                    let vrr = Message::VR(this.handle_vote_request(v));
                    this.send(v.candidate_id, vrr);
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
                    debug!("how did we get this {:?}", m);
                    panic!()
                }
            };
        }
    }

    fn send(&mut self, node_id: NodeId, message: Message) {
        // TODO: not sure where this is coming from
        // but i'm getting vote requests from overflow ids
        // if !self.peers.contains_key(&node_id) {
        //     return;
        // }
        let mut peer_stream = self
            .peers
            .get_mut(&node_id)
            .unwrap()
            .connection
            .as_mut()
            .unwrap();
        debug!(
            "node {:?} sending message {:?} to {:?}",
            self.id,
            message,
            peer_stream.peer_addr()
        );
        let rpc_message = CRCMessage::new(message);
        let rpc_message_bin = bincode::serialize(&rpc_message).unwrap();
        peer_stream.write(&rpc_message_bin).unwrap();
        peer_stream.flush().unwrap();
    }

    fn send_to_all_peers(&mut self, message: Message) {
        debug!(
            "node {:?} sending message {:?} to all peers",
            self.id, message
        );
        let self_id = self.id;
        crossbeam_thread::scope(|scope| {
            self.peers.iter_mut().for_each(|(_, peer)| {
                let message = message.clone();
                let mut peer_stream = peer.connection.as_mut().unwrap();
                scope.spawn(move |_| {
                    debug!(
                        "node {:?} sending message {:?} to {:?}",
                        self_id,
                        message,
                        peer_stream.peer_addr()
                    );
                    let rpc_message = CRCMessage::new(message);
                    let rpc_message_bin = bincode::serialize(&rpc_message).unwrap();
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
        (self.persistent_state.log.len() - 1) as u64
    }

    fn get_last_log_term(&self) -> LogTerm {
        self.persistent_state.log[self.get_last_log_index() as usize].term
    }

    fn convert_to_follower(&mut self, term: LogTerm, leader_id: Option<NodeId>) {
        self.role = Role::Follower;
        self.leader_state = None;
        self.persistent_state.current_term = term;
        // TODO who do i follow if term T > currentTerm
        self.current_leader = leader_id;
        self.persistent_state.voted_for = None;
        self.refresh_timeout();
    }
}
