use std::net::{SocketAddrV4, TcpStream};

use crc32fast::Hasher;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

pub type LogTerm = u64;
pub type NodeId = u64;
pub type LogIndex = usize;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub message: String,
    pub term: LogTerm,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
}

#[derive(Debug)]
pub struct PersistentState {
    pub current_term: LogTerm,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub state_machine: Vec<LogEntry>,
}

#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Debug)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
    pub votes: HashMap<NodeId, VoteRequestResponse>,
}

#[derive(Debug)]
pub struct RaftNode {
    pub persistent_state: PersistentState,
    pub volatile_state: VolatileState,
    pub leader_state: Option<LeaderState>,

    //
    pub current_leader: Option<NodeId>,
    pub role: Role,
    pub next_timeout: Option<Instant>,
    pub id: NodeId,
    pub address: SocketAddrV4,
    pub peers: HashMap<NodeId, Peer>,
    pub MAJORITY: usize,
    pub MIN_TIMEOUT: usize,
    pub MAX_TIMEOUT: usize,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct VoteRequest {
    pub term: LogTerm,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: LogTerm,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct VoteRequestResponse {
    pub node_id: NodeId,
    pub term: LogTerm,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, macros::FromTuple)]
pub struct AppendEntriesRequest {
    pub term: LogTerm,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: LogTerm,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct AppendEntriesResponse {
    pub node_id: NodeId,
    pub term: LogTerm,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct Heartbeat {
    pub term: LogTerm,
    pub node_id: NodeId,
    pub leader_commit: LogIndex,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct HeartbeatResponse {
    pub success: bool,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Clone, Debug, macros::FromTuple)]
pub struct ClientRequest {
    pub message: String,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct ClientResponse {
    pub success: bool,
    pub leader_id: NodeId,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct Ping {
    pub pinger_id: NodeId,
}

#[derive(Serialize, Deserialize, Copy, Clone, Debug, macros::FromTuple)]
pub struct PingResponse {
    pub pinger_id: NodeId,
    pub ponger_id: NodeId,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Message {
    V(VoteRequest),
    VR(VoteRequestResponse),
    A(AppendEntriesRequest),
    AR(AppendEntriesResponse),
    H(Heartbeat),
    HR(HeartbeatResponse),
    C(ClientRequest),
    CR(ClientResponse),
    P(Ping),
    PR(PingResponse),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CRCMessage {
    pub msg: Message,
    crc: u32,
}

impl CRCMessage {
    pub fn new(msg: Message) -> Self {
        let message_bin = bincode::serialize(&msg).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(message_bin.as_ref());
        let checksum = hasher.finalize();
        CRCMessage { msg, crc: checksum }
    }

    pub fn check_crc(&self) -> bool {
        let message_bin = bincode::serialize(&self.msg).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(message_bin.as_ref());
        let checksum = hasher.finalize();
        return checksum == self.crc;
    }
}

#[derive(Debug)]
pub struct Peer {
    pub id: NodeId,
    pub address: SocketAddrV4,
    pub connection: Option<TcpStream>,
}
