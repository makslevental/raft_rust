use std::collections::HashMap;
use std::net::{SocketAddrV4, TcpStream};

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::constants::NUM_SERVERS;

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
}

#[derive(Debug)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
}

#[derive(Debug)]
pub struct LeaderState {
    pub next_index: [LogIndex; NUM_SERVERS],
    pub match_index: [LogIndex; NUM_SERVERS],
}

#[derive(Debug)]
pub struct Maintenance {
    pub current_leader: Option<NodeId>,
    pub role: Role,
    pub next_timeout: Option<Instant>,
    pub peer_nodes: HashMap<String, TcpStream>,
}

#[derive(Debug)]
pub struct RaftNode {
    pub persistent_state: PersistentState,
    pub volatile_state: VolatileState,
    pub leader_state: Option<LeaderState>,
    pub maintenance: Maintenance,
    pub id: NodeId,
    pub address: SocketAddrV4,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    VoteRequest {
        term: LogTerm,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: LogTerm,
    },
    VoteRequestResponse {
        term: LogTerm,
        vote_granted: bool,
    },
    AppendEntriesRequest {
        term: LogTerm,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: LogTerm,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    },
    AppendEntriesResponse {
        term: LogTerm,
        success: bool,
    },
    Heartbeat {
        term: LogTerm,
        node_id: NodeId,
    },
    HeartbeatResponse {
        success: bool,
        node_id: NodeId,
    },
}

#[derive(Debug, Copy, Clone)]
pub struct Peer {
    pub id: NodeId,
    pub address: SocketAddrV4,
}
