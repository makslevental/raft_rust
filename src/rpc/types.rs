use serde::{Deserialize, Serialize};

use crate::raft::types::{LogEntry, LogIndex, NodeId, RaftNode, Term};
use std::collections::HashMap;
use std::net::{SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Debug)]
pub enum RpcMessage {
    VoteRequest(VoteRequest),
    VoteRequestResponse(VoteRequestResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    Heartbeat(Heartbeat),
    HeartbeatResponse(HeartbeatResponse),
}

#[derive(Debug)]
pub struct RpcClient {
    pub servers: HashMap<String, TcpStream>,
}

pub struct RpcServer {
    pub raft_node: Arc<Mutex<RaftNode>>,
    pub address: SocketAddrV4,
}

#[derive(Debug, Copy, Clone)]
pub struct Peer {
    pub id: NodeId,
    pub address: SocketAddrV4,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequestResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit_index: LogIndex,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Heartbeat {
    pub term: Term,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HeartbeatResponse {
    pub success: bool,
    pub node_id: NodeId,
}
