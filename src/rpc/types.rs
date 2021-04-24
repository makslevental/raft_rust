use std::collections::HashMap;
use std::net::{SocketAddrV4, TcpStream};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::raft::types::{LogEntry, LogIndex, LogTerm, NodeId, RaftNode};

#[derive(Debug)]
pub struct RpcClient {
    pub servers: HashMap<String, TcpStream>,
}

pub struct RpcServer {
    pub raft_node: Arc<Mutex<RaftNode>>,
    pub address: SocketAddrV4,
}
