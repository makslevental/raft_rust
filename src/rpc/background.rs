use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use log::info;

use rand::Rng;

use crate::constants::MIN_QUORUM;
use crate::raft::types::{RaftNode, Role};
use crate::rpc::types::RpcClient;

// pub fn background_task(raft_node: Arc<Mutex<RaftNode>>, rpc_client: &RpcClient) {
//     loop {
//         broadcast_heartbeat(Arc::clone(&raft_node), rpc_client);
//         handle_timeout(Arc::clone(&raft_node), rpc_client);
//     }
// }
