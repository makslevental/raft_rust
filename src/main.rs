#[macro_use]
extern crate lazy_static;

use crate::constants::NUM_SERVERS;
use crate::raft::types::{Peer, RaftNode};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};

mod constants;
mod raft;

// pub fn start_node(raft_node: Arc<Mutex<RaftNode>>, rpc_client: RpcClient) {
//     raft_node.lock().unwrap().refresh_timeout();
//
//     let background_task_handle = thread::spawn(move || {
//         rpc::background::background_task(raft_node, &rpc_client);
//     });
//
//     background_task_handle.join().unwrap();
// }

fn main() {
    println!("hello world");

    // &peers
    //     .clone()
    //     .into_iter()
    //     .filter(|p| p.id != i as u64)
    //     .collect(),
    // raft_nodes.into_iter().for_each(|r| RaftNode::start(r))
}
