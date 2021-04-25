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
    let node_ids = 0..NUM_SERVERS;
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
                &peers
                    .clone()
                    .into_iter()
                    .filter(|p| p.id != i as u64)
                    .collect(),
            )))
        })
        .collect();

    raft_nodes.into_iter().for_each(|r| RaftNode::start(r))
}
