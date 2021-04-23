#[macro_use]
extern crate lazy_static;

use crate::constants::NUM_SERVERS;
use crate::raft::types::RaftNode;
use crate::rpc::types::{Peer, RpcClient, RpcServer};
use std::borrow::Borrow;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};
use std::thread;

mod constants;
mod raft;
mod rpc;

pub fn start_node(raft_node: Arc<Mutex<RaftNode>>, rpc_client: RpcClient) {
    raft_node.lock().unwrap().refresh_timeout();

    let background_task_handle = thread::spawn(move || {
        rpc::background::background_task(raft_node, &rpc_client);
    });

    background_task_handle.join().unwrap();
}

fn main() {
    let node_ids = (0..NUM_SERVERS);
    let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
        .clone()
        .map(|i| Arc::new(Mutex::new(RaftNode::new(i as u64))))
        .collect();
    let peers: Vec<Peer> = node_ids
        .clone()
        .map(|i| Peer {
            id: i as u64,
            address: SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16),
        })
        .collect();
    let rpc_servers: Vec<RpcServer> = node_ids
        .clone()
        .map(|i| {
            RpcServer::new(
                Arc::clone(&raft_nodes[i as usize]),
                peers[i as usize].address.clone(),
            )
        })
        .collect();

    let mut server_threads = Vec::new();
    for rpc_server in rpc_servers {
        server_threads.push(thread::spawn(move || {
            rpc_server.start();
        }));
    }

    let mut rpc_clients: Vec<RpcClient> = node_ids
        .clone()
        .map(|i| {
            let l_peers: Vec<Peer> = peers
                .clone()
                .into_iter()
                .filter(|p| p.id != i as u64)
                .collect();
            println!("{:?}", i);
            println!("{:?}", peers);
            RpcClient::new(&l_peers)
        })
        .collect();

    node_ids.clone().map(|i| {
        start_node(
            Arc::clone(&raft_nodes[i as usize]),
            rpc_clients.remove(i as usize),
        )
    });
}
