#![allow(warnings)]

#[macro_use]
extern crate lazy_static;

use crate::constants::NUM_SERVERS;
use crate::raft::types::{NodeId, RaftNode};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};

mod constants;
mod raft;

fn main() {
    // let node_ids = 0..NUM_SERVERS;
    // let nodes: Vec<(NodeId, SocketAddrV4)> = node_ids
    //     .clone()
    //     .map(|i| (i, SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16)))
    //     .collect();
    // let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
    //     .clone()
    //     .map(|i| {
    //         Arc::new(Mutex::new(RaftNode::new(
    //             i,
    //             nodes[i as usize].1,
    //             nodes
    //                 .clone()
    //                 .into_iter()
    //                 .filter(|(pid, _address)| *pid != i)
    //                 .collect(),
    //         )))
    //     })
    //     .collect();
    //
    // raft_nodes
    //     .iter()
    //     .for_each(|r| RaftNode::start_server(r.clone()));
    //
    // raft_nodes
    //     .iter()
    //     .for_each(|r| r.clone().lock().unwrap().connect_to_peers());
    //
    // raft_nodes
    //     .iter()
    //     .for_each(|r| RaftNode::start_background_tasks(r.clone()));
}
