use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use log::info;

use crate::constants::MESSAGE_LENGTH;
use crate::raft::types::{Message, RaftNode};
use crate::rpc::types::RpcServer;

impl RpcServer {
    pub fn new(raft_node: Arc<Mutex<RaftNode>>, address: SocketAddrV4) -> Self {
        RpcServer { raft_node, address }
    }

    pub fn start(&self) {
        info!("Starting server at: {}...", self.address);
        let listener = TcpListener::bind(self.address).unwrap();

        for stream in listener.incoming() {
            let node_clone = Arc::clone(&self.raft_node);

            match stream {
                Ok(stream) => {
                    thread::spawn(move || handle_connection(node_clone, stream));
                }
                Err(e) => {
                    info!("Error while listening to client: {}", e);
                }
            }
        }
    }
}

fn handle_connection(raft_node: Arc<Mutex<RaftNode>>, mut stream: TcpStream) {
    loop {
        let mut buffer = [0; MESSAGE_LENGTH];
        stream.read(&mut buffer).unwrap();

        let deserialized: Message = bincode::deserialize(&buffer).unwrap();

        let response = match deserialized {
            // TODO missing other fields from voterequets
            Message::VoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let (term, vote_granted) = raft_node.lock().unwrap().handle_vote_request(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                );
                bincode::serialize(&Message::VoteRequestResponse { term, vote_granted }).unwrap()
            }
            Message::AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                let (term, success) = raft_node.lock().unwrap().handele_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                );
                bincode::serialize(&Message::AppendEntriesResponse { term, success }).unwrap()
            }
            Message::Heartbeat { term, node_id } => {
                let (success, node_id) = raft_node.lock().unwrap().handle_heartbeat(term, node_id);
                bincode::serialize(&Message::HeartbeatResponse { success, node_id }).unwrap()
            }
            _ => todo!(), // Response messages;
        };

        stream.write(&response).unwrap();
        stream.flush().unwrap();
    }
}

// fn handle_request_votes(server: Arc<Mutex<RaftNode>>, request: VoteRequest) -> Vec<u8> {
//     let term = crate::raft::core::handle_log_entry(
//         server,
//         LogEntry::Heartbeat {
//             term: term,
//             peer_id: peer_id.to_string(),
//         },
//     );
//
//     let response = RpcMessage::HeartbeatResponse {
//         term: term,
//         peer_id: peer_id,
//     };
//
//     bincode::serialize(&response).unwrap()
// }
//
// fn handle_vote_request(server: Arc<Mutex<RaftNode>>, term: u64, candidate_id: String) -> Vec<u8> {
//     let response = crate::raft::core::handle_vote_request(
//         server,
//         VoteRequest {
//             term: term,
//             candidate_id: candidate_id,
//         },
//     );
//
//     let response = RpcMessage::VoteResponse {
//         term: response.term,
//         vote_granted: response.vote_granted,
//     };
//
//     bincode::serialize(&response).unwrap()
// }
