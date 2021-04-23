use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::panic::panic_any;
use std::sync::{Arc, Mutex};

use crate::constants::MESSAGE_LENGTH;
use crate::raft::types::{LogEntry, LogIndex, NodeId, RaftNode, Term};
use crate::rpc::types::{
    AppendEntriesRequest, AppendEntriesResponse, Heartbeat, HeartbeatResponse, Peer, RpcClient,
    RpcMessage, VoteRequest, VoteRequestResponse,
};

impl RpcClient {
    pub fn new(peers: &Vec<Peer>) -> Self {
        RpcClient {
            servers: peers
                .iter()
                .map(|p| (p.id.to_string(), TcpStream::connect(&p.address).unwrap()))
                .collect(),
        }
    }

    pub(crate) fn request_votes(
        &self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Vec<VoteRequestResponse> {
        let rpc_message = RpcMessage::VoteRequest(VoteRequest {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        });

        let rpc_responses = self.send(rpc_message);
        rpc_responses
            .into_iter()
            .map(|x| match x {
                RpcMessage::VoteRequestResponse(v) => v,
                _ => panic!(),
            })
            .collect()
    }

    fn append_entries(
        &self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit_index: LogIndex,
    ) -> Vec<AppendEntriesResponse> {
        let rpc_message = RpcMessage::AppendEntriesRequest(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit_index,
        });
        let rpc_responses = self.send(rpc_message);
        rpc_responses
            .into_iter()
            .map(|x| match x {
                RpcMessage::AppendEntriesResponse(v) => v,
                _ => panic!(),
            })
            .collect()
    }

    pub fn broadcast_heartbeat(&self, term: Term, server_id: NodeId) -> Vec<HeartbeatResponse> {
        let rpc_message = RpcMessage::Heartbeat(Heartbeat {
            term,
            node_id: server_id,
        });
        let rpc_responses = self.send(rpc_message);
        rpc_responses
            .into_iter()
            .map(|x| match x {
                RpcMessage::HeartbeatResponse(v) => v,
                _ => panic!(),
            })
            .collect()
    }

    fn send(&self, rpc_message: RpcMessage) -> Vec<RpcMessage> {
        let request_vote_bin = bincode::serialize(&rpc_message).unwrap();
        let mut rpc_responses: Vec<RpcMessage> = Vec::new();

        for mut stream in self.servers.values() {
            stream.write(&request_vote_bin).unwrap();

            let mut buffer = [0; MESSAGE_LENGTH];
            stream.read(&mut buffer).unwrap();
            rpc_responses.push(bincode::deserialize(&buffer).unwrap());
        }
        rpc_responses
    }
}
