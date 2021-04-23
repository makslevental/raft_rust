use std::convert::TryInto;
use std::net::SocketAddrV4;
use std::time::{Duration, Instant};

use log::info;
use rand::prelude::ThreadRng;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::constants::{MAX_TIMEOUT, MIN_TIMEOUT, NUM_SERVERS};

pub type Term = u64;
pub type NodeId = u64;
pub type LogEntry = String;
pub type LogIndex = u32;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Role {
    Follower,
    Leader,
    Candidate,
}

#[derive(Debug)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
}

#[derive(Debug)]
pub struct RaftNode {
    pub state: PersistentState,

    pub commit_index: Vec<LogIndex>,
    pub last_applied: Vec<LogIndex>,

    pub next_index: [LogIndex; NUM_SERVERS],
    pub match_index: [LogIndex; NUM_SERVERS],

    pub id: NodeId,
    pub current_leader: Option<NodeId>,
    pub role: Role,
    pub next_timeout: Option<Instant>,
}
