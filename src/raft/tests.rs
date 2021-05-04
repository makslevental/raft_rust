use crate::raft::types::{LogEntry, LogTerm, NodeId, RaftNode, Role};
use simple_logger::SimpleLogger;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::iter::FromIterator;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::mpsc::Sender;
use std::sync::{Arc, LockResult, Mutex, MutexGuard};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const RAFT_ELECTION_GRACE_PERIOD: u64 = 1;
const REPEATS: usize = 10;
const NUM_SERVERS: usize = 10;

fn build_raft_nodes(
    num_servers: usize,
    MIN_QUORUM: usize,
    MIN_TIMEOUT: usize,
    MAX_TIMEOUT: usize,
) -> Vec<Arc<Mutex<RaftNode>>> {
    let node_ids = 0..num_servers;
    let nodes: Vec<(NodeId, SocketAddrV4)> = node_ids
        .clone()
        .map(|i| {
            (
                i as u64,
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, (3300 + i) as u16),
            )
        })
        .collect();
    let raft_nodes: Vec<Arc<Mutex<RaftNode>>> = node_ids
        .clone()
        .map(|i| {
            Arc::new(Mutex::new(RaftNode::new(
                i as u64,
                nodes[i as usize].1,
                nodes
                    .clone()
                    .into_iter()
                    .filter(|(pid, _address)| *pid != i as u64)
                    .collect(),
                MIN_QUORUM,
                MIN_TIMEOUT,
                MAX_TIMEOUT,
            )))
        })
        .collect();

    raft_nodes
}

fn check_no_leader(raft_nodes: &Vec<Arc<Mutex<RaftNode>>>) -> Result<(), String> {
    println!("check no leader {:?}", raft_nodes);
    // try a few times in case re-elections are needed.
    for i in 0..10 {
        thread::sleep(Duration::from_millis(500));
        let mut leaders: HashMap<LogTerm, Vec<NodeId>> = Default::default();
        for r in raft_nodes {
            let r = r.lock().unwrap();

            if r.role == Role::Leader {
                if !leaders.contains_key(&r.persistent_state.current_term) {
                    leaders.insert(r.persistent_state.current_term, vec![]);
                }
                leaders
                    .get_mut(&r.persistent_state.current_term)
                    .unwrap()
                    .push(r.id);
            }
        }

        let mut last_term_with_leader = 0;
        for (term, leaders) in &leaders {
            if leaders.len() > 1 {
                return Err(format!("term {:?} has (>1) leaders: {:?}", term, leaders));
            }
            last_term_with_leader = max(last_term_with_leader, *term);
        }
        if leaders.len() > 0 && leaders.get(&last_term_with_leader).unwrap().len() == 1 {
            return Err(format!(
                "found leader {:?}",
                *leaders
                    .get(&last_term_with_leader)
                    .unwrap()
                    .first()
                    .unwrap()
            ));
        }
    }
    return Ok(());
}

fn check_one_leader(raft_nodes: &Vec<Arc<Mutex<RaftNode>>>) -> Result<NodeId, String> {
    println!("check one leader {:?}", raft_nodes);
    // try a few times in case re-elections are needed.
    for i in 0..10 {
        thread::sleep(Duration::from_millis(500));
        let mut leaders: HashMap<LogTerm, Vec<NodeId>> = Default::default();
        for r in raft_nodes {
            let r = r.lock().unwrap();

            if r.role == Role::Leader {
                if !leaders.contains_key(&r.persistent_state.current_term) {
                    leaders.insert(r.persistent_state.current_term, vec![]);
                }
                leaders
                    .get_mut(&r.persistent_state.current_term)
                    .unwrap()
                    .push(r.id);
            }
        }

        let mut last_term_with_leader = 0;
        for (term, leaders) in &leaders {
            if leaders.len() > 1 {
                return Err(format!("term {:?} has (>1) leaders: {:?}", term, leaders));
            }
            last_term_with_leader = max(last_term_with_leader, *term);
        }
        if leaders.len() > 0 && leaders.get(&last_term_with_leader).unwrap().len() == 1 {
            return Ok(*leaders
                .get(&last_term_with_leader)
                .unwrap()
                .first()
                .unwrap());
        }
    }
    return Err("expected one leader, got none".to_string());
}

fn check_terms(raft_nodes: &Vec<Arc<Mutex<RaftNode>>>) -> Result<LogTerm, String> {
    let terms: HashSet<LogTerm> = HashSet::from_iter(
        raft_nodes
            .iter()
            .map(|r| r.lock().unwrap().persistent_state.current_term),
    );
    if terms.len() != 1 {
        return Err("expected one term, got many".to_string());
    }
    return Ok(*terms.iter().next().unwrap());
}

#[test]
fn test_ping_pong() {
    // SimpleLogger::new().init().unwrap();

    let raft_nodes = build_raft_nodes(NUM_SERVERS, 6, 150, 300);
    let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
        .iter()
        .map(|r| RaftNode::start_server(r.clone()))
        .collect();

    for raft_node in raft_nodes.iter() {
        raft_node.lock().unwrap().connect_to_peers();
    }
    // TODO: make sure to let all connections come online
    thread::sleep(Duration::from_secs(5));

    for raft_node in raft_nodes.iter() {
        raft_node.lock().unwrap().send_ping();
    }

    thread::sleep(Duration::from_secs(5));

    for raft_node in raft_nodes.iter() {
        let ping_ponged_peers: HashSet<u64> = HashSet::from_iter(
            raft_node.lock().unwrap().persistent_state.log[1..]
                .iter()
                .map(|le| le.message.parse().unwrap()),
        );
        let all_peers: HashSet<_> = HashSet::from_iter(
            raft_node
                .lock()
                .unwrap()
                .peers
                .iter()
                .map(|(pid, peer)| *pid),
        );
        assert_eq!(ping_ponged_peers, all_peers);
    }
}

#[test]
fn test_rigged_election() {
    SimpleLogger::new().init().unwrap();

    for num_servers in 3..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);
            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in &raft_nodes[1..] {
                let id = raft_node.lock().unwrap().id;
                raft_node.lock().unwrap().next_timeout = None;
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            println!("start tests with {:?} nodes", num_servers);

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            assert_eq!(leader.unwrap(), 0);

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
            let term1 = check_terms(&raft_nodes);
            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
            let term2 = check_terms(&raft_nodes);
            assert!(term2.is_ok());
            assert_eq!(term1, term2);

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            assert_eq!(leader.unwrap(), 0);

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn test_initial_election() {
    // SimpleLogger::new().init().unwrap();

    for num_servers in 3..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);

            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            println!("start tests with {:?} nodes", num_servers);

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let term1 = check_terms(&raft_nodes);
            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));
            let term2 = check_terms(&raft_nodes);
            assert!(term2.is_ok());
            assert_eq!(term1, term2);

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn test_reelection() {
    // SimpleLogger::new().init().unwrap();

    for num_servers in 5..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);

            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            println!("start tests with {:?} nodes", num_servers);

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            let original_leader_id = leader.unwrap();
            let new_leader;
            {
                let pause_node = raft_nodes.get(original_leader_id as usize).unwrap().lock();
                thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));
                drop(pause_node);
                new_leader = check_one_leader(
                    &raft_nodes
                        .iter()
                        .filter(|r| r.lock().unwrap().id != original_leader_id)
                        .map(|r| r.clone())
                        .collect(),
                );
                assert!(new_leader.is_ok(), format!("{:?}", new_leader));
                assert_ne!(new_leader.clone().unwrap(), original_leader_id);
            }
            // old leader returns
            let same_leader = check_one_leader(&raft_nodes);
            assert!(new_leader.is_ok(), format!("{:?}", new_leader));
            let new_leader_id = new_leader.unwrap();
            assert_ne!(new_leader_id, original_leader_id);

            // if there's no quorum, no leader should
            // be elected.
            {
                thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));
                let pause_node = raft_nodes.get(new_leader_id as usize).unwrap().lock();
                let maj_nodes: Vec<_> = if new_leader_id <= (majority - 1) as u64 {
                    (0..majority + 1)
                        .filter(|&i| i != new_leader_id as usize)
                        .collect()
                } else {
                    (0..majority).collect()
                };
                let locks: Vec<LockResult<MutexGuard<RaftNode>>> = maj_nodes
                    .iter()
                    .filter(|&&i| i != new_leader_id as usize)
                    .map(|i| raft_nodes.get(*i as usize).unwrap().lock())
                    .collect();
                thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));
                assert!(
                    check_no_leader(
                        &(0..num_servers)
                            .filter(|i| *i != new_leader_id as usize && !maj_nodes.contains(i))
                            .map(|i| raft_nodes.get(i).unwrap().clone())
                            .collect(),
                    )
                    .is_ok(),
                    "found some leader"
                );
            }
            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            // unfreeze nodes
            // if a quorum arises, it should elect a leader.
            let some_leader = check_one_leader(&raft_nodes);
            assert!(some_leader.is_ok(), format!("{:?}", some_leader));

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn test_basic_agree() {
    // SimpleLogger::new().init().unwrap();

    for num_servers in 3..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);

            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            println!("start tests with {:?} nodes", num_servers);

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            let original_leader_id = leader.unwrap();
            let mut leader = raft_nodes
                .get(original_leader_id as usize)
                .unwrap()
                .lock()
                .unwrap();
            let current_term = leader.persistent_state.current_term;
            leader.persistent_state.log.push(LogEntry {
                message: "testing1".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing2".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing3".to_string(),
                term: current_term,
            });
            let leader_log = leader.persistent_state.log.clone();
            let leader_state_machine = leader.persistent_state.state_machine.clone();
            let leader_commit_index = leader.volatile_state.commit_index;
            let leader_last_applied = leader.volatile_state.last_applied;
            drop(leader);
            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            for (i, raft_node) in raft_nodes.iter().enumerate() {
                assert_eq!(leader_log, raft_node.lock().unwrap().persistent_state.log);
                assert_eq!(
                    leader_state_machine,
                    raft_node.lock().unwrap().persistent_state.state_machine
                );
                assert_eq!(
                    leader_last_applied,
                    raft_node.lock().unwrap().volatile_state.last_applied
                );
                assert_eq!(
                    leader_commit_index,
                    raft_node.lock().unwrap().volatile_state.commit_index
                );
            }

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn test_overwrite() {
    // SimpleLogger::new().init().unwrap();

    for num_servers in 3..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);

            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            println!("start tests with {:?} nodes", num_servers);

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            let leader_id = leader.unwrap();
            let current_term = raft_nodes
                .get(leader_id as usize)
                .unwrap()
                .lock()
                .unwrap()
                .persistent_state
                .current_term;
            for raft_node in raft_nodes.iter() {
                let mut raft_node = raft_node.lock().unwrap();
                if raft_node.id == leader_id {
                    continue;
                }
                for i in 0..3 {
                    raft_node.persistent_state.log.push(LogEntry {
                        message: "WRONG".to_string(),
                        // TODO: hmm there's no double check if same index same term
                        // are the same entry. i guess that's because there can only be one
                        // leader per term?
                        // term: current_term,
                        term: 1,
                    })
                }
            }

            let mut leader = raft_nodes.get(leader_id as usize).unwrap().lock().unwrap();
            leader.persistent_state.log.push(LogEntry {
                message: "testing1".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing2".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing3".to_string(),
                term: current_term,
            });

            let leader_log = leader.persistent_state.log.clone();
            let leader_state_machine = leader.persistent_state.state_machine.clone();
            let leader_commit_index = leader.volatile_state.commit_index;
            let leader_last_applied = leader.volatile_state.last_applied;
            drop(leader);
            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            for (i, raft_node) in raft_nodes.iter().enumerate() {
                assert_eq!(leader_log, raft_node.lock().unwrap().persistent_state.log);
                assert_eq!(
                    leader_state_machine,
                    raft_node.lock().unwrap().persistent_state.state_machine
                );
                assert_eq!(
                    leader_last_applied,
                    raft_node.lock().unwrap().volatile_state.last_applied
                );
                assert_eq!(
                    leader_commit_index,
                    raft_node.lock().unwrap().volatile_state.commit_index
                );
            }

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[test]
fn test_fail_agree() {
    // SimpleLogger::new().init().unwrap();

    for num_servers in 3..=NUM_SERVERS {
        for _ in 0..REPEATS {
            let majority = if num_servers % 2 == 1 {
                (num_servers + 1) / 2
            } else {
                num_servers / 2 + 1
            };
            let raft_nodes = build_raft_nodes(num_servers, majority, 150, 300);

            let server_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_server(r.clone()))
                .collect();

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            for raft_node in raft_nodes.iter() {
                raft_node.lock().unwrap().connect_to_peers();
            }

            thread::sleep(Duration::new(RAFT_ELECTION_GRACE_PERIOD, 0));

            let background_handles: Vec<(JoinHandle<()>, Sender<()>)> = raft_nodes
                .iter()
                .map(|r| RaftNode::start_background_tasks(r.clone()))
                .collect();

            println!("start tests with {:?} nodes", num_servers);

            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            let leader = check_one_leader(&raft_nodes);
            assert!(leader.is_ok(), format!("{:?}", leader));
            let leader_id = leader.unwrap();
            let current_term = raft_nodes
                .get(leader_id as usize)
                .unwrap()
                .lock()
                .unwrap()
                .persistent_state
                .current_term;
            for raft_node in raft_nodes.iter() {
                let mut raft_node = raft_node.lock().unwrap();
                if raft_node.id == leader_id {
                    continue;
                }
                for i in 0..3 {
                    raft_node.persistent_state.log.push(LogEntry {
                        message: "WRONG".to_string(),
                        // TODO: hmm there's no double check if same index same term
                        // are the same entry. i guess that's because there can only be one
                        // leader per term?
                        // term: current_term,
                        term: 1,
                    })
                }
            }

            let mut leader = raft_nodes.get(leader_id as usize).unwrap().lock().unwrap();
            leader.persistent_state.log.push(LogEntry {
                message: "testing1".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing2".to_string(),
                term: current_term,
            });

            leader.persistent_state.log.push(LogEntry {
                message: "testing3".to_string(),
                term: current_term,
            });
            let leader_log = leader.persistent_state.log.clone();
            drop(leader);
            thread::sleep(Duration::new(2 * RAFT_ELECTION_GRACE_PERIOD, 0));

            for (i, raft_node) in raft_nodes.iter().enumerate() {
                assert_eq!(leader_log, raft_node.lock().unwrap().persistent_state.log)
            }

            println!("tests with {:?} nodes successfully completed", num_servers);

            for (handle, tx) in background_handles {
                tx.send(());
                handle.join().unwrap();
            }

            for (handle, tx) in server_handles {
                tx.send(());
                handle.join().unwrap();
            }

            thread::sleep(Duration::from_millis(10));
        }
    }
}
