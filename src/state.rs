use crate::raftlog::{Command, Log};
use std::{collections::HashMap, time::Instant};

pub type Id = u64;

// volatile state on candidates
// reinitialised when election is started
#[derive(Debug)]
pub struct Candidate {
    pub votes_received: u64,
}

// volatile state on leaders
// reinitialised after each election
#[derive(Debug)]
pub struct Leader {
    pub next_indices: HashMap<Id, u64>,
    pub match_indices: HashMap<Id, u64>,
}

#[derive(Debug)]
pub enum OperationMode {
    Follower,
    Candidate(Candidate),
    Leader(Leader),
    Dead,
}

// we want to only compare the mode and not the volatile state
impl PartialEq for OperationMode {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (OperationMode::Follower, OperationMode::Follower)
            | (OperationMode::Candidate(_), OperationMode::Candidate(_))
            | (OperationMode::Leader(_), OperationMode::Leader(_))
            | (OperationMode::Dead, OperationMode::Dead) => true,
            _ => false,
        }
    }
}

impl Eq for OperationMode {}

pub struct State<T: Command> {
    // persistent state
    // updated on stable storage before responding to RPCs
    pub cur_term: u64,
    pub voted_for: Option<Id>,
    pub log: Log<T>,

    // volatile state
    pub opmode: OperationMode,
    pub commit_index: u64,
    pub last_applied: u64,
    pub election_reset_event: Instant,
}

impl<T: Command> State<T> {
    pub fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
            log: Log::new(),

            opmode: OperationMode::Follower,
            // TODO: think about this, what should be the initial value?
            // Does it even matter?
            commit_index: 0,
            last_applied: 0,
            election_reset_event: Instant::now(),
        }
    }
}
