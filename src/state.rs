use crate::raftlog::Log;
use std::{collections::HashMap, time::Instant};

#[derive(Debug, PartialEq, Eq)]
pub enum OperationMode {
    Follower,
    Candidate,
    Leader,
    Dead,
}

pub type Id = u64;

pub struct State<T: Clone> {
    // persistent state
    pub cur_term: u64,
    pub voted_for: Option<Id>,
    pub log: Log<T>,

    // volatile state
    pub opmode: OperationMode,
    pub commit_index: u64,
    pub last_applied: u64,
    pub election_reset_event: Instant,

    // TODO: as they are opmode specific, make them part of `OperationMode` enum

    // volatile state on leaders
    // reinitialised after each election
    pub next_index: HashMap<Id, u64>,
    pub match_index: HashMap<Id, u64>,

    // volatile state on candidates
    // reinitialised when election is started
    pub votes_received: u64,
}

impl<T: Default + Clone> State<T> {
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

            next_index: HashMap::new(),
            match_index: HashMap::new(),

            votes_received: 0,
        }
    }
}
