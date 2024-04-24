use crate::raftlog::Log;
use std::time::Instant;

#[derive(Debug, PartialEq, Eq)]
pub enum OperationMode {
    Follower,
    Candidate,
    Leader,
    Dead,
}

pub type Id = u64;

pub struct State<T> {
    // persistent
    pub cur_term: u64,
    pub voted_for: Option<Id>,
    pub log: Log<T>,

    // volatile
    pub opmode: OperationMode,
    pub election_reset_event: Instant,

    pub votes_received: u64,
}

impl<T> State<T> {
    pub fn new() -> Self {
        Self {
            cur_term: 0,
            voted_for: None,
            log: Log::new(),

            opmode: OperationMode::Follower,
            // TODO: think about this, what should be the initial value?
            // Does it even matter?
            election_reset_event: Instant::now(),

            votes_received: 0,
        }
    }
}
