use crate::{
    persistence::Persistence,
    raftlog::{Command, Entry},
    rpc,
    state::{Candidate, Id, Leader, OperationMode, State},
};
use log::trace;
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex, MutexGuard},
    task::JoinHandle,
    time::{interval, timeout},
};

pub struct RaftConsensusInner<T: Command, P: Persistence> {
    pub id: Id,
    pub peers: Mutex<HashMap<Id, Option<rpc::RaftClient<tonic::transport::Channel>>>>,
    pub state: Mutex<State<T>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    send_entries_to_client_service: Mutex<mpsc::Sender<CommitEntry<T>>>,
    pub(crate) commit_entries_ready: Mutex<mpsc::Sender<bool>>,
    pub(crate) trigger_append_entries: Mutex<Option<mpsc::Sender<bool>>>,
    pub(crate) persistence: P,
}

pub struct RaftConsensus<T: Command, P: Persistence>(Arc<RaftConsensusInner<T, P>>);

impl<T: Command, P: Persistence> Deref for RaftConsensus<T, P> {
    type Target = RaftConsensusInner<T, P>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Command, P: Persistence> RaftConsensus<T, P> {
    pub async fn new(
        id: Id,
        mut ready: broadcast::Receiver<()>,
        send_entries_to_client_service: mpsc::Sender<CommitEntry<T>>,
        persistence: P,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (commit_entries_ready_tx, commit_entries_ready_rx) = mpsc::channel(16);
        let should_restore_persistent_data = persistence.has_data().await;

        let inner = RaftConsensusInner {
            id,
            peers: Mutex::new(HashMap::new()),
            state: Mutex::new(State::<T>::new()),
            shutdown: Mutex::new(Some(shutdown_tx)),
            send_entries_to_client_service: Mutex::new(send_entries_to_client_service),
            commit_entries_ready: Mutex::new(commit_entries_ready_tx),
            trigger_append_entries: Mutex::new(None),
            persistence,
        };

        let raft = {
            let raft = Self(Arc::new(inner));
            if should_restore_persistent_data {
                raft.restore_persistent_data().await;
            }
            raft
        };

        trace!(
            "[{}] new server log: {:?}",
            raft.id,
            raft.state.lock().await.log
        );

        let raft2 = raft.clone();
        tokio::spawn(async move {
            let dst = raft2.get_listen_addr(false);
            raft2.start_rpc_server(dst, shutdown_rx).await;
        });

        let raft3 = raft.clone();
        tokio::spawn(async move {
            ready.recv().await.unwrap();
            {
                // ensure lock is dropped before `run_election_timer` is called
                // TODO: required?
                raft3.state.lock().await.election_reset_event = Instant::now();
            }
            raft3.run_election_timer().await;
        });

        let raft4 = raft.clone();
        tokio::spawn(async move {
            raft4
                .send_commands_to_client_service(commit_entries_ready_rx)
                .await;
        });

        raft
    }

    pub fn get_listen_addr(&self, proto: bool) -> String {
        let proto = if proto { "http://" } else { "" };
        format!("{}127.0.0.1:{}", proto, crate::PORT_BASE + self.id)
    }

    fn clone(&self) -> Self {
        let raft = Arc::clone(&self.0);
        Self(raft)
    }

    // specific to a (term, opmode)
    // if any of them is changed, then this instance of `run_election_timer` comes to an end
    async fn run_election_timer(&self) {
        let timeout_duration = self.election_timeout();
        let term_started = self.state.lock().await.cur_term;
        trace!(
            "[{}] election timer started {:?}, term {}",
            self.id,
            timeout_duration,
            term_started
        );

        let mut interval = interval(Duration::from_millis(10));
        loop {
            interval.tick().await;

            let state = self.state.lock().await;

            if !matches!(
                state.opmode,
                OperationMode::Candidate(_) | OperationMode::Follower
            ) {
                trace!(
                    "[{}] election timer state: {:?} bailing out!",
                    self.id,
                    state.opmode
                );
                return;
            }

            if term_started != state.cur_term {
                trace!(
                    "[{}] election timer term changed from {} to {}",
                    self.id,
                    term_started,
                    state.cur_term
                );
                return;
            }

            if state.election_reset_event.elapsed() > timeout_duration {
                // start election
                break;
            }
        }

        // guard is created within the loop scope and dropped by the time we get here
        self.start_election().await;
    }

    fn election_timeout(&self) -> Duration {
        let random = if std::env::var(crate::RAFT_FORCE_MORE_REELECTION).is_ok() {
            0
        } else {
            crate::random(0, 150)
        };
        Duration::from_millis(150 + random)
    }

    async fn start_election(&self) {
        let mut state = self.state.lock().await;
        // move to next term before starting election
        state.cur_term += 1;
        state.election_reset_event = Instant::now();
        // vote for yourself
        state.opmode = OperationMode::Candidate(Candidate { votes_received: 1 });
        state.voted_for = Some(self.id);

        trace!(
            "[{}] becomes candidate (term = {}, log: {:?})",
            self.id,
            state.cur_term,
            state.log
        );

        let args = rpc::RequestVoteArgs {
            term: state.cur_term,
            candidate_id: self.id,
            last_log_index: state.log.get_last_log_index(),
            last_log_term: state.log.get_last_log_term(),
        };

        // send RequestVote RPCs to all other servers concurrently.
        for peer in self.peers.lock().await.keys() {
            spawn_call_request_vote(self.clone(), *peer, args.clone());
        }

        // starts another election timer in the background
        // that is used to start another round of election if there is no winner (no server has received a majority of votes)
        // possibly split votes
        // or this server is isolated from the rest of the cluster
        spawn_run_election_timer(self.clone());
    }

    async fn start_leader(&self, state: &mut MutexGuard<'_, State<T>>) {
        let (trigger_append_entries_tx, trigger_append_entries_rx) = mpsc::channel(1);
        // state set to `Leader`
        state.opmode = {
            let mut next_indices = HashMap::new();
            let mut match_indices = HashMap::new();

            // number of actual entries in the log + 1 (dummy entry added at the beginning)
            // works as intended as we use 1-based indexing
            let log_len = state.log.len();

            let peers = self.peers.lock().await;
            for peer in peers.keys() {
                // 1-based indexing
                // 0 is used as the sentinel value (represents no entry in the log)

                // `next_index` points to the next entry to be sent to the follower
                // which is optimistically set to 1 more than the index of last entry
                // decreased initially until we find the actual number of entries replicated on the follower
                next_indices.insert(*peer, log_len);

                // `match_index` is the latest log entry that is KNOWN to be replicated on the follower
                // set pesimistically to 0
                // increased monotonically as the leader sends `AppendEntries` RPC requests
                match_indices.insert(*peer, 0);
            }

            trace!(
                "[{}] becomes leader; term={}, next_index: {:?}, match_index: {:?}, log={:?}",
                self.id,
                state.cur_term,
                next_indices,
                match_indices,
                state.log
            );

            OperationMode::Leader(Leader {
                next_indices,
                match_indices,
            })
        };

        *self.trigger_append_entries.lock().await = Some(trigger_append_entries_tx);

        let raft = self.clone();
        tokio::spawn(async move {
            raft.trigger_append_entries_task(trigger_append_entries_rx, Duration::from_millis(50))
                .await;
        });
    }

    async fn send_append_entries(&self) {
        let state = self.state.lock().await;

        // send append_entries only if `Leader`
        if let OperationMode::Leader(Leader { next_indices, .. }) = &state.opmode {
            for peer in self.peers.lock().await.keys() {
                // prepare AppendEntries request
                // different for each follower, which depends on the `Leader`s view of the extent to which log is replicated on the follower

                // `Leader`s idea of the next entry to be sent to the follower (may or may not be true)
                trace!(
                    "[{}] send_append_entries: peer: {}, next_index: {:#?}",
                    self.id,
                    peer,
                    next_indices,
                );
                let next_index = *next_indices.get(peer).unwrap();

                // `Leader`s idea of the latest entry that is successfully replicated on the follower (may or may not be true)
                let prev_log_index = next_index - 1;
                let prev_log_term = state.log.get_log_term(prev_log_index);

                // `Leader`s idea of the new entries to be replicated on the follower (may or may not be true)
                let entries = state
                    .log
                    .get_entries_from(next_index)
                    .into_iter()
                    .map(|e| e.into())
                    .collect();

                let args = rpc::AppendEntriesArgs {
                    term: state.cur_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: state.commit_index,
                };

                spawn_send_append_entries(self.clone(), *peer, next_index, args);
            }
        }
    }

    pub async fn become_follower(&self, state: &mut MutexGuard<'_, State<T>>, new_term: u64) {
        trace!(
            "[{}] becomes Follower with term={}; log={:?}",
            self.id,
            new_term,
            state.log
        );

        state.opmode = OperationMode::Follower;
        state.cur_term = new_term;
        state.voted_for = None;
        state.election_reset_event = Instant::now();

        let raft = self.clone();
        tokio::spawn(async move {
            raft.run_election_timer().await;
        });
    }

    pub async fn trigger_append_entries_task(
        &self,
        mut trigger_append_entries_rx: mpsc::Receiver<bool>,
        heartbeat_timeout: Duration,
    ) {
        self.send_append_entries().await;

        loop {
            if let Ok(Some(false)) =
                timeout(heartbeat_timeout, trigger_append_entries_rx.recv()).await
            {
                return;
            }

            {
                let state = self.state.lock().await;
                if !matches!(state.opmode, OperationMode::Leader(_)) {
                    *self.trigger_append_entries.lock().await = None;
                    return;
                }
            }

            self.send_append_entries().await;
        }
    }
}

// Workaround for tokio "cycle detected"
fn spawn_call_request_vote<T: Command, P: Persistence>(
    raft: RaftConsensus<T, P>,
    peer: Id,
    args: rpc::RequestVoteArgs,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        trace!("[{}] sending RequestVote to {}: {:#?}", raft.id, peer, args);

        if let Ok(reply) = raft.call_request_vote(peer, args).await {
            trace!(
                "[{}] received RequestVoteReply from {}: {:#?}",
                raft.id,
                peer,
                reply
            );

            let mut state = raft.state.lock().await;

            // get around borrow checker's to identify disjoint borrows with `MutexGuard`
            let cur_term = state.cur_term;

            if let OperationMode::Candidate(Candidate { votes_received }) = &mut state.opmode {
                match reply.term.cmp(&cur_term) {
                    core::cmp::Ordering::Greater => {
                        trace!("[{}] term out of date in RequestVoteReply", raft.id);
                        raft.become_follower(&mut state, reply.term).await;
                    }
                    core::cmp::Ordering::Equal => {
                        if reply.vote_granted {
                            *votes_received += 1;

                            let peers_count = raft.peers.lock().await.len() as u64;
                            if 2 * *votes_received > peers_count + 1 {
                                // received a majority of votes
                                trace!("[{}] wins election with {} votes", raft.id, votes_received);
                                raft.start_leader(&mut state).await;
                            }
                        }
                    }
                    // the receiver updates its term before sending the reply if its term was previously lower
                    // so receiving a response from previous term is to be ignored
                    core::cmp::Ordering::Less => {}
                }
            } else {
                trace!(
                    "[{}] while waiting for reply, state = {:?}",
                    raft.id,
                    state.opmode
                );
            }
        }
    })
}

fn spawn_send_append_entries<T: Command, P: Persistence>(
    raft: RaftConsensus<T, P>,
    peer: Id,
    next_index: u64,
    args: rpc::AppendEntriesArgs,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        trace!(
            "[{}] sending AppendEntries to {}: ni={}, args={:#?}",
            raft.id,
            peer,
            next_index,
            args
        );

        let entries_len = args.entries.len() as u64;

        if let Ok(reply) = raft.call_append_entries(peer, args).await {
            let mut state = raft.state.lock().await;

            // not the actual `Leader`, revert back to being a follower and update the term
            if reply.term > state.cur_term {
                trace!("[{}] term out of date in heartbeat reply", raft.id);
                raft.become_follower(&mut state, reply.term).await;
                return;
            }

            // get around borrow checker's to identify disjoint borrows with `MutexGuard`
            let state2 = state.deref_mut();

            // identify if the RPC response received is for the current term
            if let OperationMode::Leader(Leader {
                next_indices,
                match_indices,
            }) = &mut state2.opmode
            {
                if reply.success && reply.term == state2.cur_term {
                    // `reply.success == true` shows that all the sent log entries are now part of the follower's log
                    // so, we can increase the (previous) `next_index` corresponding to this follower by the number of log entries in the sent request
                    next_indices.insert(peer, next_index + entries_len);
                    // `match_index` is 1 less than `next_index` by definition
                    // `match_index` shows the max entry that is known to be successfully replicated on the follower
                    // whereas, `next_index` points to the next entry to be sent
                    match_indices.insert(peer, next_index + entries_len - 1);
                    trace!("[{}] AppendEntries reply from {} success: nextIndex := {:?}, matchIndex := {:?}", raft.id, peer, next_indices, match_indices);

                    let saved_commit_index = state2.commit_index;
                    // update `commit_index` of `Leader`
                    // find the index of log entry that is successfully replicated on a MAJORITY of servers
                    // the updated `commit_index` is propagated to the followers when the next set of `AppendEntries` are sent
                    for index in state2.commit_index + 1..state2.log.len() {
                        // raft never commits log entries from previous terms by counting replicas
                        // only log entries from the leader’s current term are committed by counting replicas
                        if state2.log.get_log_term(index) == state2.cur_term {
                            // `match_count` and `peers_count` don't include `Leader`
                            let match_count =
                                match_indices.values().filter(|v| **v >= index).count();
                            let peers_count = raft.peers.lock().await.len();

                            // majority is atleast ceil(total number of servers / 2)
                            // => majority is an integer greater than (total number of servers / 2)
                            // => 2 * majority is greater than total number of servers
                            // peers_count + 1 is the total number of servers in the cluster (including `Leader`)

                            // match_count only counts the peers where the command has been replicated (excluding Leader)
                            // But the Leader the propagates the command only after adding it to its own log
                            // match_count + 1 is the total number of servers where the command has been successfully replicated (including `Leader`)
                            if 2 * (match_count + 1) > peers_count + 1 {
                                state2.commit_index = index;
                            }
                        }
                    }
                    if state2.commit_index != saved_commit_index {
                        trace!(
                            "[{}] leader sets commitIndex := {}",
                            raft.id,
                            state2.commit_index
                        );

                        drop(state);

                        // signal change in `commit_index`
                        let tx = raft.commit_entries_ready.lock().await;
                        tx.send(true).await.unwrap();

                        // propagate this to rest of the cluster
                        let tx = raft.trigger_append_entries.lock().await;
                        let tx = tx.as_ref().unwrap();
                        tx.send(true).await.unwrap();
                    }
                } else {
                    // prepare for the next round
                    // decrement `next_index` by 1
                    next_indices.insert(peer, next_index - 1);
                    trace!(
                        "[{}] AppendEntries reply from {} !success: nextIndex := {}",
                        raft.id,
                        peer,
                        next_index - 1
                    );
                }
            }
        }
    })
}

// Workaround for tokio "cycle detected"
fn spawn_run_election_timer<T: Command, P: Persistence>(raft: RaftConsensus<T, P>) {
    tokio::spawn(async move {
        raft.run_election_timer().await;
    });
}

// Functions used by tests
impl<T: Command, P: Persistence> RaftConsensus<T, P> {
    pub async fn report(&self) -> (Id, u64, bool) {
        let state = self.state.lock().await;
        (
            self.id,
            state.cur_term,
            matches!(state.opmode, OperationMode::Leader(_)),
        )
    }

    pub async fn stop(&self) {
        let mut state = self.state.lock().await;
        state.opmode = OperationMode::Dead;
        trace!("[{}] becomes Dead", self.id);
        let tx = self.commit_entries_ready.lock().await;
        tx.send(false).await.unwrap();
    }

    pub async fn shutdown(&self) {
        self.stop().await;
        // stop the tonic RPC server
        self.shutdown.lock().await.take().unwrap().send(()).unwrap();
    }

    pub async fn connect_peer(&self, id: Id, connect_string: String) -> Result<(), tonic::Status> {
        let mut peers = self.peers.lock().await;

        match peers.get(&id) {
            None | Some(None) => {
                let client = rpc::get_client(connect_string).await?;
                peers.insert(id, Some(client));
            }
            _ => {}
        }

        // let mut state = self.state.lock().await;
        // if state.opmode == OperationMode::Leader {
        //     let log_len = state.log.len();
        //     if !state.next_index.contains_key(&id) {
        //         state.next_index.insert(id, log_len);
        //     }
        //     if !state.match_index.contains_key(&id) {
        //         state.match_index.insert(id, 0);
        //     }
        // }

        Ok(())
    }

    pub async fn disconnect_peer(&self, id: Id) {
        let mut peers = self.peers.lock().await;
        let peer = peers.get_mut(&id).unwrap();
        *peer = None;
        // connection closed automatically when client is dropped
    }

    pub async fn disconnect_all_peers(&self) {
        let mut peers = self.peers.lock().await;
        for value in peers.values_mut() {
            *value = None;
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct CommitEntry<T> {
    pub command: T,
    pub index: u64,
    pub term: u64,
}

// Don't compare the term
// all we care is that the COMMITTED log commands are the same and are in the same order
// entries are committed in different terms for different servers depending on their connectedness
// `term` is present here mainly for debugging purposes
impl<T: PartialEq> PartialEq for CommitEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.command.eq(&other.command) && self.index.eq(&other.index)
    }
}

// functions used for communication with the client service using Raft
impl<T: Command, P: Persistence> RaftConsensus<T, P> {
    pub async fn submit(&self, command: T) -> bool {
        let mut state = self.state.lock().await;
        trace!(
            "[{}] submit received by {:?}: {:?}",
            self.id,
            state.opmode,
            command
        );

        self.store_persistent_data(state.cur_term, state.voted_for, state.log.clone())
            .await;

        if matches!(state.opmode, OperationMode::Leader(_)) {
            let entry = Entry::new(command, state.cur_term);
            state.log.add_new_entry(entry);
            trace!("[{}] ... log={:?}", self.id, state.log);
            drop(state);

            let tx = self.trigger_append_entries.lock().await;
            let tx = tx.as_ref().unwrap();
            tx.send(true).await.unwrap();

            return true;
        }

        false
    }

    async fn send_commands_to_client_service(
        &self,
        mut commit_entries_ready: mpsc::Receiver<bool>,
    ) {
        while let Some(true) = commit_entries_ready.recv().await {
            // find entries to send
            let (entries, saved_term, saved_last_applied) = {
                let mut state = self.state.lock().await;
                let saved_term = state.cur_term;
                let saved_last_applied = state.last_applied;
                let entries = if state.commit_index > state.last_applied {
                    let entries = state
                        .log
                        .get_entries_in_range(state.last_applied + 1, state.commit_index + 1);
                    state.last_applied = state.commit_index;
                    entries
                } else {
                    vec![]
                };
                (entries, saved_term, saved_last_applied)
            };

            trace!(
                "[{}] commitChanSender entries={:?}, savedLastApplied={}",
                self.id,
                entries,
                saved_last_applied,
            );

            for (i, entry) in entries.into_iter().enumerate() {
                let commit_entry = CommitEntry {
                    command: entry.command,
                    index: saved_last_applied + i as u64 + 1,
                    term: saved_term,
                };
                let tx = self.send_entries_to_client_service.lock().await;
                tx.send(commit_entry).await.unwrap();
            }
        }

        trace!("[{}] commitChanSender done", self.id);
    }
}
