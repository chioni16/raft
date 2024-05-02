use crate::{
    raftlog::Entry,
    rpc,
    state::{Id, OperationMode, State},
};
use log::trace;
use std::{
    collections::HashMap,
    fmt::Debug,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, oneshot, Mutex, MutexGuard},
    task::JoinHandle,
    time::interval,
};

pub struct RaftConsensusInner<T: Clone> {
    pub id: Id,
    pub peers: Mutex<HashMap<Id, Option<rpc::RaftClient<tonic::transport::Channel>>>>,
    pub state: Mutex<State<T>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    send_entries_to_client_service: Mutex<mpsc::Sender<CommitEntry<T>>>,
    pub(crate) commit_entries_ready: Mutex<mpsc::Sender<bool>>,
}

pub struct RaftConsensus<T: Clone>(Arc<RaftConsensusInner<T>>);

impl<T: Clone> Deref for RaftConsensus<T> {
    type Target = RaftConsensusInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Debug + Default + Clone + Send + From<String> + Into<String> + 'static> RaftConsensus<T> {
    pub fn new(
        id: Id,
        mut ready: broadcast::Receiver<()>,
        send_entries_to_client_service: mpsc::Sender<CommitEntry<T>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (commit_entries_ready_tx, commit_entries_ready_rx) = mpsc::channel(16);
        let inner = RaftConsensusInner {
            id,
            peers: Mutex::new(HashMap::new()),
            state: Mutex::new(State::<T>::new()),
            shutdown: Mutex::new(Some(shutdown_tx)),
            send_entries_to_client_service: Mutex::new(send_entries_to_client_service),
            commit_entries_ready: Mutex::new(commit_entries_ready_tx),
        };
        let raft = Self(Arc::new(inner));

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

            if state.opmode != OperationMode::Candidate && state.opmode != OperationMode::Follower {
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
        state.opmode = OperationMode::Candidate;
        // move to next term before starting election
        state.cur_term += 1;
        state.election_reset_event = Instant::now();
        // vote for yourself
        state.voted_for = Some(self.id);
        state.votes_received = 1;

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
        // state set to `Leader`
        state.opmode = OperationMode::Leader;

        // re-initialise `next_index` and `match_index`
        {
            // remove old entries, if any
            state.next_index.clear();
            state.match_index.clear();

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
                state.next_index.insert(*peer, log_len);

                // `match_index` is the latest log entry that is KNOWN to be replicated on the follower
                // set pesimistically to 0
                // increased monotonically as the leader sends `AppendEntries` RPC requests
                state.match_index.insert(*peer, 0);
            }
        }

        trace!(
            "[{}] becomes leader; term={}, next_index: {:?}, match_index: {:?}, log={:?}",
            self.id,
            state.cur_term,
            state.next_index,
            state.match_index,
            state.log
        );

        let raft = self.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(50));

            // send periodic heartbeats as long as it is leader
            loop {
                raft.send_heartbeats().await;
                interval.tick().await;

                let state = raft.state.lock().await;
                if state.opmode != OperationMode::Leader {
                    return;
                }
            }
        });
    }

    async fn send_heartbeats(&self) {
        let state = self.state.lock().await;

        // send heartbeats only if `Leader`
        if state.opmode != OperationMode::Leader {
            return;
        }

        for peer in self.peers.lock().await.keys() {
            // prepare AppendEntries request
            // different for each follower, which depends on the `Leader`s view of the extent to which log is replicated on the follower

            // `Leader`s idea of the next entry to be sent to the follower (may or may not be true)
            trace!(
                "[{}] send_heartbeats: peer: {}, next_index: {:#?}",
                self.id,
                peer,
                state.next_index
            );
            let next_index = *state.next_index.get(peer).unwrap();

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

            spawn_send_heartbeats(self.clone(), *peer, next_index, args);
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
}

// Workaround for tokio "cycle detected"
fn spawn_call_request_vote<
    T: Debug + Default + Clone + Send + From<String> + Into<String> + 'static,
>(
    raft: RaftConsensus<T>,
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

            if state.opmode != OperationMode::Candidate {
                trace!(
                    "[{}] while waiting for reply, state = {:?}",
                    raft.id,
                    state.opmode
                );
                return;
            }

            match reply.term.cmp(&state.cur_term) {
                core::cmp::Ordering::Greater => {
                    trace!("[{}] term out of date in RequestVoteReply", raft.id);
                    raft.become_follower(&mut state, reply.term).await;
                }
                core::cmp::Ordering::Equal => {
                    if reply.vote_granted {
                        state.votes_received += 1;

                        let peers_count = raft.peers.lock().await.len() as u64;
                        if 2 * state.votes_received > peers_count + 1 {
                            // received a majority of votes
                            trace!(
                                "[{}] wins election with {} votes",
                                raft.id,
                                state.votes_received
                            );
                            raft.start_leader(&mut state).await;
                        }
                    }
                }
                // the receiver updates its term before sending the reply if its term was previously lower
                // so receiving a response from previous term is to be ignored
                core::cmp::Ordering::Less => {}
            }
        }
    })
}

fn spawn_send_heartbeats<
    T: Debug + Default + Clone + Send + From<String> + Into<String> + 'static,
>(
    raft: RaftConsensus<T>,
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

            // identify if the RPC response received is for the current term
            if state.opmode == OperationMode::Leader && reply.term == state.cur_term {
                if reply.success {
                    // `reply.success == true` shows that all the sent log entries are now part of the follower's log
                    // so, we can increase the (previous) `next_index` corresponding to this follower by the number of log entries in the sent request
                    state.next_index.insert(peer, next_index + entries_len);
                    // `match_index` is 1 less than `next_index` by definition
                    // `match_index` shows the max entry that is known to be successfully replicated on the follower
                    // whereas, `next_index` points to the next entry to be sent
                    state.match_index.insert(peer, next_index + entries_len - 1);
                    trace!("[{}] AppendEntries reply from {} success: nextIndex := {:?}, matchIndex := {:?}", raft.id, peer, state.next_index, state.match_index);

                    let saved_commit_index = state.commit_index;
                    // update `commit_index` of `Leader`
                    // find the index of log entry that is successfully replicated on a MAJORITY of servers
                    // the updated `commit_index` is propagated to the followers when the next set of `AppendEntries` are sent
                    for index in state.commit_index + 1..state.log.len() {
                        // raft never commits log entries from previous terms by counting replicas
                        // only log entries from the leader’s current term are committed by counting replicas
                        if state.log.get_log_term(index) == state.cur_term {
                            // `match_count` and `peers_count` don't include `Leader`
                            let match_count =
                                state.match_index.values().filter(|v| **v >= index).count();
                            let peers_count = raft.peers.lock().await.len();

                            // majority is atleast ceil(total number of servers / 2)
                            // => majority is an integer greater than (total number of servers / 2)
                            // => 2 * majority is greater than total number of servers
                            // peers_count + 1 is the total number of servers in the cluster (including `Leader`)

                            // match_count only counts the peers where the command has been replicated (excluding Leader)
                            // But the Leader the propagates the command only after adding it to its own log
                            // match_count + 1 is the total number of servers where the command has been successfully replicated (including `Leader`)
                            if 2 * (match_count + 1) > peers_count + 1 {
                                state.commit_index = index;
                            }
                        }
                    }
                    if state.commit_index != saved_commit_index {
                        trace!(
                            "[{}] leader sets commitIndex := {}",
                            raft.id,
                            state.commit_index
                        );

                        // signal change in `commit_index`
                        let tx = raft.commit_entries_ready.lock().await;
                        tx.send(true).await.unwrap();
                    }
                } else {
                    // prepare for the next round
                    // decrement `next_index` by 1
                    state.next_index.insert(peer, next_index - 1);
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
fn spawn_run_election_timer<
    T: Debug + Default + Clone + Send + From<String> + Into<String> + 'static,
>(
    raft: RaftConsensus<T>,
) {
    tokio::spawn(async move {
        raft.run_election_timer().await;
    });
}

// Functions used by tests
impl<T: Debug + Default + Clone + Send + 'static> RaftConsensus<T> {
    pub async fn report(&self) -> (Id, u64, bool) {
        let state = self.state.lock().await;
        (
            self.id,
            state.cur_term,
            state.opmode == OperationMode::Leader,
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
impl<T: Debug + Default + Clone + Send + 'static> RaftConsensus<T> {
    pub async fn submit(&self, command: T) -> bool {
        let mut state = self.state.lock().await;
        trace!(
            "[{}] submit received by {:?}: {:?}",
            self.id,
            state.opmode,
            command
        );

        if state.opmode == OperationMode::Leader {
            let entry = Entry::new(command, state.cur_term);
            state.log.add_new_entry(entry);
            trace!("[{}] ... log={:?}", self.id, state.log);
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
