use crate::{
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
    sync::{broadcast, oneshot, Mutex, MutexGuard},
    task::JoinHandle,
    time::interval,
};

pub struct RaftConsensusInner<T> {
    pub id: Id,
    pub peers: Mutex<HashMap<Id, rpc::RaftClient<tonic::transport::Channel>>>,
    pub state: Mutex<State<T>>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
}

pub struct RaftConsensus<T>(Arc<RaftConsensusInner<T>>);

impl<T> Deref for RaftConsensus<T> {
    type Target = RaftConsensusInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Debug + Send + 'static> RaftConsensus<T> {
    pub fn new(id: Id, mut ready: broadcast::Receiver<()>) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let inner = RaftConsensusInner {
            id,
            peers: Mutex::new(HashMap::new()),
            state: Mutex::new(State::<T>::new()),
            shutdown: Mutex::new(Some(shutdown_tx)),
        };
        let raft = Self(Arc::new(inner));

        let raft2 = raft.clone();
        tokio::spawn(async move {
            // let addr = raft2.get_listen_addr();
            // raft2.start_rpc_server(addr, shutdown_rx).await;
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
    // if any of them is changed, then this instance of run_election_timer comes to an end
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
                break;
            }
        }

        // guard is created within the loop scope
        // guard is dropped by the time we get here
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
        state.cur_term += 1;
        state.election_reset_event = Instant::now();
        state.voted_for = Some(self.id);

        let saved_current_term = state.cur_term;
        trace!(
            "[{}] becomes candidate (term = {}, log: {:?})",
            self.id,
            saved_current_term,
            state.log
        );

        state.votes_received = 1;

        // Send RequestVote RPCs to all other servers concurrently.
        for peer in self.peers.lock().await.keys() {
            let peer = *peer;
            let args = rpc::RequestVoteArgs {
                term: saved_current_term,
                candidate_id: self.id,
                ..Default::default()
            };

            let raft = self.clone();
            spawn_call_request_vote(raft, saved_current_term, peer, args);
        }

        let raft = self.clone();
        spawn_run_election_timer(raft);
    }

    async fn start_leader(&self, state: &mut MutexGuard<'_, State<T>>) {
        // let mut state = self.state.lock().await;

        state.opmode = OperationMode::Leader;
        trace!(
            "[{}] becomes leader; term={}, log={:?}",
            self.id,
            state.cur_term,
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

        if state.opmode != OperationMode::Leader {
            return;
        }

        let saved_cur_term = state.cur_term;

        for peer in self.peers.lock().await.keys() {
            let peer = *peer;
            let args = rpc::AppendEntriesArgs {
                term: saved_cur_term,
                leader_id: self.id,
                ..Default::default()
            };

            let raft = self.clone();
            tokio::spawn(async move {
                trace!(
                    "[{}] sending AppendEntries to {}: ni={}, args={:#?}",
                    raft.id,
                    peer,
                    0,
                    args
                );

                if let Ok(reply) = raft.call_append_entries(peer, args).await {
                    if reply.term > saved_cur_term {
                        trace!("[{}] term out of date in heartbeat reply", raft.id);
                        let mut state = raft.state.lock().await;
                        raft.become_follower(&mut state, reply.term).await;
                    }
                }
            });
        }
    }

    pub async fn become_follower(&self, state: &mut MutexGuard<'_, State<T>>, new_term: u64) {
        // let mut state = self.state.lock().await;

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
fn spawn_call_request_vote<T: Debug + Send + 'static>(
    raft: RaftConsensus<T>,
    saved_current_term: u64,
    peer: Id,
    args: rpc::RequestVoteArgs,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        trace!("[{}] sending RequestVote to {}: {:#?}", raft.id, peer, args);

        if let Ok(reply) = raft.call_request_vote(peer, args).await {
            trace!("[{}] received RequestVoteReply {:#?}", raft.id, reply);

            let mut state = raft.state.lock().await;

            if state.opmode != OperationMode::Candidate {
                trace!(
                    "[{}] while waiting for reply, state = {:?}",
                    raft.id,
                    state.opmode
                );
                return;
            }

            if reply.term > saved_current_term {
                trace!("[{}] term out of date in RequestVoteReply", raft.id);
                raft.become_follower(&mut state, reply.term).await;
                // return;
            } else if reply.term == saved_current_term {
                if reply.vote_granted {
                    state.votes_received += 1;

                    if 2 * state.votes_received > raft.peers.lock().await.len() as u64 + 1 {
                        // Won the election!
                        trace!(
                            "[{}] wins election with {} votes",
                            raft.id,
                            state.votes_received
                        );
                        raft.start_leader(&mut state).await;
                        // return;
                    }
                }
            }
        }
    })
}

// Workaround for tokio "cycle detected"
fn spawn_run_election_timer<T: Debug + Send + 'static>(raft: RaftConsensus<T>) {
    tokio::spawn(async move {
        raft.run_election_timer().await;
    });
}

// Functions used by tests
impl<T: Debug + Send + 'static> RaftConsensus<T> {
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
    }

    pub async fn shutdown(&self) {
        self.stop().await;
        // stop the tonic RPC server
        self.shutdown.lock().await.take().unwrap().send(()).unwrap();
    }

    pub async fn connect_peer(&self, id: Id, connect_string: String) -> Result<(), tonic::Status> {
        let mut peers = self.peers.lock().await;

        if peers.get(&id).is_none() {
            let client = rpc::get_client(connect_string).await?;
            peers.insert(id, client);
        }

        Ok(())
    }

    pub async fn disconnect_peer(&self, id: Id) {
        let mut peers = self.peers.lock().await;
        peers.remove(&id);
        // connection closed automatically when client is dropped
    }

    pub async fn disconnect_all_peers(&self) {
        let mut peers = self.peers.lock().await;
        peers.clear();
    }

}