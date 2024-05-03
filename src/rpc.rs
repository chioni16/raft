mod raft {
    tonic::include_proto!("raft");
}

use crate::{
    consensus::RaftConsensus,
    persistance::Persistance,
    raftlog::Command,
    state::{Id, OperationMode},
};
use log::trace;
pub use raft::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, LogEntry, RequestVoteArgs, RequestVoteReply,
};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

// TODO: Are the RPC requests handled in a different `tokio::spawn` automatically?
// Or should I do it explicitly within each handler function?
#[tonic::async_trait]
impl<T: Command, P: Persistance> Raft for RaftConsensus<T, P> {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        simulate_network("RequestVote").await?;

        let args = request.into_inner();
        let mut state = self.state.lock().await;

        if state.opmode == OperationMode::Dead {
            return Err(Status::unavailable(format!("node {} is dead", self.id)));
        }

        let last_log_index = state.log.get_last_log_index();
        let last_log_term = state.log.get_last_log_term();

        trace!(
            "[{}] received RequestVote: {:?} [current_term = {}, voted_for = {:?} log index/term=({}, {})",
            self.id,
            args,
            state.cur_term,
            state.voted_for,
            last_log_index,
            last_log_term,
        );

        if args.term > state.cur_term {
            trace!("[{}] ... term out of date in RequestVote", self.id);
            self.become_follower(&mut state, args.term).await;
        }

        let mut vote_granted = false;
        if state.cur_term == args.term {
            // we can only vote for one candidate per term
            // NOTE: if we receive a request from the candidate we have already voted for, we need to reaffirm our vote
            if state.voted_for.is_none()
                || matches!(state.voted_for, Some(id) if id == args.candidate_id)
            {
                // Election Safety:
                // candidate should have a more "up to date" log
                if args.last_log_term > last_log_term
                    || (args.last_log_term == last_log_term
                        && args.last_log_index >= last_log_index)
                {
                    vote_granted = true;
                    state.voted_for = Some(args.candidate_id);
                    state.election_reset_event = Instant::now();
                }
            }
        }

        self.store_persistent_data(state.cur_term, state.voted_for, state.log.clone())
            .await;

        let reply = RequestVoteReply {
            term: state.cur_term,
            vote_granted,
        };
        trace!("[{}] RequestVote reply {:?}", self.id, reply);
        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        simulate_network("AppendEntries").await?;

        let args = request.into_inner();
        let mut state = self.state.lock().await;

        if state.opmode == OperationMode::Dead {
            return Err(Status::unavailable(format!("node {} is dead", self.id)));
        }

        trace!("[{}] AppendEntries: {:?}", self.id, args);

        if args.term > state.cur_term {
            trace!("[{}] ... term out of date in AppendEntries", self.id);
            self.become_follower(&mut state, args.term).await;
        }

        let mut success = false;
        if state.cur_term == args.term {
            if state.opmode != OperationMode::Follower {
                self.become_follower(&mut state, args.term).await;
            }
            // reset election timer when you get a valid `AppendEntries` message
            // valid here means message from a Leader and args.term >= term
            state.election_reset_event = Instant::now();

            // log contains an entry at prevLogIndex whose term matches prevLogTerm
            if args.prev_log_index < state.log.len()
                && args.prev_log_term == state.log.get_log_term(args.prev_log_index)
            {
                success = true;

                // overwrite follower's log so that it matches leader's log

                // find last entry that are same in both logs
                let mut log_insert_index = args.prev_log_index + 1;
                let mut new_entries_index = 0;

                loop {
                    if log_insert_index >= state.log.len()
                        || new_entries_index >= args.entries.len()
                    {
                        break;
                    }
                    if state.log.get_log_term(log_insert_index)
                        != args.entries[new_entries_index].term
                    {
                        break;
                    }
                    log_insert_index += 1;
                    new_entries_index += 1;
                }

                // `log_insert_index` points at the end of the follower's log or the index where there is a mismatch
                // `new_entries_index` points at the end of the `args.entries` or the index where there is a mismatch

                // there are log entries in the leader's log that are not replicated in follower's log
                if new_entries_index < args.entries.len() {
                    trace!(
                        "[{}]... inserting entries {:?} from index {}",
                        self.id,
                        &args.entries[new_entries_index..],
                        log_insert_index
                    );

                    // overwrite follower's log

                    // remove entries in the follower's log after the mismatch
                    state.log.truncate(log_insert_index);
                    // add entries from the leader's log
                    state.log.extend(
                        args.entries[new_entries_index..]
                            .iter()
                            .map(|e| e.clone().into())
                            .collect(),
                    );

                    trace!("[{}]... log is now: {:?}", self.id, state.log);
                }

                if args.leader_commit > state.commit_index {
                    state.commit_index = core::cmp::min(args.leader_commit, state.log.len() - 1);
                    trace!(
                        "[{}]... setting commitIndex={}",
                        self.id,
                        state.commit_index
                    );
                    // signal change in `commit_index`
                    let tx = self.commit_entries_ready.lock().await;
                    tx.send(true).await.unwrap();
                }
            }
        }

        self.store_persistent_data(state.cur_term, state.voted_for, state.log.clone())
            .await;

        let reply = AppendEntriesReply {
            term: state.cur_term,
            success,
        };
        trace!("[{}] AppendEntries reply {:?}", self.id, reply);
        Ok(Response::new(reply))
    }
}

impl<T: Command, P: Persistance> RaftConsensus<T, P> {
    pub async fn call_request_vote(
        &self,
        id: Id,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Status> {
        let mut peers = self.peers.lock().await;
        let client = peers
            .get_mut(&id)
            .ok_or(Status::resource_exhausted(format!(
                "[{}] call_request_vote peer client {} closed",
                self.id, id
            )))?;

        if let Some(client) = client {
            let request = Request::new(args);
            let response = client.request_vote(request).await?.into_inner();
            Ok(response)
        } else {
            Err(Status::unavailable("target disconnected"))
        }
    }

    pub async fn call_append_entries(
        &self,
        id: Id,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Status> {
        let mut peers = self.peers.lock().await;
        let client = peers
            .get_mut(&id)
            .ok_or(Status::resource_exhausted(format!(
                "[{}] call_append_entries peer client {} closed",
                self.id, id
            )))?;

        if let Some(client) = client {
            let request = Request::new(args);
            let response = client.append_entries(request).await?.into_inner();
            Ok(response)
        } else {
            Err(Status::unavailable("target disconnected"))
        }
    }
}

async fn simulate_network(rpc: &str) -> Result<(), Status> {
    if let Ok(_) = std::env::var(crate::RAFT_UNRELIABLE_RPC) {
        let dice = crate::random(0, 10);
        if dice == 9 {
            trace!("drop {rpc}");
            return Err(Status::unavailable(format!(
                "network simulation drop RPC {rpc}"
            )));
        } else if dice == 8 {
            trace!("delay {rpc}");
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
    } else {
        let wait_for = Duration::from_millis(1 + crate::random(0, 5));
        tokio::time::sleep(wait_for).await;
    }

    Ok(())
}

pub async fn get_client(dst: String) -> Result<RaftClient<Channel>, Status> {
    RaftClient::connect(dst)
        .await
        .map_err(|err| Status::from_error(Box::new(err)))
}

impl<T: Command, P: Persistance> RaftConsensus<T, P> {
    pub async fn start_rpc_server(self, dst: String, shutdown: oneshot::Receiver<()>) {
        trace!("[{}] dst: {}", self.id, dst);
        let addr = dst.parse().unwrap();
        let svc = RaftServer::new(self);
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = shutdown.await;
            })
            .await
            .unwrap();
    }
}
