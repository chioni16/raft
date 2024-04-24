mod raft {
    tonic::include_proto!("raft");
}

use crate::{
    consensus::RaftConsensus,
    state::{Id, OperationMode},
};
use log::trace;
pub use raft::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply,
};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

// TODO: Are the RPC requests handled in a different `tokio::spawn` automatically?
// Or should I do it explicitly within each handler function?
#[tonic::async_trait]
impl<T: Debug + Send + 'static> Raft for RaftConsensus<T> {
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

        trace!(
            "[{}] received RequestVote: {:?} [current_term = {}, voted_for = {:?}",
            self.id,
            args,
            state.cur_term,
            state.voted_for
        );

        if args.term > state.cur_term {
            trace!("[{}] ... term out of date in RequestVote", self.id);
            self.become_follower(&mut state, args.term).await;
        }

        let mut vote_granted = false;
        if state.cur_term == args.term {
            if state.voted_for.is_none()
                || matches!(state.voted_for, Some(id) if id == args.candidate_id)
            {
                vote_granted = true;
                state.voted_for = Some(args.candidate_id);
                state.election_reset_event = Instant::now();
            }
        }

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
            state.election_reset_event = Instant::now();
            success = true;
        }

        let reply = AppendEntriesReply {
            term: state.cur_term,
            success,
        };
        trace!("[{}] AppendEntries reply {:?}", self.id, reply);
        Ok(Response::new(reply))
    }
}

impl<T: Debug + Send + 'static> RaftConsensus<T> {
    pub async fn call_request_vote(
        &self,
        id: Id,
        args: RequestVoteArgs,
    ) -> Result<RequestVoteReply, Status> {
        // let mut client = get_client(id).await?;
        let mut peers = self.peers.lock().await;
        let client = peers
            .get_mut(&id)
            .ok_or(Status::resource_exhausted(format!(
                "[{}] call_request_vote peer client {} closed",
                self.id, id
            )))?;

        let request = Request::new(args);
        let response = client.request_vote(request).await?.into_inner();

        Ok(response)
    }

    pub async fn call_append_entries(
        &self,
        id: Id,
        args: AppendEntriesArgs,
    ) -> Result<AppendEntriesReply, Status> {
        // let mut client = get_client(id).await?;
        let mut peers = self.peers.lock().await;
        let client = peers
            .get_mut(&id)
            .ok_or(Status::resource_exhausted(format!(
                "[{}] call_append_entries peer client {} closed",
                self.id, id
            )))?;

        let request = Request::new(args);
        let response = client.append_entries(request).await?.into_inner();

        Ok(response)
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

// async fn get_client(id: Id) -> Result<RaftClient<Channel>, Status> {
//     let dst = format!("http://[::1]:{}", crate::PORT_BASE + id);
//     RaftClient::connect(dst)
//         .await
//         .map_err(|err| Status::from_error(Box::new(err)))
// }

pub async fn get_client(dst: String) -> Result<RaftClient<Channel>, Status> {
    RaftClient::connect(dst)
        .await
        .map_err(|err| Status::from_error(Box::new(err)))
}

impl<T: Debug + Send + 'static> RaftConsensus<T> {
    // pub async fn start_rpc_server(self, addr: SocketAddr, shutdown: oneshot::Receiver<()>) {
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
