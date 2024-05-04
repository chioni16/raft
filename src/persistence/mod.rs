pub mod mock;

use async_trait::async_trait;
use std::fmt::Debug;

use crate::{
    consensus::RaftConsensus,
    raftlog::{Command, Log},
};

#[async_trait]
pub trait Persistence: Debug + Send + Sync + 'static {
    async fn get<T: Command>(&self, key: String) -> Option<T>;
    async fn set<T: Command>(&self, key: String, value: T);
    async fn has_data(&self) -> bool;
}

impl<T: Command, P: Persistence> RaftConsensus<T, P> {
    // restores the state marked as persistent in `state.rs`
    pub(crate) async fn restore_persistent_data(&self) {
        let mut state = self.state.lock().await;

        let cur_term = self.persistence.get("cur_term".to_string()).await.unwrap();
        state.cur_term = cur_term;

        let voted_for = self.persistence.get("voted_for".to_string()).await.unwrap();
        state.voted_for = voted_for;

        let log = self.persistence.get("log".to_string()).await.unwrap();
        log::trace!(
            "[{}] restoring state: cur_term: {}, voted_for: {:?}, log: {:?}",
            self.id,
            cur_term,
            voted_for,
            log
        );
        state.log = log;
    }

    // stores the state marked as persistent in `state.rs`
    // update stable storage before responding to RPCs
    pub(crate) async fn store_persistent_data(
        &self,
        cur_term: u64,
        voted_for: Option<u64>,
        log: Log<T>,
    ) {
        self.persistence.set("cur_term".to_string(), cur_term).await;
        self.persistence
            .set("voted_for".to_string(), voted_for)
            .await;
        self.persistence.set("log".to_string(), log).await;
    }
}
