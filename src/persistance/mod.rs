pub mod mock;

use async_trait::async_trait;
use std::fmt::Debug;

use crate::{
    consensus::RaftConsensus,
    raftlog::{Command, Log},
};

#[async_trait]
pub trait Persistance: Debug + Send + Sync + 'static {
    async fn get<T: Command>(&self, key: String) -> Option<T>;
    async fn set<T: Command>(&self, key: String, value: T);
    async fn has_data(&self) -> bool;
}

impl<T: Command, P: Persistance> RaftConsensus<T, P> {
    // restores the state marked as persistent in `state.rs`
    pub(crate) async fn restore_persistent_data(&self) {
        let mut state = self.state.lock().await;

        let cur_term = self.persistance.get("cur_term".to_string()).await.unwrap();
        state.cur_term = cur_term;

        let voted_for = self.persistance.get("voted_for".to_string()).await.unwrap();
        state.voted_for = Some(voted_for);

        let log = self.persistance.get("log".to_string()).await.unwrap();
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
        self.persistance.set("cur_term".to_string(), cur_term).await;
        self.persistance
            .set("voted_for".to_string(), voted_for)
            .await;
        self.persistance.set("log".to_string(), log).await;
    }
}
