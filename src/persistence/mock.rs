use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::Persistence;
use crate::raftlog::Command;

// mainly used for testing raft implementation
#[derive(Debug)]
pub struct MockPersistence {
    map: Arc<Mutex<HashMap<String, String>>>,
}

impl MockPersistence {
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn clone(&self) -> Self {
        Self {
            map: Arc::clone(&self.map),
        }
    }
}

#[async_trait]
impl Persistence for MockPersistence {
    async fn get<T: Command>(&self, key: String) -> Option<T> {
        let map = self.map.lock().await;
        map.get(&key).map(|value| {
            let value = serde_json::from_str(value).unwrap();
            value
        })
    }

    async fn set<T: Command>(&self, key: String, value: T) {
        let mut map = self.map.lock().await;
        let value = serde_json::to_string(&value).unwrap();
        map.insert(key, value);
    }

    async fn has_data(&self) -> bool {
        let map = self.map.lock().await;
        map.len() > 0
    }
}
