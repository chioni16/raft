use std::time::Duration;

use raft::consensus::RaftConsensus;
use tokio::sync::broadcast;

pub struct Harness {
    cluster: Vec<RaftConsensus<String>>,
    connected: Vec<bool>,
    num_servers: usize,
}

impl Harness {
    pub async fn new(num_servers: usize) -> Self {
        let (ready_tx, _) = broadcast::channel(1);

        let cluster = (0..num_servers)
            .map(|id| {
                let rx = ready_tx.subscribe();
                RaftConsensus::new(id as u64, rx)
            })
            .collect::<Vec<_>>();

        tokio::time::sleep(Duration::from_secs(2)).await;

        for i in 0..num_servers {
            for j in 0..num_servers {
                if i != j {
                    let dst = cluster[j].get_listen_addr(true);
                    cluster[i].connect_peer(j as u64, dst).await.unwrap();
                }
            }
        }

        let connected = vec![true; num_servers];

        ready_tx.send(()).unwrap();

        Self {
            cluster,
            connected,
            num_servers,
        }
    }

    pub async fn shutdown(&mut self) {
        for i in 0..self.num_servers {
            self.cluster[i].disconnect_all_peers().await;
            self.connected[i] = false;
        }
        for i in 0..self.num_servers {
            self.cluster[i].shutdown().await;
        }
    }

    pub async fn disconnect_peer(&mut self, id: u64) {
        let id = id as usize;

        self.cluster[id].disconnect_all_peers().await;

        for j in 0..self.num_servers {
            if j != id {
                self.cluster[j].disconnect_peer(id as u64).await;
            }
        }

        self.connected[id] = false;
    }

    pub async fn reconnect_peer(&mut self, id: u64) {
        let id = id as usize;

        for j in 0..self.num_servers {
            if j != id {
                self.cluster[id]
                    .connect_peer(j as u64, self.cluster[j].get_listen_addr(true))
                    .await
                    .unwrap();
                self.cluster[j]
                    .connect_peer(id as u64, self.cluster[id].get_listen_addr(true))
                    .await
                    .unwrap();
            }
        }

        self.connected[id] = true;
    }

    pub async fn check_single_leader(&self) -> Option<(u64, u64)> {
        for _ in 0..5 {
            let mut leader_id = None;
            let mut leader_term = None;
            for i in 0..self.num_servers {
                if self.connected[i] {
                    let (_, term, is_leader) = self.cluster[i].report().await;
                    if is_leader {
                        if let Some(leader_id) = leader_id {
                            println!("both {i} and {leader_id} think they're leaders");
                            return None;
                        }
                        leader_id = Some(i);
                        leader_term = Some(term);
                    }
                }
            }
            if leader_id.is_some() {
                return Some((leader_id.unwrap() as u64, leader_term.unwrap()));
            }

            tokio::time::sleep(Duration::from_millis(150)).await;
        }

        None
    }

    pub async fn check_no_leader(&self) -> bool {
        for i in 0..self.num_servers {
            if self.connected[i] {
                let (_, _, is_leader) = self.cluster[i].report().await;
                if is_leader {
                    return false;
                }
            }
        }

        true
    }
}
