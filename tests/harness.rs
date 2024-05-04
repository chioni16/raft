use std::{sync::Arc, time::Duration};

use raft::{
    consensus::{CommitEntry, RaftConsensus},
    persistence::mock::MockPersistence,
};
use tokio::sync::{broadcast, mpsc, Mutex};

pub struct HarnessInner {
    cluster: Vec<RaftConsensus<String, MockPersistence>>,
    commits: Vec<Vec<CommitEntry<String>>>,
    connected: Vec<bool>,
    alive: Vec<bool>,
    num_servers: usize,
    persistences: Vec<MockPersistence>,
}

pub struct Harness(Arc<Mutex<HarnessInner>>);

impl Harness {
    pub async fn new(num_servers: usize) -> Self {
        let (ready_tx, _) = broadcast::channel(1);

        // create nodes in the cluster
        let mut cluster = vec![];
        let mut commit_rxs = vec![];
        let mut persistences = vec![];
        for id in 0..num_servers {
            let ready_rx = ready_tx.subscribe();
            let (commit_channel_tx, commit_channel_rx) = mpsc::channel(1);
            let persistence = MockPersistence::new();
            let node =
                RaftConsensus::new(id as u64, ready_rx, commit_channel_tx, persistence.clone())
                    .await;

            cluster.push(node);
            commit_rxs.push((id, commit_channel_rx));
            persistences.push(persistence);
        }

        // wait for nodes to come up and attain a stable state
        tokio::time::sleep(Duration::from_secs(2)).await;

        // connect all the nodes in the cluster to each other
        // this also sets up the peer list for each node
        let connected = {
            for i in 0..num_servers {
                for j in 0..num_servers {
                    if i != j {
                        let dst = cluster[j].get_listen_addr(true);
                        cluster[i].connect_peer(j as u64, dst).await.unwrap();
                    }
                }
            }

            vec![true; num_servers]
        };

        let alive = vec![true; num_servers];

        let harness = {
            let harness = HarnessInner {
                cluster,
                commits: vec![vec![]; num_servers],
                connected,
                alive,
                num_servers,
                persistences,
            };

            Self(Arc::new(Mutex::new(harness)))
        };

        // start async tasks that collect commit entries from each node
        for (id, commit_rx) in commit_rxs {
            let harness2 = harness.clone();
            tokio::spawn(async move {
                harness2.collect_commits(id, commit_rx).await;
            });
        }

        // start the nodes
        ready_tx.send(()).unwrap();

        harness
    }

    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub async fn shutdown(&self) {
        let mut harness = self.0.lock().await;

        for i in 0..harness.num_servers {
            harness.cluster[i].disconnect_all_peers().await;
            harness.connected[i] = false;
        }

        for i in 0..harness.num_servers {
            if harness.alive[i] {
                harness.alive[i] = false;
                harness.cluster[i].shutdown().await;
            }
        }
    }

    pub async fn disconnect_peer(&self, id: u64) {
        let mut harness = self.0.lock().await;

        let id = id as usize;

        harness.cluster[id].disconnect_all_peers().await;

        for j in 0..harness.num_servers {
            if j != id {
                harness.cluster[j].disconnect_peer(id as u64).await;
            }
        }

        harness.connected[id] = false;
    }

    pub async fn reconnect_peer(&self, id: u64) {
        let mut harness = self.0.lock().await;

        let id = id as usize;

        for j in 0..harness.num_servers {
            if j != id && harness.alive[j] {
                harness.cluster[id]
                    .connect_peer(j as u64, harness.cluster[j].get_listen_addr(true))
                    .await
                    .unwrap();
                harness.cluster[j]
                    .connect_peer(id as u64, harness.cluster[id].get_listen_addr(true))
                    .await
                    .unwrap();
            }
        }

        harness.connected[id] = true;
    }

    pub async fn check_single_leader(&self) -> Option<(u64, u64)> {
        let harness = self.0.lock().await;

        for _ in 0..8 {
            let mut leader_id = None;
            let mut leader_term = None;
            for i in 0..harness.num_servers {
                if harness.connected[i] {
                    let (_, term, is_leader) = harness.cluster[i].report().await;
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
        let harness = self.0.lock().await;

        for i in 0..harness.num_servers {
            if harness.connected[i] {
                let (_, _, is_leader) = harness.cluster[i].report().await;
                if is_leader {
                    return false;
                }
            }
        }

        true
    }

    // https://github.com/eliben/raft/blob/master/part2/testharness.go
    //
    // CheckCommitted verifies that all connected servers have cmd committed with
    // the same index. It also verifies that all commands *before* cmd in
    // the commit sequence match. For this to work properly, all commands submitted
    // to Raft should be unique positive ints.
    // Returns the number of servers that have this command committed, and its
    // log index.
    // TODO: this check may be too strict. Consider that a server can commit
    // something and crash before notifying the channel. It's a valid commit but
    // this checker will fail because it may not match other servers. This scenario
    // is described in the paper...

    pub async fn check_committed(&self, command: String) -> (usize, u64) {
        // (num of servers committed, index of commit)
        let harness = self.0.lock().await;

        // Find the length of the commits slice for connected servers
        let mut commits_len = None;
        for i in 0..harness.num_servers {
            if harness.connected[i] {
                log::trace!("commits for [{}]: {:?}", i, harness.commits[i]);
                if let Some(commits_len) = commits_len {
                    assert_eq!(harness.commits[i].len(), commits_len);
                } else {
                    commits_len = Some(harness.commits[i].len());
                }
            }
        }

        // Check consistency of commits from the start and to the command we're asked
        // about. This loop will return once a command=cmd is found.

        for c in 0..commits_len.unwrap() {
            let mut cmdc = None;

            for i in 0..harness.num_servers {
                if harness.connected[i] {
                    let cmdi = &harness.commits[i][c];
                    if let Some(cmd) = cmdc {
                        assert_eq!(cmdi, cmd);
                    } else {
                        cmdc = Some(cmdi);
                    }
                }
            }

            if &cmdc.unwrap().command == &command {
                // Check consistency of Index
                let mut index = None;
                let mut num = 0;

                for i in 0..harness.num_servers {
                    if harness.connected[i] {
                        if let Some(index) = index {
                            assert_eq!(index, harness.commits[i][c].index);
                        } else {
                            index = Some(harness.commits[i][c].index)
                        }
                        num += 1;
                    }
                }

                return (num, index.unwrap());
            }
        }

        panic!("cmd={} not found in commits", command);
    }

    // CheckCommittedN verifies that cmd was committed by exactly n connected servers
    pub async fn check_committed_n(&self, command: String, num: usize) {
        let (nc, _) = self.check_committed(command).await;
        assert_eq!(nc, num);
    }

    // CheckNotCommitted verifies that no command equal to cmd has been committed
    // by any of the active servers yet
    pub async fn check_not_committed(&self, command: String) {
        let harness = self.0.lock().await;

        for i in 0..harness.num_servers {
            if harness.connected[i] {
                for c in 0..harness.commits[i].len() {
                    let cmdi = &harness.commits[i][c].command;
                    assert_ne!(cmdi, &command);
                }
            }
        }
    }

    pub async fn submit_to_server(&self, node: u64, command: String) -> bool {
        let harness = self.0.lock().await;
        harness.cluster[node as usize].submit(command).await
    }

    pub async fn collect_commits(
        &self,
        node: usize,
        mut commit_rx: mpsc::Receiver<CommitEntry<String>>,
    ) {
        // the idea is that the `tx` is closed when it's dropped in the `send_commands_to_client_service` function of `RaftConsensus`
        // as there is only `tx` associated with this `rx`, the `recv` method should return `None`
        // which leads this function to end as well
        while let Some(commit) = commit_rx.recv().await {
            let mut harness = self.0.lock().await;
            if harness.alive[node] {
                println!("collectCommits({}) got {:?}", node, commit);
                harness.commits[node].push(commit);
            }
        }
    }

    //     // CrashPeer "crashes" a server by disconnecting it from all peers and then
    //     // asking it to shut down. We're not going to use the same server instance
    //     // again, but its storage is retained.
    //     func (h *Harness) CrashPeer(id int) {
    //         tlog("Crash %d", id)
    //         h.DisconnectPeer(id)
    //         h.alive[id] = false
    //         h.cluster[id].Shutdown()

    //         // Clear out the commits slice for the crashed server; Raft assumes the client
    //         // has no persistent state. Once this server comes back online it will replay
    //         // the whole log to us.
    //         h.mu.Lock()
    //         h.commits[id] = h.commits[id][:0]
    //         h.mu.Unlock()
    //     }

    pub async fn crash_peer(&self, id: u64) {
        println!("crash {}", id);

        self.disconnect_peer(id).await;

        let id = id as usize;

        let mut harness = self.0.lock().await;
        harness.alive[id] = false;
        harness.cluster[id].shutdown().await;

        // Clear out the commits slice for the crashed server; Raft assumes the client
        // has no persistent state. Once this server comes back online it will replay
        // the whole log to us.
        harness.commits[id] = vec![];

        log::trace!("[{}] crash_peer commits: {:?}", id, harness.commits);
    }

    // // RestartPeer "restarts" a server by creating a new Server instance and giving
    // // it the appropriate storage, reconnecting it to peers.
    // func (h *Harness) RestartPeer(id int) {
    // 	if h.alive[id] {
    // 		log.Fatalf("id=%d is alive in RestartPeer", id)
    // 	}
    // 	tlog("Restart %d", id)

    // 	peerIds := make([]int, 0)
    // 	for p := 0; p < h.n; p++ {
    // 		if p != id {
    // 			peerIds = append(peerIds, p)
    // 		}
    // 	}

    // 	ready := make(chan interface{})
    // 	h.cluster[id] = NewServer(id, peerIds, h.storage[id], ready, h.commitChans[id])
    // 	h.cluster[id].Serve()
    // 	h.ReconnectPeer(id)
    // 	close(ready)
    // 	h.alive[id] = true
    // 	sleepMs(20)
    // }

    pub async fn restart_peer(&self, id: u64) {
        let id = id as usize;

        let mut harness = self.0.lock().await;

        assert!(!harness.alive[id]);

        println!("restart {}", id);

        let (ready_tx, ready_rx) = broadcast::channel(1);
        let (commit_channel_tx, commit_channel_rx) = mpsc::channel(1);
        let persistence = harness.persistences[id].clone();
        let node = RaftConsensus::new(id as u64, ready_rx, commit_channel_tx, persistence).await;

        let harness2 = self.clone();
        tokio::spawn(async move {
            harness2.collect_commits(id, commit_channel_rx).await;
        });

        harness.cluster[id] = node;
        harness.alive[id] = true;

        log::trace!("[{}] restart_peer commits: {:?}", id, harness.commits);

        drop(harness);
        self.reconnect_peer(id as u64).await;
        ready_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
