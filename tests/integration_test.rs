mod harness;

use harness::Harness;
use log::LevelFilter;
use raft::Logger;
use std::{process::Command, sync::Once, time::Duration};

static LOGGER: Logger = Logger;
static INIT: Once = Once::new();

fn setup_tracing() {
    INIT.call_once(|| {
        // use tracing_subscriber::prelude::*;
        // tracing_subscriber::registry()
        //     .with(console_subscriber::spawn())
        //     .with(tracing_subscriber::fmt::layer())
        //     .init();

        // tracing_subscriber::fmt::init();
        log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace))
            .unwrap();
        // console_subscriber::init();
    })
}

fn free_ports(num_servers: usize) {
    let ports = (0..num_servers).map(|i| raft::PORT_BASE + i as u64);
    #[cfg(target_os = "macos")]
    {
        // kill -9 $(lsof -ti:3000,3001)
        for port in ports {
            let output = Command::new("lsof")
                .arg(format!("-ti:{}", port))
                .output()
                .unwrap();
            if output.status.success() {
                // port is currently being used
                println!("{:?}", output);
                let pid = String::from_utf8(output.stdout).unwrap();
                let output = Command::new("kill").arg("-9").arg(pid).output().unwrap();
                if output.status.success() {
                    println!("successfully killed process running on port {port}");
                }
            }
        }
    }
}

// to be called before every test
fn setup(num_servers: usize) {
    setup_tracing();
    free_ports(num_servers);
    std::thread::sleep(Duration::from_secs(2));
}

// WARNING: Run the tests in a sequential manner
// pass `--test-threads=1` to `cargo t`

#[tokio::test]
async fn raft_basic() {
    setup(3);

    let harness = Harness::new(3).await;
    let leader = harness.check_single_leader().await;
    assert!(leader.is_some());
}

#[tokio::test]
async fn raft_election_leader_disconnect() {
    setup(3);

    let harness = Harness::new(3).await;
    let (old_leader_id, old_leader_term) = harness.check_single_leader().await.unwrap();

    harness.disconnect_peer(old_leader_id).await;
    tokio::time::sleep(Duration::from_millis(350)).await;

    let (new_leader_id, new_leader_term) = harness.check_single_leader().await.unwrap();

    assert_ne!(old_leader_id, new_leader_id);
    assert!(new_leader_term > old_leader_term);
}

#[tokio::test]
async fn raft_election_leader_and_another_disconnect() {
    setup(3);

    let harness = Harness::new(3).await;
    let (old_leader_id, _) = harness.check_single_leader().await.unwrap();

    harness.disconnect_peer(old_leader_id).await;
    let other_id = (old_leader_id + 1) % 3;
    harness.disconnect_peer(other_id).await;
    tokio::time::sleep(Duration::from_millis(450)).await;

    assert!(harness.check_no_leader().await);

    harness.reconnect_peer(other_id).await;
    assert!(harness.check_single_leader().await.is_some());
}

#[tokio::test]
async fn raft_disconnect_all_and_then_restore() {
    setup(3);

    let harness = Harness::new(3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..3 {
        harness.disconnect_peer(i).await;
    }
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(harness.check_no_leader().await);

    for i in 0..3 {
        harness.reconnect_peer(i).await;
    }
    assert!(harness.check_single_leader().await.is_some());
}

#[tokio::test]
async fn raft_election_leader_disconnect_then_reconnect() {
    setup(3);

    let harness = Harness::new(3).await;
    let (old_leader_id, _) = harness.check_single_leader().await.unwrap();

    harness.disconnect_peer(old_leader_id).await;
    tokio::time::sleep(Duration::from_millis(350)).await;

    let (new_leader_id, new_leader_term) = harness.check_single_leader().await.unwrap();

    harness.reconnect_peer(old_leader_id).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (again_leader_id, again_leader_term) = harness.check_single_leader().await.unwrap();

    assert_eq!(new_leader_id, again_leader_id);
    assert_eq!(new_leader_term, again_leader_term);
}

#[tokio::test]
async fn raft_election_leader_disconnect_then_reconnect5() {
    setup(5);

    let harness = Harness::new(5).await;
    let (old_leader_id, _) = harness.check_single_leader().await.unwrap();

    harness.disconnect_peer(old_leader_id).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (new_leader_id, new_leader_term) = harness.check_single_leader().await.unwrap();

    harness.reconnect_peer(old_leader_id).await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (again_leader_id, again_leader_term) = harness.check_single_leader().await.unwrap();

    assert_eq!(new_leader_id, again_leader_id);
    assert_eq!(new_leader_term, again_leader_term);
}

// #[tokio::test]
// async fn raft_election_follower_comes_back() {
//     setup(3);

//     let mut harness = Harness::new(3).await;
//     let (old_leader_id, old_leader_term) = harness.check_single_leader().await.unwrap();

//     let other_id = (old_leader_id +1 ) % 3;

//     harness.disconnect_peer(other_id).await;
//     tokio::time::sleep(Duration::from_millis(650)).await;

//     harness.reconnect_peer(other_id).await;
//     tokio::time::sleep(Duration::from_millis(150)).await;

// }

#[tokio::test]
async fn raft_election_disconnect_loop() {
    setup(3);

    let harness = Harness::new(3).await;

    for _ in 0..5 {
        let (leader_id, _) = harness.check_single_leader().await.unwrap();
        harness.disconnect_peer(leader_id).await;

        let other_id = (leader_id + 1) % 3;
        harness.disconnect_peer(other_id).await;
        tokio::time::sleep(Duration::from_millis(310)).await;

        assert!(harness.check_no_leader().await);

        harness.reconnect_peer(leader_id).await;
        harness.reconnect_peer(other_id).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn raft_commit_one_command() {
    setup(3);

    let harness = Harness::new(3).await;
    let (leader_id, _) = harness.check_single_leader().await.unwrap();

    let command = "42".to_string();
    let is_leader = harness.submit_to_server(leader_id, command.clone()).await;
    assert!(is_leader);

    tokio::time::sleep(Duration::from_millis(250)).await;

    harness.check_committed_n(command, 3).await;
}

#[tokio::test]
async fn raft_submit_non_leader_fails() {
    setup(3);

    let harness = Harness::new(3).await;
    let (leader_id, _) = harness.check_single_leader().await.unwrap();

    let other_id = (leader_id + 1) % 3;
    let command = "42".to_string();
    let is_leader = harness.submit_to_server(other_id, command.clone()).await;
    assert!(!is_leader);

    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::test]
async fn raft_commit_multiple_commands() {
    setup(3);

    let harness = Harness::new(3).await;
    let (leader_id, _) = harness.check_single_leader().await.unwrap();

    let values = vec!["42", "55", "81"];
    for value in values {
        let is_leader = harness.submit_to_server(leader_id, value.to_string()).await;
        assert!(is_leader);

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;

    let (nc, i1) = harness.check_committed("42".to_string()).await;
    let (_, i2) = harness.check_committed("55".to_string()).await;
    let (_, i3) = harness.check_committed("81".to_string()).await;

    assert_eq!(nc, 3);
    assert!(i1 < i2);
    assert!(i2 < i3);
}

#[tokio::test]
async fn raft_commit_with_disconnection_and_recover() {
    setup(3);

    let harness = Harness::new(3).await;

    // Submit a couple of values to a fully connected cluster
    let (leader_id, _) = harness.check_single_leader().await.unwrap();
    harness.submit_to_server(leader_id, "5".to_string()).await;
    harness.submit_to_server(leader_id, "6".to_string()).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    harness.check_committed_n("6".to_string(), 3).await;

    let other_id = (leader_id + 1) % 3;
    harness.disconnect_peer(other_id).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Submit a new command; it will be committed but only to two servers
    harness.submit_to_server(leader_id, "7".to_string()).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("7".to_string(), 2).await;

    // Now reconnect dPeerId and wait a bit; it should find the new command too
    harness.reconnect_peer(other_id).await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_single_leader().await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    harness.check_committed_n("7".to_string(), 3).await;
}

#[tokio::test]
async fn raft_no_commit_with_no_quorum() {
    setup(3);

    let harness = Harness::new(3).await;

    // Submit a couple of values to a fully connected cluster
    let (original_leader_id, original_term) = harness.check_single_leader().await.unwrap();
    harness
        .submit_to_server(original_leader_id, "5".to_string())
        .await;
    harness
        .submit_to_server(original_leader_id, "6".to_string())
        .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("6".to_string(), 3).await;

    // Disconnect both followers
    let follower1 = (original_leader_id + 1) % 3;
    let follower2 = (original_leader_id + 2) % 3;
    harness.disconnect_peer(follower1).await;
    harness.disconnect_peer(follower2).await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    harness
        .submit_to_server(original_leader_id, "8".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_not_committed("8".to_string()).await;

    // Reconnect both other servers, we'll have quorum now
    harness.reconnect_peer(follower1).await;
    harness.reconnect_peer(follower2).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    // 8 is still not committed because the term has changed
    harness.check_not_committed("8".to_string()).await;

    let (new_leader_id, new_term) = harness.check_single_leader().await.unwrap();
    assert_ne!(original_term, new_term);

    let values = ["9", "10", "11"];
    for value in values {
        harness
            .submit_to_server(new_leader_id, value.to_string())
            .await;
    }
    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }
}

#[tokio::test]
async fn raft_disconnect_leader_briefly() {
    setup(3);

    let harness = Harness::new(3).await;

    // Submit a couple of values to a fully connected cluster
    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness
        .submit_to_server(original_leader_id, "5".to_string())
        .await;
    harness
        .submit_to_server(original_leader_id, "6".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("6".to_string(), 3).await;

    // Disconnect leader for a short time (less than election timeout in peers)
    harness.disconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(90)).await;
    harness.reconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    harness
        .submit_to_server(original_leader_id, "7".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("7".to_string(), 3).await;
}

#[tokio::test]
async fn raft_test_commits_with_leader_disconnects() {
    setup(5);

    let harness = Harness::new(5).await;

    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness
        .submit_to_server(original_leader_id, "5".to_string())
        .await;
    harness
        .submit_to_server(original_leader_id, "6".to_string())
        .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("6".to_string(), 5).await;

    // Leader disconnected
    harness.disconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Submit 7 to original leader, even though it's disconnected.
    harness
        .submit_to_server(original_leader_id, "7".to_string())
        .await;

    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_not_committed("7".to_string()).await;

    let (new_leader_id, _) = harness.check_single_leader().await.unwrap();

    // Submit 8 to new leader.
    harness
        .submit_to_server(new_leader_id, "8".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("8".to_string(), 4).await;

    // Reconnect old leader and let it settle. The old leader shouldn't be the one winning
    // election safety property
    harness.reconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    let (final_leader_id, _) = harness.check_single_leader().await.unwrap();
    assert_ne!(final_leader_id, original_leader_id);

    // Submit 9 and check it's fully committed.
    harness
        .submit_to_server(final_leader_id, "9".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_committed_n("9".to_string(), 5).await;
    harness.check_committed_n("8".to_string(), 5).await;

    // But 7 is not committed
    harness.check_not_committed("7".to_string()).await;
}

#[tokio::test]
async fn raft_crash_follower() {
    setup(3);

    let harness = Harness::new(3).await;

    // Submit a couple of values to a fully connected cluster
    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness
        .submit_to_server(original_leader_id, "5".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    harness.check_committed_n("5".to_string(), 3).await;

    let follower = (original_leader_id + 1) % 3;
    harness.crash_peer(follower).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    harness.check_committed_n("5".to_string(), 2).await;
}

#[tokio::test]
async fn raft_crash_then_restart_follower() {
    setup(3);

    let harness = Harness::new(3).await;

    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();

    let values = ["5", "6", "7"];
    for value in values {
        harness
            .submit_to_server(original_leader_id, value.to_string())
            .await;
    }

    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }

    let follower = (original_leader_id + 1) % 3;

    harness.crash_peer(follower).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 2).await;
    }

    // Restart the crashed follower and give it some time to come up-to-date
    harness.restart_peer(follower).await;
    tokio::time::sleep(Duration::from_millis(650)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }
}

#[tokio::test]
async fn raft_crash_then_restart_leader() {
    setup(3);

    let harness = Harness::new(3).await;

    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();

    let values = ["5", "6", "7"];
    for value in values {
        harness
            .submit_to_server(original_leader_id, value.to_string())
            .await;
    }

    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }

    harness.crash_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 2).await;
    }

    harness.restart_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(550)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }
}

#[tokio::test]
async fn raft_crash_then_restart_all() {
    setup(3);

    let harness = Harness::new(3).await;

    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();

    let values = ["5", "6", "7"];
    for value in values {
        harness
            .submit_to_server(original_leader_id, value.to_string())
            .await;
    }

    tokio::time::sleep(Duration::from_millis(350)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }

    for id in 0..3 {
        harness.crash_peer(id).await;
    }
    tokio::time::sleep(Duration::from_millis(350)).await;

    for id in 0..3 {
        harness.restart_peer(id).await;
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (new_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness
        .submit_to_server(new_leader_id, "8".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    for value in values.into_iter().chain(["8"]) {
        harness.check_committed_n(value.to_string(), 3).await;
    }
}

#[tokio::test]
async fn raft_replace_multiple_log_entries() {
    setup(3);

    let harness = Harness::new(3).await;

    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();

    // Submit a couple of values to a fully connected cluster
    let values = ["5", "6"];
    for value in values {
        harness
            .submit_to_server(original_leader_id, value.to_string())
            .await;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    for value in values {
        harness.check_committed_n(value.to_string(), 3).await;
    }

    // Leader disconnected
    harness.disconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Submit a few entries to the original leader; it's disconnected, so they won't be replicated
    let values = ["21", "22", "23", "24"];
    for value in values {
        harness
            .submit_to_server(original_leader_id, value.to_string())
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (new_leader_id, _) = harness.check_single_leader().await.unwrap();

    // Submit entries to new leader -- these will be replicated
    let values = ["8", "9", "10"];
    for value in values {
        harness
            .submit_to_server(new_leader_id, value.to_string())
            .await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    harness.check_not_committed("21".to_string()).await;
    harness.check_committed_n("10".to_string(), 2).await;

    // Crash/restart new leader to reset its nextIndex, to ensure that the new
    // leader of the cluster (could be the third server after elections) tries
    // to replace the original's servers unreplicated entries from the very end
    harness.crash_peer(new_leader_id).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    harness.restart_peer(new_leader_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let (final_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness.reconnect_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Submit another entry; this is because leaders won't commit entries from
    // previous terms (paper 5.4.2) so the 8,9,10 may not be committed everywhere
    // after the restart before a new command comes it
    harness
        .submit_to_server(final_leader_id, "11".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    // At this point, 11 and 10 should be replicated everywhere; 21 won't be
    harness.check_not_committed("21".to_string()).await;
    harness.check_committed_n("11".to_string(), 3).await;
    harness.check_committed_n("10".to_string(), 3).await;
}

#[tokio::test]
async fn raft_crash_after_submit() {
    setup(3);

    let harness = Harness::new(3).await;

    // Wait for a leader to emerge, and submit a command - then immediately
    // crash; the leader should have no time to send an updated LeaderCommit
    // to followers. It doesn't have time to get back AE responses either, so
    // the leader itself won't send it on the commit channel
    let (original_leader_id, _) = harness.check_single_leader().await.unwrap();

    harness
        .submit_to_server(original_leader_id, "5".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(1)).await;
    harness.crash_peer(original_leader_id).await;

    // Make sure 5 is not committed when a new leader is elected. Leaders won't
    // commit commands from previous terms
    tokio::time::sleep(Duration::from_millis(10)).await;
    harness.check_single_leader().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    harness.check_not_committed("5".to_string()).await;

    // The old leader restarts. After a while, 5 is still not committed.
    harness.restart_peer(original_leader_id).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (new_leader_id, _) = harness.check_single_leader().await.unwrap();
    harness.check_not_committed("5".to_string()).await;

    // When we submit a new command, it will be submitted, and so will 5, because
    // it appears in everyone's logs.
    harness
        .submit_to_server(new_leader_id, "6".to_string())
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    harness.check_committed_n("5".to_string(), 3).await;
    harness.check_committed_n("6".to_string(), 3).await;
}
