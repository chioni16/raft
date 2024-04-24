mod harness;

use harness::Harness;
use log::LevelFilter;
use raft::Logger;
use std::{
    process::{Command, ExitStatus},
    sync::Once,
    time::Duration,
};

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
// pass `--test-threads=1` to `cargo c`

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

    let mut harness = Harness::new(3).await;
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

    let mut harness = Harness::new(3).await;
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

    let mut harness = Harness::new(3).await;
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

    let mut harness = Harness::new(3).await;
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

    let mut harness = Harness::new(5).await;
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

    let mut harness = Harness::new(3).await;

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
