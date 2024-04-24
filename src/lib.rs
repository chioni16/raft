pub mod consensus;
mod raftlog;
mod rpc;
pub mod state;

const RAFT_FORCE_MORE_REELECTION: &str = "RAFT_FORCE_MORE_REELECTION";
const RAFT_UNRELIABLE_RPC: &str = "RAFT_UNRELIABLE_RPC";
pub const PORT_BASE: u64 = 50050;

// [start, end)
fn random(low: u64, high: u64) -> u64 {
    use rand::distributions::{Distribution, Uniform};
    let mut rng = rand::thread_rng();
    let range = Uniform::new(low, high);
    range.sample(&mut rng)
}

use log::{Level, Metadata, Record};

pub struct Logger;
impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Trace
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "[{}:{}] {}",
                record.file().unwrap(),
                record.line().unwrap(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}

// use once_cell::sync::Lazy;

// static LOGGER: Lazy<Logger> = Lazy::new(|| {
//     let logger = Logger;
//     log::set_logger(&logger)
//         .map(|()| log::set_max_level(LevelFilter::Trace))
//         .unwrap();
//     logger
// });
