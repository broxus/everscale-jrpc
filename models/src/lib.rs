use std::time::SystemTime;

pub mod jrpc;

pub const MC_ACCEPTABLE_TIME_DIFF: u64 = 120;
pub const SC_ACCEPTABLE_TIME_DIFF: u64 = 120;
pub const ACCEPTABLE_BLOCKS_DIFF: u32 = 10;

pub const ACCEPTABLE_NODE_BLOCK_INSERT_TIME: u64 = 240;

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
