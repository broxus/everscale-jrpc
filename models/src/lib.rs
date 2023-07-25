use std::time::SystemTime;

use nekoton_utils::*;

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

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Timings {
    pub last_mc_block_seqno: u32,
    pub last_shard_client_mc_block_seqno: u32,
    pub last_mc_utime: u32,
    pub mc_time_diff: i64,
    pub shard_client_time_diff: i64,
}

impl Timings {
    pub fn is_reliable(&self) -> bool {
        // just booted up
        if self == &Self::default() {
            return false;
        }

        let acceptable_time = (now_sec_u64() - ACCEPTABLE_NODE_BLOCK_INSERT_TIME) as u32;

        self.mc_time_diff.unsigned_abs() < MC_ACCEPTABLE_TIME_DIFF
            && self.shard_client_time_diff.unsigned_abs() < SC_ACCEPTABLE_TIME_DIFF
            && self.last_mc_block_seqno - self.last_shard_client_mc_block_seqno
                < ACCEPTABLE_BLOCKS_DIFF
            && self.last_mc_utime > acceptable_time
    }

    pub fn has_state_for(&self, time: u32) -> bool {
        let now = now();

        self.last_mc_utime > time && (now - self.shard_client_time_diff as u64) > time as u64
    }
}

impl PartialOrd for Timings {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Timings {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.shard_client_time_diff, self.mc_time_diff)
            .cmp(&(other.shard_client_time_diff, other.mc_time_diff))
    }
}

impl From<jrpc::GetTimingsResponse> for Timings {
    #[inline]
    fn from(t: jrpc::GetTimingsResponse) -> Self {
        Self {
            last_mc_block_seqno: t.last_mc_block_seqno,
            last_shard_client_mc_block_seqno: t.last_shard_client_mc_block_seqno,
            last_mc_utime: t.last_mc_utime,
            mc_time_diff: t.mc_time_diff,
            shard_client_time_diff: t.shard_client_time_diff,
        }
    }
}

impl From<everscale_rpc_proto::rpc::response::GetTimings> for Timings {
    #[inline]
    fn from(t: everscale_rpc_proto::rpc::response::GetTimings) -> Self {
        Self {
            last_mc_block_seqno: t.last_mc_block_seqno,
            last_shard_client_mc_block_seqno: t.last_shard_client_mc_block_seqno,
            last_mc_utime: t.last_mc_utime,
            mc_time_diff: t.mc_time_diff,
            shard_client_time_diff: t.shard_client_time_diff,
        }
    }
}
