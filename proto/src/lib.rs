pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

pub use prost;

impl rpc::response::GetTimings {
    pub fn is_reliable(&self) -> bool {
        // just booted up
        if self == &Self::default() {
            return false;
        }

        let acceptable_time = (nekoton::utils::now_sec_u64()
            - everscale_jrpc_models::ACCEPTABLE_NODE_BLOCK_INSERT_TIME)
            as u32;

        self.mc_time_diff.unsigned_abs() < everscale_jrpc_models::MC_ACCEPTABLE_TIME_DIFF
            && self.shard_client_time_diff.unsigned_abs()
                < everscale_jrpc_models::SC_ACCEPTABLE_TIME_DIFF
            && self.last_mc_block_seqno - self.last_shard_client_mc_block_seqno
                < everscale_jrpc_models::ACCEPTABLE_BLOCKS_DIFF
            && self.last_mc_utime > acceptable_time
    }

    pub fn has_state_for(&self, time: u32) -> bool {
        let now = everscale_jrpc_models::now();

        self.last_mc_utime > time && (now - self.shard_client_time_diff as u64) > time as u64
    }
}

impl PartialOrd for rpc::response::GetTimings {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for rpc::response::GetTimings {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.shard_client_time_diff, self.mc_time_diff)
            .cmp(&(other.shard_client_time_diff, other.mc_time_diff))
    }
}
