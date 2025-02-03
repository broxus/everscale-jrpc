use std::time::SystemTime;

pub mod jrpc;
pub mod proto;

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
    pub smallest_known_lt: Option<u64>,
}

impl Timings {
    pub fn is_reliable(
        &self,
        mc_acceptable_time_diff: u64,
        sc_acceptable_time_diff: u64,
        _acceptable_blocks_diff: u32,
    ) -> bool {
        // just booted up
        if self == &Self::default() {
            return false;
        }

        self.mc_time_diff.unsigned_abs() < mc_acceptable_time_diff
            && self.shard_client_time_diff.unsigned_abs() < sc_acceptable_time_diff
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
            smallest_known_lt: t.smallest_known_lt,
        }
    }
}

impl From<nekoton_proto::protos::rpc::response::GetTimings> for Timings {
    #[inline]
    fn from(t: nekoton_proto::protos::rpc::response::GetTimings) -> Self {
        Self {
            last_mc_block_seqno: t.last_mc_block_seqno,
            last_shard_client_mc_block_seqno: t.last_shard_client_mc_block_seqno,
            last_mc_utime: t.last_mc_utime,
            mc_time_diff: t.mc_time_diff,
            shard_client_time_diff: t.shard_client_time_diff,
            smallest_known_lt: if t.smallest_known_lt == 0 {
                None
            } else {
                Some(t.smallest_known_lt)
            },
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Timings;

    #[test]
    fn reliable() {
        let metrics = Timings {
            last_mc_block_seqno: 100,
            last_shard_client_mc_block_seqno: 0,
            last_mc_utime: 100,
            mc_time_diff: 0,
            shard_client_time_diff: 0,
            smallest_known_lt: None,
        };
        assert!(!metrics.is_reliable(0, 0, 0));

        let metrics = Timings {
            last_mc_block_seqno: 100,
            last_shard_client_mc_block_seqno: 0,
            last_mc_utime: 100,
            mc_time_diff: 150,
            shard_client_time_diff: 0,
            smallest_known_lt: None,
        };
        assert!(!metrics.is_reliable(0, 0, 0));
    }
}
