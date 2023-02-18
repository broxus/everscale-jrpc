use nekoton_utils::*;
use serde::{Deserialize, Serialize};

const MC_ACCEPTABLE_TIME_DIFF: u64 = 120;
const SC_ACCEPTABLE_TIME_DIFF: u64 = 120;
const ACCEPTABLE_BLOCKS_DIFF: u32 = 10;

const ACCEPTABLE_NODE_BLOCK_INSERT_TIME: u64 = 240;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetContractStateRequest {
    /// Address as string
    #[serde(with = "serde_address")]
    pub address: ton_block::MsgAddressInt,
}

#[derive(Debug, Clone, Serialize)]
pub struct GetContractStateRequestRef<'a> {
    /// Address as string
    #[serde(with = "serde_address")]
    pub address: &'a ton_block::MsgAddressInt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    /// Base64 encoded message
    #[serde(with = "serde_ton_block")]
    pub message: ton_block::Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockResponse {
    /// Base64 encoded block
    #[serde(with = "serde_ton_block")]
    pub block: ton_block::Block,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum ContractStateResponse {
    NotExists,
    #[serde(rename_all = "camelCase")]
    Exists {
        /// Base64 encoded account data
        #[serde(with = "serde_account_stuff")]
        account: ton_block::AccountStuff,
        timings: nekoton_abi::GenTimings,
        last_transaction_id: nekoton_abi::LastTransactionId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct EngineMetrics {
    pub last_mc_block_seqno: u32,
    pub last_shard_client_mc_block_seqno: u32,
    pub last_mc_utime: u32,
    pub mc_time_diff: i64,
    pub shard_client_time_diff: i64,
}

impl EngineMetrics {
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
}

#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionRequest<'a> {
    #[serde(with = "serde_hex_array")]
    pub id: &'a [u8; 32],
}
