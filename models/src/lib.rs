use nekoton_utils::*;
use serde::{Deserialize, Serialize};

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
