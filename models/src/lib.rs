use nekoton_utils::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetContractStateRequest {
    /// Address as string
    #[serde(with = "serde_address")]
    pub address: ton_block::MsgAddressInt,
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

impl PartialEq<Self> for ExistingContract {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for ExistingContract {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self.timings, other.timings) {
            (
                nekoton_abi::GenTimings::Known { gen_lt, .. },
                nekoton_abi::GenTimings::Known {
                    gen_lt: other_gen_lt,
                    ..
                },
            ) => gen_lt.partial_cmp(&other_gen_lt),
            _ => None,
        }
    }
}
