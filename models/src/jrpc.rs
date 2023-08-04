use nekoton_utils::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub trait Request: Serialize {
    type ResponseContainer: DeserializeOwned;
    type Response: From<Self::ResponseContainer>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetContractStateRequest {
    /// Address as string
    #[serde(with = "serde_address")]
    pub address: ton_block::MsgAddressInt,
    /// last transaction lt on this account
    #[serde(default, with = "serde_optional_u64")]
    pub last_transaction_lt: Option<u64>,
}

impl Request for GetContractStateRequest {
    type ResponseContainer = Self::Response;
    type Response = GetContractStateResponse;
}

#[derive(Debug, Clone, Serialize)]
pub struct GetContractStateRequestRef<'a> {
    /// Address as string
    #[serde(with = "serde_address")]
    pub address: &'a ton_block::MsgAddressInt,
}

impl Request for GetContractStateRequestRef<'_> {
    type ResponseContainer = Self::Response;
    type Response = GetContractStateResponse;
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum GetContractStateResponse {
    NotExists,
    #[serde(rename_all = "camelCase")]
    Exists {
        /// Base64 encoded account data
        #[serde(with = "serde_account_stuff")]
        account: ton_block::AccountStuff,
        timings: nekoton_abi::GenTimings,
        last_transaction_id: nekoton_abi::LastTransactionId,
    },
    Unchanged {
        timings: nekoton_abi::GenTimings,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    /// Base64 encoded message
    #[serde(with = "serde_ton_block")]
    pub message: ton_block::Message,
}

impl Request for SendMessageRequest {
    type ResponseContainer = Self::Response;
    type Response = ();
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct GetLatestKeyBlockRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLatestKeyBlockResponse {
    /// Base64 encoded block
    #[serde(with = "serde_ton_block")]
    pub block: ton_block::Block,
}

impl Request for GetLatestKeyBlockRequest {
    type ResponseContainer = Self::Response;
    type Response = GetLatestKeyBlockResponse;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct GetBlockchainConfigRequest;

impl Request for GetBlockchainConfigRequest {
    type ResponseContainer = Self::Response;
    type Response = GetBlockchainConfigResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockchainConfigResponse {
    pub global_id: i32,
    #[serde(with = "serde_ton_block")]
    pub config: ton_block::ConfigParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionsListRequest {
    #[serde(with = "serde_address")]
    pub account: ton_block::MsgAddressInt,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_optional_u64"
    )]
    pub last_transaction_lt: Option<u64>,

    pub limit: u8,
}

impl Request for GetTransactionsListRequest {
    type ResponseContainer = GetTransactionsListResponse;
    type Response = Vec<ton_block::Transaction>;
}

#[derive(Debug, Clone)]
pub struct GetTransactionsListResponse(pub Vec<ton_block::Transaction>);

impl From<GetTransactionsListResponse> for Vec<ton_block::Transaction> {
    #[inline]
    fn from(value: GetTransactionsListResponse) -> Self {
        value.0
    }
}

impl Serialize for GetTransactionsListResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        #[derive(Serialize)]
        struct Item<'a>(#[serde(with = "serde_ton_block")] &'a ton_block::Transaction);

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for transaction in &self.0 {
            seq.serialize_element(&Item(transaction))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for GetTransactionsListResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[repr(transparent)]
        struct Item(#[serde(with = "serde_ton_block")] ton_block::Transaction);

        Ok(Self(
            Vec::<Item>::deserialize(deserializer)?
                .into_iter()
                .map(|Item(tx)| tx)
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionsListRequestRef<'a> {
    #[serde(with = "serde_address")]
    pub account: &'a ton_block::MsgAddressInt,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_optional_u64"
    )]
    pub last_transaction_lt: Option<u64>,

    pub limit: u8,
}

impl Request for GetTransactionsListRequestRef<'_> {
    type ResponseContainer = GetTransactionsListResponse;
    type Response = Vec<ton_block::Transaction>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionRequest {
    #[serde(with = "serde_hex_array")]
    pub id: [u8; 32],
}

impl Request for GetTransactionRequest {
    type ResponseContainer = GetTransactionResponse;
    type Response = Option<ton_block::Transaction>;
}

#[derive(Debug, Clone)]
pub struct GetTransactionResponse(pub Option<ton_block::Transaction>);

impl From<GetTransactionResponse> for Option<ton_block::Transaction> {
    #[inline]
    fn from(value: GetTransactionResponse) -> Self {
        value.0
    }
}

impl Serialize for GetTransactionResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "serde_ton_block")] &'a ton_block::Transaction);

        self.0.as_ref().map(Helper).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GetTransactionResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "serde_ton_block")] ton_block::Transaction);

        Ok(Self(
            Option::<Helper>::deserialize(deserializer)?.map(|Helper(tx)| tx),
        ))
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTransactionRequestRef<'a> {
    #[serde(with = "serde_hex_array")]
    pub id: &'a [u8; 32],
}

impl Request for GetTransactionRequestRef<'_> {
    type ResponseContainer = GetTransactionResponse;
    type Response = Option<ton_block::Transaction>;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDstTransactionRequest {
    #[serde(with = "serde_hex_array")]
    pub message_hash: [u8; 32],
}

impl Request for GetDstTransactionRequest {
    type ResponseContainer = GetDstTransactionResponse;
    type Response = Option<ton_block::Transaction>;
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetDstTransactionRequestRef<'a> {
    #[serde(with = "serde_hex_array")]
    pub message_hash: &'a [u8; 32],
}

#[derive(Debug, Clone)]
pub struct GetDstTransactionResponse(pub Option<ton_block::Transaction>);

impl From<GetDstTransactionResponse> for Option<ton_block::Transaction> {
    #[inline]
    fn from(value: GetDstTransactionResponse) -> Self {
        value.0
    }
}

impl Serialize for GetDstTransactionResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a>(#[serde(with = "serde_ton_block")] &'a ton_block::Transaction);

        self.0.as_ref().map(Helper).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GetDstTransactionResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper(#[serde(with = "serde_ton_block")] ton_block::Transaction);

        Ok(Self(
            Option::<Helper>::deserialize(deserializer)?.map(|Helper(tx)| tx),
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAccountsByCodeHashRequest {
    #[serde(with = "serde_hex_array")]
    pub code_hash: [u8; 32],

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_optional_address"
    )]
    pub continuation: Option<ton_block::MsgAddressInt>,

    pub limit: u8,
}

impl Request for GetAccountsByCodeHashRequest {
    type ResponseContainer = GetAccountsByCodeHashResponse;
    type Response = Vec<ton_block::MsgAddressInt>;
}

#[derive(Debug, Clone)]
pub struct GetAccountsByCodeHashResponse(pub Vec<ton_block::MsgAddressInt>);

impl From<GetAccountsByCodeHashResponse> for Vec<ton_block::MsgAddressInt> {
    #[inline]
    fn from(value: GetAccountsByCodeHashResponse) -> Self {
        value.0
    }
}

impl Serialize for GetAccountsByCodeHashResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        #[derive(Serialize)]
        struct Item<'a>(#[serde(with = "serde_address")] &'a ton_block::MsgAddressInt);

        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for address in &self.0 {
            seq.serialize_element(&Item(address))?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for GetAccountsByCodeHashResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[repr(transparent)]
        struct Item(#[serde(with = "serde_address")] ton_block::MsgAddressInt);

        Ok(Self(
            Vec::<Item>::deserialize(deserializer)?
                .into_iter()
                .map(|Item(tx)| tx)
                .collect(),
        ))
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct GetStatusRequest;

impl Request for GetStatusRequest {
    type ResponseContainer = Self::Response;
    type Response = GetStatusResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetStatusResponse {
    pub ready: bool,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct GetTimingsRequest;

impl Request for GetTimingsRequest {
    type ResponseContainer = Self::Response;
    type Response = GetTimingsResponse;
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Default, Eq, PartialEq)]
pub struct GetTimingsResponse {
    pub last_mc_block_seqno: u32,
    pub last_shard_client_mc_block_seqno: u32,
    pub last_mc_utime: u32,
    pub mc_time_diff: i64,
    pub shard_client_time_diff: i64,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{now, Timings};

    #[test]
    fn older_than() {
        let metrics = GetTimingsResponse {
            last_mc_block_seqno: 0,
            last_shard_client_mc_block_seqno: 0,
            last_mc_utime: now() as u32,
            mc_time_diff: 100,
            shard_client_time_diff: 100,
        };

        assert!(Timings::from(metrics).has_state_for(0));
        assert!(!Timings::from(metrics).has_state_for(now() as u32 - 1));
        assert!(!Timings::from(metrics).has_state_for(now() as u32 - 99));
        assert!(Timings::from(metrics).has_state_for(now() as u32 - 101));
    }
}
