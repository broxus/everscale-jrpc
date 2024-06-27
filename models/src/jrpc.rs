use nekoton_utils::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use serde::de::{self, MapAccess, Visitor};
use serde::Deserializer;
use std::fmt;

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
    /// last transaction lt on this account
    #[serde(default, with = "serde_optional_u64")]
    pub last_transaction_lt: Option<u64>,
}

impl Request for GetContractStateRequestRef<'_> {
    type ResponseContainer = Self::Response;
    type Response = GetContractStateResponse;
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum GetContractStateResponse {
    NotExists {
        timings: nekoton_abi::GenTimings,
    },
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

#[derive(Debug, Copy, Clone, Serialize, Default, Eq, PartialEq)]
pub struct GetTimingsResponse {
    pub last_mc_block_seqno: u32,
    pub last_shard_client_mc_block_seqno: u32,
    pub last_mc_utime: u32,
    pub mc_time_diff: i64,
    pub shard_client_time_diff: i64,
    pub smallest_known_lt: Option<u64>,
}

impl<'de> Deserialize<'de> for GetTimingsResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            LastMcBlockSeqno,
            LastShardClientMcBlockSeqno,
            LastMcUtime,
            McTimeDiff,
            ShardClientTimeDiff,
            SmallestKnownLt,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("field identifier")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "last_mc_block_seqno" | "lastMcBlockSeqno" => {
                                Ok(Field::LastMcBlockSeqno)
                            }
                            "last_shard_client_mc_block_seqno" | "lastShardClientMcBlockSeqno" => {
                                Ok(Field::LastShardClientMcBlockSeqno)
                            }
                            "last_mc_utime" | "lastMcUtime" => Ok(Field::LastMcUtime),
                            "mc_time_diff" | "mcTimeDiff" => Ok(Field::McTimeDiff),
                            "shard_client_time_diff" | "shardClientTimeDiff" => {
                                Ok(Field::ShardClientTimeDiff)
                            }
                            "smallest_known_lt" | "smallestKnownLt" => Ok(Field::SmallestKnownLt),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct GetTimingsResponseVisitor;

        impl<'de> Visitor<'de> for GetTimingsResponseVisitor {
            type Value = GetTimingsResponse;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct GetTimingsResponse")
            }

            fn visit_map<V>(self, mut map: V) -> Result<GetTimingsResponse, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut last_mc_block_seqno = None;
                let mut last_shard_client_mc_block_seqno = None;
                let mut last_mc_utime = None;
                let mut mc_time_diff = None;
                let mut shard_client_time_diff = None;
                let mut smallest_known_lt = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::LastMcBlockSeqno => {
                            if last_mc_block_seqno.is_some() {
                                return Err(de::Error::duplicate_field("last_mc_block_seqno"));
                            }
                            last_mc_block_seqno = Some(map.next_value()?);
                        }
                        Field::LastShardClientMcBlockSeqno => {
                            if last_shard_client_mc_block_seqno.is_some() {
                                return Err(de::Error::duplicate_field(
                                    "last_shard_client_mc_block_seqno",
                                ));
                            }
                            last_shard_client_mc_block_seqno = Some(map.next_value()?);
                        }
                        Field::LastMcUtime => {
                            if last_mc_utime.is_some() {
                                return Err(de::Error::duplicate_field("last_mc_utime"));
                            }
                            last_mc_utime = Some(map.next_value()?);
                        }
                        Field::McTimeDiff => {
                            if mc_time_diff.is_some() {
                                return Err(de::Error::duplicate_field("mc_time_diff"));
                            }
                            mc_time_diff = Some(map.next_value()?);
                        }
                        Field::ShardClientTimeDiff => {
                            if shard_client_time_diff.is_some() {
                                return Err(de::Error::duplicate_field("shard_client_time_diff"));
                            }
                            shard_client_time_diff = Some(map.next_value()?);
                        }
                        Field::SmallestKnownLt => {
                            if smallest_known_lt.is_some() {
                                return Err(de::Error::duplicate_field("smallest_known_lt"));
                            }
                            smallest_known_lt = Some(map.next_value()?);
                        }
                    }
                }

                let last_mc_block_seqno = last_mc_block_seqno
                    .ok_or_else(|| de::Error::missing_field("last_mc_block_seqno"))?;
                let last_shard_client_mc_block_seqno = last_shard_client_mc_block_seqno
                    .ok_or_else(|| de::Error::missing_field("last_shard_client_mc_block_seqno"))?;
                let last_mc_utime =
                    last_mc_utime.ok_or_else(|| de::Error::missing_field("last_mc_utime"))?;
                let mc_time_diff =
                    mc_time_diff.ok_or_else(|| de::Error::missing_field("mc_time_diff"))?;
                let shard_client_time_diff = shard_client_time_diff
                    .ok_or_else(|| de::Error::missing_field("shard_client_time_diff"))?;

                Ok(GetTimingsResponse {
                    last_mc_block_seqno,
                    last_shard_client_mc_block_seqno,
                    last_mc_utime,
                    mc_time_diff,
                    shard_client_time_diff,
                    smallest_known_lt,
                })
            }
        }

        const FIELDS: &[&str] = &[
            "last_mc_block_seqno",
            "last_shard_client_mc_block_seqno",
            "last_mc_utime",
            "mc_time_diff",
            "shard_client_time_diff",
            "smallest_known_lt",
        ];

        deserializer.deserialize_struct("GetTimingsResponse", FIELDS, GetTimingsResponseVisitor)
    }
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
            smallest_known_lt: None,
        };

        assert!(Timings::from(metrics).has_state_for(0));
        assert!(!Timings::from(metrics).has_state_for(now() as u32 - 1));
        assert!(!Timings::from(metrics).has_state_for(now() as u32 - 99));
        assert!(Timings::from(metrics).has_state_for(now() as u32 - 101));
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn test_deserialize_snake_case() {
        let json = json!({
            "last_mc_block_seqno": 123,
            "last_shard_client_mc_block_seqno": 456,
            "last_mc_utime": 789,
            "mc_time_diff": 1000,
            "shard_client_time_diff": 2000,
            "smallest_known_lt": 3000
        });

        let response: GetTimingsResponse = serde_json::from_value(json).unwrap();

        assert_eq!(response.last_mc_block_seqno, 123);
        assert_eq!(response.last_shard_client_mc_block_seqno, 456);
        assert_eq!(response.last_mc_utime, 789);
        assert_eq!(response.mc_time_diff, 1000);
        assert_eq!(response.shard_client_time_diff, 2000);
        assert_eq!(response.smallest_known_lt, Some(3000));
    }

    #[test]
    fn test_deserialize_camel_case() {
        let json = json!({
            "lastMcBlockSeqno": 123,
            "lastShardClientMcBlockSeqno": 456,
            "lastMcUtime": 789,
            "mcTimeDiff": 1000,
            "shardClientTimeDiff": 2000,
            "smallestKnownLt": 3000
        });

        let response: GetTimingsResponse = serde_json::from_value(json).unwrap();

        assert_eq!(response.last_mc_block_seqno, 123);
        assert_eq!(response.last_shard_client_mc_block_seqno, 456);
        assert_eq!(response.last_mc_utime, 789);
        assert_eq!(response.mc_time_diff, 1000);
        assert_eq!(response.shard_client_time_diff, 2000);
        assert_eq!(response.smallest_known_lt, Some(3000));
    }

    #[test]
    fn test_deserialize_mixed_case() {
        let json = json!({
            "last_mc_block_seqno": 123,
            "lastShardClientMcBlockSeqno": 456,
            "last_mc_utime": 789,
            "mcTimeDiff": 1000,
            "shard_client_time_diff": 2000,
            "smallestKnownLt": 3000
        });

        let response: GetTimingsResponse = serde_json::from_value(json).unwrap();

        assert_eq!(response.last_mc_block_seqno, 123);
        assert_eq!(response.last_shard_client_mc_block_seqno, 456);
        assert_eq!(response.last_mc_utime, 789);
        assert_eq!(response.mc_time_diff, 1000);
        assert_eq!(response.shard_client_time_diff, 2000);
        assert_eq!(response.smallest_known_lt, Some(3000));
    }

    #[test]
    fn test_deserialize_missing_optional_field() {
        let json = json!({
            "last_mc_block_seqno": 123,
            "last_shard_client_mc_block_seqno": 456,
            "last_mc_utime": 789,
            "mc_time_diff": 1000,
            "shard_client_time_diff": 2000
        });

        let response: GetTimingsResponse = serde_json::from_value(json).unwrap();

        assert_eq!(response.last_mc_block_seqno, 123);
        assert_eq!(response.last_shard_client_mc_block_seqno, 456);
        assert_eq!(response.last_mc_utime, 789);
        assert_eq!(response.mc_time_diff, 1000);
        assert_eq!(response.shard_client_time_diff, 2000);
        assert_eq!(response.smallest_known_lt, None);
    }

    #[test]
    fn test_deserialize_error_missing_required_field() {
        let json = json!({
            "last_mc_block_seqno": 123,
            "last_shard_client_mc_block_seqno": 456,
            "last_mc_utime": 789,
            "mc_time_diff": 1000
        });

        let result: Result<GetTimingsResponse, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }
}
