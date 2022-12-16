#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use nekoton::transport::models::ExistingContract;
use nekoton::utils::SimpleClock;
use nekoton_utils::{serde_address, serde_hex_array};
use parking_lot::RwLock;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use ton_block::{
    Account, Deserializable, GetRepresentationHash, Message, MsgAddressInt, Transaction,
};

use everscale_jrpc_models::*;

#[derive(Clone)]
pub struct JrpcClient {
    state: Arc<State>,
    is_capable_of_message_tracking: bool,
}

impl JrpcClient {
    /// [endpoints] full URLs of the RPC endpoints.
    pub async fn new<I: IntoIterator<Item = Url>>(
        endpoints: I,
        options: JrpcClientOptions,
    ) -> Result<Self> {
        let client = reqwest::ClientBuilder::new()
            .gzip(true)
            .timeout(options.request_timeout)
            .build()?;

        Self::with_client(client, endpoints, options).await
    }

    /// [endpoints] full URLs of the RPC endpoints.
    pub async fn with_client<I: IntoIterator<Item = Url>>(
        client: reqwest::Client,
        endpoints: I,
        options: JrpcClientOptions,
    ) -> Result<Self> {
        let mut client = Self {
            state: Arc::new(State {
                endpoints: endpoints
                    .into_iter()
                    .map(|e| JrpcConnection::new(e.to_string(), client.clone()))
                    .collect(),
                live_endpoints: Default::default(),
            }),
            is_capable_of_message_tracking: true,
        };

        let state = client.state.clone();
        let mut live = state.update_endpoints().await;

        tokio::spawn(async move {
            loop {
                let sleep_time = if live != 0 {
                    options.probe_interval
                } else {
                    options.aggressive_poll_interval
                };

                tokio::time::sleep(sleep_time).await;
                live = state.update_endpoints().await;
            }
        });

        for endpoint in &client.state.endpoints {
            if !endpoint.method_is_supported("getDstTransaction").await? {
                client.is_capable_of_message_tracking = false;
                break;
            }
        }

        Ok(client)
    }

    pub async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        let req = GetContractStateRequestRef { address };
        match self.request("getContractState", req).await? {
            ContractStateResponse::NotExists => Ok(None),
            ContractStateResponse::Exists {
                account,
                timings,
                last_transaction_id,
            } => Ok(Some(ExistingContract {
                account,
                timings,
                last_transaction_id,
            })),
        }
    }

    pub async fn run_local(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton::abi::ExecutionOutput>> {
        use nekoton::abi::FunctionExt;

        let state = match self.get_contract_state(address).await? {
            Some(a) => a,
            None => return Ok(None),
        };

        function
            .clone()
            .run_local(&SimpleClock, state.account, input)
            .map(Some)
    }

    pub async fn broadcast_message(&self, message: Message) -> Result<()> {
        anyhow::ensure!(
            message.is_inbound_external(),
            "Only inbound external messages are allowed"
        );

        let req = SendMessageRequest { message };
        self.request("sendMessage", req).await
    }

    /// Works with any jrpc node, but not as reliable as `send_message`.
    /// You should use `send_message` if possible.
    pub async fn send_message_unrelaible(
        &self,
        message: Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        let dst = &message
            .dst()
            .context("Only inbound external messages are allowed")?;

        let initial_state = self
            .get_contract_state(dst)
            .await
            .context("Failed to get dst state")?;

        log::debug!(
            "Initial state. Contract exists: {}",
            initial_state.is_some()
        );

        self.broadcast_message(message).await?;
        log::debug!("Message broadcasted");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;

            let state = self.get_contract_state(dst).await;
            let state = match (options.error_action, state) {
                (TransportErrorAction::Poll, Err(e)) => {
                    log::error!("Error while polling for message: {e:?}. Continue polling");
                    continue;
                }
                (TransportErrorAction::Return, Err(e)) => {
                    return Err(e);
                }
                (_, Ok(res)) => res,
            };

            if state.partial_cmp(&initial_state) == Some(Ordering::Greater) {
                return Ok(SendStatus::LikelyConfirmed);
            }
            if start.elapsed() > options.ttl {
                return Ok(SendStatus::Expired);
            }
            log::debug!("Message not confirmed yet");
        }
    }

    /// NOTE: This method won't work on regular jrpc endpoint without database.
    pub async fn send_message(&self, message: Message, options: SendOptions) -> Result<SendStatus> {
        anyhow::ensure!(self.is_capable_of_message_tracking,
        "One of your endpoints doesn't support message tracking (it's a light node). Use `send_message_unreliable` instead or change endpoints"
        );

        message
            .dst()
            .context("Only inbound external messages are allowed")?;
        let message_hash = *message.hash()?.as_slice();

        self.broadcast_message(message).await?;
        log::info!("Message broadcasted");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;

            let tx = self.get_dst_transaction(&message_hash).await;
            let tx = match (options.error_action, tx) {
                (TransportErrorAction::Poll, Err(e)) => {
                    log::error!("Error while polling for message: {e:?}. Continue polling");
                    continue;
                }
                (TransportErrorAction::Return, Err(e)) => {
                    return Err(e);
                }
                (_, Ok(res)) => res,
            };

            match tx {
                Some(tx) => {
                    return Ok(SendStatus::Confirmed(tx));
                }
                None => {
                    log::debug!("Message not confirmed yet");
                    if start.elapsed() > options.ttl {
                        return Ok(SendStatus::Expired);
                    }
                }
            }
        }
    }

    /// applies `message` to account set as dst and returns the resulting transaction.
    /// Useful for testing purposes.
    pub async fn apply_message(&self, message: &Message) -> Result<ton_block::Transaction> {
        let message_dst = message
            .dst()
            .context("Only inbound external messages are allowed")?;

        let config = self.get_blockchain_config().await?;
        let dst_account = match self.get_contract_state(&message_dst).await? {
            None => Account::AccountNone,
            Some(ac) => Account::Account(ac.account),
        };

        let executor = nekoton_abi::Executor::new(&SimpleClock, config, dst_account)
            .context("Failed to create executor")?;

        executor.run(message)
    }

    pub async fn get_latest_key_block(&self) -> Result<BlockResponse> {
        self.request("getLatestKeyBlock", ()).await
    }

    pub async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig> {
        let key_block = self.get_latest_key_block().await?;
        let block = key_block.block;

        let extra = block.read_extra()?;

        let master = extra.read_custom()?.context("No masterchain block extra")?;

        let params = master.config().context("Invalid config")?.clone();

        let config =
            ton_executor::BlockchainConfig::with_config(params).context("Invalid config")?;

        Ok(config)
    }

    pub async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        #[derive(Debug, Clone, Serialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct GetDstTransactionRequest {
            #[serde(with = "serde_hex_array")]
            pub message_hash: [u8; 32],
        }

        let message_hash = message_hash.try_into().context("Invalid message hash")?;
        let req = GetDstTransactionRequest { message_hash };
        let result: Vec<u8> = match self.request("getDstTransaction", req).await? {
            Some(s) => base64::decode::<String>(s)?,
            None => return Ok(None),
        };
        let result = Transaction::construct_from_bytes(result.as_slice())?;

        Ok(Some(result))
    }

    pub async fn get_transactions(
        &self,
        limit: u16,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<ton_block::Transaction>> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct GetTransactionsRequest<'a> {
            #[serde(with = "serde_address")]
            account: &'a MsgAddressInt,
            limit: u16,
            last_transaction_lt: u64,
        }

        let request = GetTransactionsRequest {
            account,
            limit,
            last_transaction_lt: last_transaction_lt.unwrap_or(0),
        };

        let data: Vec<String> = self.request("getTransactionsList", request).await?;
        let raw_transactions = data
            .into_iter()
            .map(|x| decode_raw_transaction(&x))
            .collect::<Result<_>>()?;

        Ok(raw_transactions)
    }

    async fn request<'a, T, D>(&self, method: &'a str, params: T) -> Result<D>
    where
        T: Serialize,
        for<'de> D: Deserialize<'de>,
    {
        let client = match self.state.get_client() {
            Some(client) => client,
            None => return Err(JrpcClientError::NoEndpointsAvailable.into()),
        };

        client.request(method, params).await
    }
}

fn decode_raw_transaction(boc: &str) -> Result<Transaction> {
    let bytes = base64::decode(boc)?;
    let cell = ton_types::deserialize_tree_of_cells(&mut bytes.as_slice())?;
    let data = Transaction::construct_from(&mut cell.into())?;
    Ok(data)
}

pub struct JrpcClientOptions {
    /// How often the probe should update health statuses.
    ///
    /// Default: `60 sec`
    pub probe_interval: Duration,

    /// How long to wait for a response from a node.
    ///
    /// Default: `1 sec`
    pub request_timeout: Duration,
    /// How long to wait between health checks in case if all nodes are down.
    ///
    /// Default: `1 sec`
    pub aggressive_poll_interval: Duration,
}

impl Default for JrpcClientOptions {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(60),
            request_timeout: Duration::from_secs(3),
            aggressive_poll_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct SendOptions {
    /// action to perform if an error occurs during waiting for message delivery.
    ///
    /// Default: [`TransportErrorAction::Return`]
    pub error_action: TransportErrorAction,

    /// time after which the message is considered as expired.
    ///
    /// Default: `60 sec`
    pub ttl: Duration,

    /// how often the message is checked for delivery.
    ///
    /// Default: `10 sec`
    pub poll_interval: Duration,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            error_action: TransportErrorAction::Return,
            ttl: Duration::from_secs(60),
            poll_interval: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum TransportErrorAction {
    /// Poll endlessly until message is delivered or expired
    Poll,
    /// Fail immediately
    Return,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendStatus {
    LikelyConfirmed,
    Confirmed(Transaction),
    Expired,
}

struct State {
    endpoints: Vec<JrpcConnection>,
    live_endpoints: RwLock<Vec<JrpcConnection>>,
}

impl State {
    fn get_client(&self) -> Option<JrpcConnection> {
        use rand::prelude::SliceRandom;

        let live_endpoints = self.live_endpoints.read();
        live_endpoints.choose(&mut rand::thread_rng()).cloned()
    }

    async fn update_endpoints(&self) -> usize {
        // to preserve order of endpoints within round-robin
        let mut futures = futures::stream::FuturesOrdered::new();
        for endpoint in &self.endpoints {
            futures.push_back(async move { endpoint.is_alive().await.then(|| endpoint.clone()) });
        }

        let mut new_endpoints = Vec::with_capacity(self.endpoints.len());
        while let Some(endpoint) = futures.next().await {
            new_endpoints.extend(endpoint);
        }

        let new_endpoints_ids: HashSet<&str> =
            HashSet::from_iter(new_endpoints.iter().map(|e| e.endpoint.as_str()));
        let mut old_endpoints = self.live_endpoints.write();
        let old_endpoints_ids =
            HashSet::from_iter(old_endpoints.iter().map(|e| e.endpoint.as_str()));

        if old_endpoints_ids != new_endpoints_ids {
            log::warn!(
                "New endpoints: {}",
                new_endpoints
                    .iter()
                    .map(|e| e.endpoint.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        *old_endpoints = new_endpoints;
        old_endpoints.len()
    }
}

#[derive(Clone)]
struct JrpcConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
}

impl JrpcConnection {
    fn new(endpoint: String, client: reqwest::Client) -> Self {
        JrpcConnection {
            endpoint: Arc::new(endpoint),
            client,
        }
    }

    async fn request<'a, T, D>(&self, method: &'a str, params: T) -> Result<D>
    where
        T: Serialize,
        for<'de> D: Deserialize<'de>,
    {
        let req = self
            .client
            .post(self.endpoint.as_str())
            .json(&JrpcRequest { method, params });

        let JsonRpcResponse { result } = req.send().await?.json().await?;
        match result {
            JsonRpcAnswer::Result(result) => Ok(serde_json::from_value(result)?),
            JsonRpcAnswer::Error(e) => {
                Err(JrpcClientError::ErrorResponse(e.code, e.message).into())
            }
        }
    }

    async fn is_alive(&self) -> bool {
        #[derive(Deserialize)]
        struct AnyResponse {}

        self.request::<_, AnyResponse>("getLatestKeyBlock", ())
            .await
            .is_ok()
    }

    async fn method_is_supported(&self, method: &str) -> Result<bool> {
        let req = self
            .client
            .post(self.endpoint.as_str())
            .json(&JrpcRequest { method, params: () });

        let JsonRpcResponse { result } = req.send().await?.json().await?;
        let res = match result {
            JsonRpcAnswer::Result(_) => true,
            JsonRpcAnswer::Error(e) => {
                if e.code == -32601 {
                    false
                } else if e.code == -32602 {
                    true // method is supported, but params are invalid
                } else {
                    anyhow::bail!("Unexpected error: {}. Code: {}", e.message, e.code);
                }
            }
        };

        Ok(res)
    }
}

#[derive(thiserror::Error, Debug)]
enum JrpcClientError {
    #[error("No endpoints available")]
    NoEndpointsAvailable,
    #[error("Error response ({0}): {1}")]
    ErrorResponse(i32, String),
}

#[derive(Debug, Clone)]
struct JrpcRequest<'a, T> {
    method: &'a str,
    params: T,
}

impl<T: Serialize> Serialize for JrpcRequest<'_, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut s = serializer.serialize_struct("JrpcRequest", 4)?;
        s.serialize_field("jsonrpc", "2.0")?;
        s.serialize_field("id", &0)?;
        s.serialize_field("method", self.method)?;
        s.serialize_field("params", &self.params)?;
        s.end()
    }
}

#[derive(Serialize, Debug, Deserialize, PartialEq, Eq)]
/// A JSON-RPC response.
pub struct JsonRpcResponse {
    #[serde(flatten)]
    pub result: JsonRpcAnswer,
}

#[derive(Serialize, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
/// JsonRpc [response object](https://www.jsonrpc.org/specification#response_object)
pub enum JsonRpcAnswer {
    Result(serde_json::Value),
    Error(JsonRpcError),
}

#[derive(Serialize, Debug, Deserialize, PartialEq, Eq)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .init();

        let rpc = [
            // "http://127.0.0.1:8081",
            "http://34.78.198.249:8081/rpc",
            "https://jrpc.everwallet.net/rpc",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect::<Vec<_>>();

        let balanced_client = JrpcClient::new(
            rpc,
            JrpcClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        for _ in 0..10 {
            let response = balanced_client
                .request::<_, serde_json::Value>("getLatestKeyBlock", ())
                .await
                .unwrap();
            log::info!("response is ok: {response:?}");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    #[tokio::test]
    async fn test_get() {
        let pr = get_client().await;

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "0:8e2586602513e99a55fa2be08561469c7ce51a7d5a25977558e77ef2bc9387b4",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "-1:efd5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(pr
            .get_contract_state(
                &MsgAddressInt::from_str(
                    "-1:aaa5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_key_block() {
        let pr = get_client().await;

        pr.get_latest_key_block().await.unwrap();
    }

    #[tokio::test]
    async fn test_transations() {
        let pr = get_client().await;

        pr.get_transactions(
            100,
            &MsgAddressInt::from_str(
                "-1:3333333333333333333333333333333333333333333333333333333333333333",
            )
            .unwrap(),
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn get_dst_transaction() {
        let pr = get_client().await;
        let tx = pr
            .get_dst_transaction(
                &hex::decode("40172727c17aa9ad0dd11c10e822226a7d22cc1e41f8c5d35b7f5614d8a12533")
                    .unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tx.lt, 33247841000007);
    }

    async fn get_client() -> JrpcClient {
        let pr = JrpcClient::new(
            ["https://jrpc.everwallet.net/rpc".parse().unwrap()],
            JrpcClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pr
    }

    #[test]
    fn test_serde() {
        let err = r#"
        {
	        "jsonrpc": "2.0",
	        "error": {
	        	"code": -32601,
	        	"message": "Method `getContractState1` not found",
	        	"data": null
	        },
	        "id": 1
        }"#;

        let resp: JsonRpcResponse = serde_json::from_str(err).unwrap();
        match resp.result {
            JsonRpcAnswer::Error(e) => {
                assert_eq!(e.code, -32601);
            }
            _ => panic!("expected error"),
        }
    }
}
