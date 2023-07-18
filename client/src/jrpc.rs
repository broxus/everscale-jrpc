#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use nekoton::utils::SimpleClock;
use parking_lot::Mutex;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use ton_block::{
    Account, CommonMsgInfo, Deserializable, GetRepresentationHash, Message, MsgAddressInt,
    Transaction,
};
use ton_types::UInt256;

use everscale_rpc_models::{jrpc, Timings};

use crate::*;

#[derive(Clone)]
pub struct JrpcClient {
    state: Arc<State<JrpcConnection>>,
    is_capable_of_message_tracking: bool,
}

impl JrpcClient {
    /// `endpoints` - full URLs of the RPC endpoints.
    pub async fn new<I: IntoIterator<Item = Url>>(
        endpoints: I,
        options: ClientOptions,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(options.request_timeout)
            .tcp_keepalive(Duration::from_secs(60))
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(60))
            .http2_keep_alive_timeout(Duration::from_secs(1))
            .http2_keep_alive_while_idle(true)
            .gzip(false)
            .build()?;

        let client = Self::with_client(client, endpoints, options).await?;

        Ok(client)
    }

    /// `endpoints` - full URLs of the RPC endpoints.
    pub async fn with_client<I: IntoIterator<Item = Url>>(
        client: reqwest::Client,
        endpoints: I,
        options: ClientOptions,
    ) -> Result<Self> {
        let mut client = Self {
            state: Arc::new(State {
                endpoints: endpoints
                    .into_iter()
                    .map(|e| JrpcConnection::new(e.to_string(), client.clone()))
                    .collect(),
                live_endpoints: Default::default(),
                options: options.clone(),
            }),
            is_capable_of_message_tracking: true,
        };

        let state = client.state.clone();
        let mut live = state.update_endpoints().await;

        if live == 0 {
            anyhow::bail!("No live endpoints");
        }

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
            if !endpoint
                .method_is_supported("getDstTransaction")
                .await
                .unwrap_or_default()
            {
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
        let req = jrpc::GetContractStateRequestRef { address };
        match self.request("getContractState", req).await?.into_inner() {
            jrpc::GetContractStateResponse::NotExists => Ok(None),
            jrpc::GetContractStateResponse::Exists {
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

    /// Works like `get_contract_state`, but also checks that node has state older than `time`.
    /// If state is not fresh, returns `Err`.
    /// This method should be used with `ChooseStrategy::TimeBased`.
    pub async fn get_contract_state_with_time_check(
        &self,
        address: &MsgAddressInt,
        time: u32,
    ) -> Result<Option<ExistingContract>, RunError> {
        let req = jrpc::GetContractStateRequestRef { address };
        let response = self.request("getContractState", req).await?;
        match response.result {
            jrpc::GetContractStateResponse::NotExists => {
                response.has_state_for(time)?;
                Ok(None)
            }
            jrpc::GetContractStateResponse::Exists {
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

    /// Works like `run_local`, but also checks that node has fresh state.
    pub async fn run_local_with_time_check(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
        time: u32,
    ) -> Result<Option<nekoton::abi::ExecutionOutput>, RunError> {
        use nekoton::abi::FunctionExt;

        let state = match self
            .get_contract_state_with_time_check(address, time)
            .await?
        {
            Some(a) => a,
            None => return Ok(None),
        };

        let result = function
            .clone()
            .run_local(&SimpleClock, state.account, input)
            .map(Some)?;
        Ok(result)
    }

    pub async fn broadcast_message(&self, message: Message) -> Result<(), RunError> {
        match message.header() {
            CommonMsgInfo::IntMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("IntMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtOutMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("ExtOutMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtInMsgInfo(_) => {}
        }
        let req = jrpc::SendMessageRequest { message };
        self.request("sendMessage", req).await.map(|x| x.result)
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

        tracing::debug!(
            "Initial state. Contract exists: {}",
            initial_state.is_some()
        );

        self.broadcast_message(message).await?;
        tracing::debug!("Message broadcasted");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;

            let state = self.get_contract_state(dst).await;
            let state = match (options.error_action, state) {
                (TransportErrorAction::Poll, Err(e)) => {
                    tracing::error!("Error while polling for message: {e:?}. Continue polling");
                    continue;
                }
                (TransportErrorAction::Return, Err(e)) => {
                    return Err(e);
                }
                (_, Ok(res)) => res,
            };

            if state.partial_cmp(&initial_state) == Some(std::cmp::Ordering::Greater) {
                return Ok(SendStatus::LikelyConfirmed);
            }
            if start.elapsed() > options.ttl {
                return Ok(SendStatus::Expired);
            }
            tracing::debug!("Message not confirmed yet");
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
        tracing::info!("Message broadcasted");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;

            let tx = self.get_dst_transaction(&message_hash).await;
            let tx = match (options.error_action, tx) {
                (TransportErrorAction::Poll, Err(e)) => {
                    tracing::error!("Error while polling for message: {e:?}. Continue polling");
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
                    tracing::debug!("Message not confirmed yet");
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

    pub async fn get_latest_key_block(&self) -> Result<jrpc::GetLatestKeyBlockResponse, RunError> {
        self.request("getLatestKeyBlock", ())
            .await
            .map(|x| x.result)
    }

    pub async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig> {
        let key_block = self.get_latest_key_block().await?;
        let block = key_block.block;

        let extra = block.read_extra()?;

        let master = extra.read_custom()?.context("No masterchain block extra")?;

        let params = master.config().context("Invalid config")?.clone();

        let config = ton_executor::BlockchainConfig::with_config(params, block.global_id)
            .context("Invalid config")?;

        Ok(config)
    }

    pub async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        let message_hash = message_hash.try_into().context("Invalid message hash")?;
        let req = jrpc::GetDstTransactionRequest { message_hash };
        let result: Vec<u8> = match self.request("getDstTransaction", req).await?.into_inner() {
            Some(s) => base64::decode::<String>(s)?,
            None => return Ok(None),
        };
        let result = Transaction::construct_from_bytes(result.as_slice())?;

        Ok(Some(result))
    }

    pub async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        let request = jrpc::GetTransactionsListRequestRef {
            account,
            limit,
            last_transaction_lt,
        };

        let data: Vec<String> = self
            .request("getTransactionsList", request)
            .await?
            .into_inner();
        let raw_transactions = data
            .into_iter()
            .map(|x| decode_raw_transaction(&x))
            .collect::<Result<_>>()?;

        Ok(raw_transactions)
    }

    pub async fn get_raw_transaction(&self, tx_hash: UInt256) -> Result<Option<Transaction>> {
        if !self.is_capable_of_message_tracking {
            anyhow::bail!("This method is not supported by light nodes")
        }
        let request = jrpc::GetTransactionRequestRef {
            id: tx_hash.as_slice(),
        };

        let data: Option<String> = self.request("getTransaction", request).await?.into_inner();
        let raw_transaction = match data {
            Some(data) => decode_raw_transaction(&data)?,
            None => return Ok(None),
        };

        Ok(Some(raw_transaction))
    }

    pub async fn request<'a, T, D>(&self, method: &'a str, params: T) -> Result<Answer<D>, RunError>
    where
        T: Serialize + Send + Sync + Clone,
        for<'de> D: Deserialize<'de>,
    {
        let request: ConnectionRequest<_, everscale_proto::rpc::Request> =
            ConnectionRequest::JRPC(JrpcRequest { method, params });

        const NUM_RETRIES: usize = 10;

        for tries in 0..=NUM_RETRIES {
            let client = self
                .state
                .get_client()
                .await
                .ok_or::<RunError>(ClientError::NoEndpointsAvailable.into())?;

            let res = match client.request(&request).await {
                Ok(res) => res.json::<JsonRpcResponse>().await,
                Err(e) => Err(e),
            };

            let response = match res {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(method, "Error while sending request to endpoint: {e:?}");
                    self.state.remove_endpoint(client.endpoint());

                    if tries == NUM_RETRIES {
                        return Err(e.into());
                    }
                    tokio::time::sleep(self.state.options.aggressive_poll_interval).await;

                    continue;
                }
            };

            match response.result {
                JsonRpcAnswer::Result(result) => {
                    return Ok(Answer {
                        result: serde_json::from_value(result)?,
                        node_stats: client.get_stats(),
                    })
                }
                JsonRpcAnswer::Error(e) => {
                    if tries == NUM_RETRIES {
                        return Err(ClientError::ErrorResponse(e.code, e.message).into());
                    }

                    tokio::time::sleep(self.state.options.aggressive_poll_interval).await;
                }
            }
        }

        unreachable!()
    }
}

fn decode_raw_transaction(boc: &str) -> Result<Transaction> {
    let bytes = base64::decode(boc)?;
    let cell = ton_types::deserialize_tree_of_cells(&mut bytes.as_slice())?;
    Transaction::construct_from_cell(cell)
}

#[derive(Clone, Debug)]
struct JrpcConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<Timings>>>,
}

impl JrpcConnection {
    fn new(endpoint: String, client: reqwest::Client) -> Self {
        JrpcConnection {
            endpoint: Arc::new(endpoint),
            client,
            was_dead: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(Default::default()),
        }
    }
}

impl PartialEq<Self> for JrpcConnection {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint() == other.endpoint()
    }
}

impl Eq for JrpcConnection {}

impl PartialOrd<Self> for JrpcConnection {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JrpcConnection {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.eq(other) {
            cmp::Ordering::Equal
        } else {
            let left_stats = self.get_stats();
            let right_stats = other.get_stats();

            match (left_stats, right_stats) {
                (Some(left_stats), Some(right_stats)) => left_stats.cmp(&right_stats),
                (None, Some(_)) => cmp::Ordering::Less,
                (Some(_), None) => cmp::Ordering::Greater,
                (None, None) => cmp::Ordering::Equal,
            }
        }
    }
}

impl std::fmt::Display for JrpcConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.endpoint())
    }
}

#[async_trait::async_trait]
impl Connection for JrpcConnection {
    fn update_was_dead(&self, is_dead: bool) {
        self.was_dead.store(is_dead, Ordering::Release);
    }

    fn endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    fn get_stats(&self) -> Option<Timings> {
        self.stats.lock().clone()
    }

    fn set_stats(&self, stats: Option<Timings>) {
        *self.stats.lock() = stats;
    }

    fn get_client(&self) -> &reqwest::Client {
        &self.client
    }

    async fn is_alive_inner(&self) -> LiveCheckResult {
        let request: ConnectionRequest<_, everscale_proto::rpc::Request> =
            ConnectionRequest::JRPC(JrpcRequest {
                method: "getTimings",
                params: (),
            });

        let res = match self.request(&request).await {
            Ok(res) => res.json::<JsonRpcResponse>().await,
            Err(e) => Err(e),
        };

        tracing::info!(res = ?res);

        let result = match res {
            Ok(a) => a,
            Err(e) => {
                // general error, link is dead, so no need to check other branch
                if !self.was_dead.load(Ordering::Acquire) {
                    tracing::error!(endpoint = ?self.endpoint, "Dead endpoint: {e:?}");
                    self.was_dead.store(true, Ordering::Release);
                }
                return LiveCheckResult::Dead;
            }
        };

        match result.result {
            JsonRpcAnswer::Result(v) => {
                let timings: Result<jrpc::GetTimingsResponse, _> = serde_json::from_value(v);
                if let Ok(t) = timings {
                    let t = Timings::from(t);
                    let is_reliable = t.is_reliable();
                    if !is_reliable {
                        let Timings {
                            last_mc_block_seqno,
                            last_shard_client_mc_block_seqno,
                            mc_time_diff,
                            shard_client_time_diff,
                            ..
                        } = t;
                        tracing::warn!(last_mc_block_seqno,last_shard_client_mc_block_seqno,mc_time_diff,shard_client_time_diff, endpoint = ?self.endpoint, "Endpoint is not reliable" );
                    }

                    return LiveCheckResult::Live(t);
                }
            }
            JsonRpcAnswer::Error(e) => {
                if e.code != -32601 {
                    tracing::error!(endpoint = ?self.endpoint, "Dead endpoint");
                    return LiveCheckResult::Dead;
                }
            }
        }

        let request: ConnectionRequest<_, everscale_proto::rpc::Request> =
            ConnectionRequest::JRPC(JrpcRequest {
                method: "getLatestKeyBlock",
                params: (),
            });

        let res = match self.request(&request).await {
            Ok(res) => res.json::<JsonRpcResponse>().await,
            Err(e) => Err(e),
        };

        // falback to keyblock request
        match res {
            Ok(res) => {
                if let JsonRpcAnswer::Result(_) = res.result {
                    LiveCheckResult::Dummy
                } else {
                    LiveCheckResult::Dead
                }
            }
            Err(e) => {
                tracing::error!(endpoint = self.endpoint.as_str(), "Dead endpoint: {e:?}");
                LiveCheckResult::Dead
            }
        }
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
