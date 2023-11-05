#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::cmp;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use itertools::Itertools;
use nekoton::transport::models::ExistingContract;
use nekoton_proto::prost::Message;
use nekoton_proto::protos::rpc;
use nekoton_utils::SimpleClock;
use parking_lot::RwLock;
use reqwest::header::CONTENT_TYPE;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use ton_block::{GetRepresentationHash, MsgAddressInt, Transaction};

use everscale_rpc_models::Timings;

use crate::jrpc::{JrpcClient, JrpcRequest};
use crate::proto::ProtoClient;

pub mod jrpc;
pub mod proto;

static ROUND_ROBIN_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub enum RpcClient {
    Jrpc(JrpcClient),
    Proto(ProtoClient),
}

impl RpcClient {
    pub async fn new<I: IntoIterator<Item = Url> + Send + Clone>(
        endpoints: I,
        options: ClientOptions,
    ) -> Result<Self> {
        let is_proto = endpoints
            .clone()
            .into_iter()
            .all(|x| x.as_str().ends_with("/proto"));
        if is_proto {
            return Ok(RpcClient::Proto(
                ProtoClient::new(endpoints, options).await?,
            ));
        }

        let is_jrpc = endpoints
            .clone()
            .into_iter()
            .all(|x| x.as_str().ends_with("/rpc"));
        if is_jrpc {
            return Ok(RpcClient::Jrpc(JrpcClient::new(endpoints, options).await?));
        }

        let result = ProtoClient::new(endpoints.clone(), options.clone()).await;
        let client = match result {
            Ok(client) => RpcClient::Proto(client),
            Err(_) => RpcClient::Jrpc(JrpcClient::new(endpoints, options).await?),
        };

        Ok(client)
    }

    pub fn is_capable_of_message_tracking(&self) -> bool {
        match self {
            RpcClient::Jrpc(client) => client.is_capable_of_message_tracking(),
            RpcClient::Proto(client) => client.is_capable_of_message_tracking(),
        }
    }

    pub async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig> {
        match self {
            RpcClient::Jrpc(client) => client.get_blockchain_config().await,
            RpcClient::Proto(client) => client.get_blockchain_config().await,
        }
    }

    pub async fn broadcast_message(&self, message: ton_block::Message) -> Result<(), RunError> {
        match self {
            RpcClient::Jrpc(client) => client.broadcast_message(message).await,
            RpcClient::Proto(client) => client.broadcast_message(message).await,
        }
    }

    pub async fn get_raw_transaction(
        &self,
        tx_hash: ton_types::UInt256,
    ) -> Result<Option<Transaction>> {
        match self {
            RpcClient::Jrpc(client) => client.get_raw_transaction(tx_hash).await,
            RpcClient::Proto(client) => client.get_raw_transaction(tx_hash).await,
        }
    }

    pub async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        match self {
            RpcClient::Jrpc(client) => client.get_dst_transaction(message_hash).await,
            RpcClient::Proto(client) => client.get_dst_transaction(message_hash).await,
        }
    }

    pub async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        match self {
            RpcClient::Jrpc(client) => {
                client
                    .get_transactions(limit, account, last_transaction_lt)
                    .await
            }
            RpcClient::Proto(client) => {
                client
                    .get_transactions(limit, account, last_transaction_lt)
                    .await
            }
        }
    }

    pub async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Option<ExistingContract>> {
        match self {
            RpcClient::Jrpc(client) => {
                client
                    .get_contract_state(address, last_transaction_lt)
                    .await
            }
            RpcClient::Proto(client) => {
                client
                    .get_contract_state(address, last_transaction_lt)
                    .await
            }
        }
    }

    pub async fn get_contract_state_with_time_check(
        &self,
        address: &MsgAddressInt,
        time: u32,
    ) -> Result<Option<ExistingContract>, RunError> {
        match self {
            RpcClient::Jrpc(client) => {
                client
                    .get_contract_state_with_time_check(address, time)
                    .await
            }
            RpcClient::Proto(client) => {
                client
                    .get_contract_state_with_time_check(address, time)
                    .await
            }
        }
    }

    pub async fn run_local(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton::abi::ExecutionOutput>> {
        match self {
            RpcClient::Jrpc(client) => client.run_local(address, function, input).await,
            RpcClient::Proto(client) => client.run_local(address, function, input).await,
        }
    }

    pub async fn apply_message(
        &self,
        message: &ton_block::Message,
    ) -> Result<ton_block::Transaction> {
        match self {
            RpcClient::Jrpc(client) => client.apply_message(message).await,
            RpcClient::Proto(client) => client.apply_message(message).await,
        }
    }

    pub async fn send_message_unrelaible(
        &self,
        message: ton_block::Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        match self {
            RpcClient::Jrpc(client) => client.send_message_unrelaible(message, options).await,
            RpcClient::Proto(client) => client.send_message_unrelaible(message, options).await,
        }
    }

    pub async fn send_message(
        &self,
        message: ton_block::Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        match self {
            RpcClient::Jrpc(client) => client.send_message(message, options).await,
            RpcClient::Proto(client) => client.send_message(message, options).await,
        }
    }

    pub async fn run_local_with_time_check(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
        time: u32,
    ) -> Result<Option<nekoton::abi::ExecutionOutput>, RunError> {
        match self {
            RpcClient::Jrpc(client) => {
                client
                    .run_local_with_time_check(address, function, input, time)
                    .await
            }
            RpcClient::Proto(client) => {
                client
                    .run_local_with_time_check(address, function, input, time)
                    .await
            }
        }
    }

    pub async fn request<'a, S, D>(
        &self,
        request: &RpcRequest<'a, S>,
    ) -> Result<RpcResponse<D>, RunError>
    where
        S: Serialize + Send + Sync + Clone,
        for<'de> D: Deserialize<'de>,
    {
        let response = match self {
            RpcClient::Jrpc(client) => {
                let res: Answer<_> = client.request(request).await?;
                RpcResponse::JRPC(res)
            }
            RpcClient::Proto(client) => {
                let res = client.request(request).await?;
                RpcResponse::PROTO(res)
            }
        };

        Ok(response)
    }
}

#[async_trait::async_trait]
pub trait Client<T>: Send + Sync + Sized
where
    T: Connection + Ord + Clone + 'static,
{
    async fn new<I: IntoIterator<Item = Url> + Send>(
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
    async fn with_client<I: IntoIterator<Item = Url> + Send>(
        client: reqwest::Client,
        endpoints: I,
        options: ClientOptions,
    ) -> Result<Self> {
        let state = Arc::new(State {
            endpoints: endpoints
                .into_iter()
                .map(|e| T::new(e.to_string(), client.clone()))
                .collect(),
            live_endpoints: Default::default(),
            options: options.clone(),
        });

        let mut is_capable_of_message_tracking = true;

        let mut live = state.update_endpoints().await;

        if live == 0 {
            anyhow::bail!("No live endpoints");
        }

        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let sleep_time = if live != 0 {
                    options.probe_interval
                } else {
                    options.aggressive_poll_interval
                };

                tokio::time::sleep(sleep_time).await;
                live = st.update_endpoints().await;
            }
        });

        for endpoint in &state.endpoints {
            if !endpoint
                .method_is_supported("getDstTransaction")
                .await
                .unwrap_or_default()
            {
                is_capable_of_message_tracking = false;
                break;
            }
        }

        Ok(Self::construct_from_state(
            state,
            is_capable_of_message_tracking,
        ))
    }

    fn construct_from_state(state: Arc<State<T>>, is_capable_of_message_tracking: bool) -> Self;

    fn is_capable_of_message_tracking(&self) -> bool;

    async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig>;

    async fn broadcast_message(&self, message: ton_block::Message) -> Result<(), RunError>;

    async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>>;

    async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Option<ExistingContract>>;

    async fn get_contract_state_with_time_check(
        &self,
        address: &MsgAddressInt,
        time: u32,
    ) -> Result<Option<ExistingContract>, RunError>;

    async fn run_local(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton::abi::ExecutionOutput>> {
        use nekoton::abi::FunctionExt;

        let state = match self.get_contract_state(address, None).await? {
            Some(a) => a,
            None => return Ok(None),
        };

        function
            .clone()
            .run_local(&SimpleClock, state.account, input)
            .map(Some)
    }

    /// applies `message` to account set as dst and returns the resulting transaction.
    /// Useful for testing purposes.
    async fn apply_message(&self, message: &ton_block::Message) -> Result<ton_block::Transaction> {
        let message_dst = message
            .dst()
            .context("Only inbound external messages are allowed")?;

        let config = self.get_blockchain_config().await?;
        let dst_account = match self.get_contract_state(&message_dst, None).await? {
            None => ton_block::Account::AccountNone,
            Some(ac) => ton_block::Account::Account(ac.account),
        };

        let executor = nekoton_abi::Executor::new(&SimpleClock, config, dst_account)
            .context("Failed to create executor")?;

        executor.run(message)
    }

    /// Works with any jrpc node, but not as reliable as `send_message`.
    /// You should use `send_message` if possible.
    async fn send_message_unrelaible(
        &self,
        message: ton_block::Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        let dst = &message
            .dst()
            .context("Only inbound external messages are allowed")?;

        let initial_state = self
            .get_contract_state(dst, None)
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

            let state = self.get_contract_state(dst, None).await;
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
    async fn send_message(
        &self,
        message: ton_block::Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        anyhow::ensure!(self.is_capable_of_message_tracking(),
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

    /// Works like `run_local`, but also checks that node has fresh state.
    async fn run_local_with_time_check(
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
}

#[async_trait::async_trait]
pub trait Connection: Send + Sync {
    fn new(endpoint: String, client: reqwest::Client) -> Self;

    fn update_was_dead(&self, is_dead: bool);

    async fn is_alive(&self) -> bool {
        let check_result = self.is_alive_inner().await;
        let is_alive = check_result.as_bool();
        self.update_was_dead(!is_alive);

        match check_result {
            LiveCheckResult::Live(stats) => self.set_stats(Some(stats)),
            LiveCheckResult::Dummy => {}
            LiveCheckResult::Dead => {}
        }

        is_alive
    }

    fn endpoint(&self) -> &str;

    fn get_stats(&self) -> Option<Timings>;

    fn set_stats(&self, stats: Option<Timings>);

    fn get_client(&self) -> &reqwest::Client;

    async fn is_alive_inner(&self) -> LiveCheckResult;

    async fn method_is_supported(&self, method: &str) -> Result<bool>;

    async fn request<T: Serialize + Send + Sync>(
        &self,
        request: &RpcRequest<T>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let req = match request {
            RpcRequest::JRPC(request) => self
                .get_client()
                .post(self.endpoint())
                .header(CONTENT_TYPE, "application/json")
                .json(request),
            RpcRequest::PROTO(request) => self
                .get_client()
                .post(self.endpoint())
                .header(CONTENT_TYPE, "application/x-protobuf")
                .body(request.encode_to_vec()),
        };

        let res = req.send().await?;
        Ok(res)
    }
}

pub enum RpcRequest<'a, T> {
    JRPC(JrpcRequest<'a, T>),
    PROTO(rpc::Request),
}

impl<'a, T: Serialize + Send + Sync> RpcRequest<'a, T> {
    pub fn create_jrpc_request(method: &'a str, params: &'a T) -> Self {
        Self::JRPC(JrpcRequest::new(method, params))
    }

    pub fn create_proto_request(req: rpc::Request) -> Self {
        Self::PROTO(req)
    }
}

pub enum RpcResponse<D>
where
    for<'de> D: Deserialize<'de>,
{
    JRPC(Answer<D>),
    PROTO(Answer<rpc::Response>),
}

pub struct State<T> {
    endpoints: Vec<T>,
    live_endpoints: RwLock<Vec<T>>,
    options: ClientOptions,
}

impl<T: Connection + Ord + Clone> State<T> {
    async fn get_client(&self) -> Option<T> {
        for _ in 0..10 {
            let client = {
                let live_endpoints = self.live_endpoints.read();
                self.options.choose_strategy.choose(&live_endpoints)
            };

            if client.is_some() {
                return client;
            } else {
                tokio::time::sleep(self.options.aggressive_poll_interval).await;
            }
        }

        None
    }

    async fn update_endpoints(&self) -> usize {
        let mut futures = futures::stream::FuturesUnordered::new();
        for endpoint in &self.endpoints {
            futures.push(async move { endpoint.is_alive().await.then(|| endpoint.clone()) });
        }

        let mut new_endpoints = Vec::with_capacity(self.endpoints.len());
        while let Some(endpoint) = futures.next().await {
            new_endpoints.extend(endpoint);
        }

        let new_endpoints_ids: HashSet<&str> =
            HashSet::from_iter(new_endpoints.iter().map(|e| e.endpoint()));
        let mut old_endpoints = self.live_endpoints.write();
        let old_endpoints_ids = HashSet::from_iter(old_endpoints.iter().map(|e| e.endpoint()));

        if old_endpoints_ids != new_endpoints_ids {
            let sorted_endpoints: Vec<_> = new_endpoints_ids.iter().sorted().collect();
            tracing::warn!(endpoints = ?sorted_endpoints, "Endpoints updated");
        }

        *old_endpoints = new_endpoints;
        old_endpoints.len()
    }

    fn remove_endpoint(&self, endpoint: &str) {
        self.live_endpoints
            .write()
            .retain(|c| c.endpoint() != endpoint);

        tracing::warn!(endpoint, "Removed endpoint");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientOptions {
    /// How often the probe should update health statuses.
    ///
    /// Default: `1 sec`
    pub probe_interval: Duration,

    /// How long to wait for a response from a node.
    ///
    /// Default: `1 sec`
    pub request_timeout: Duration,
    /// How long to wait between health checks in case if all nodes are down.
    ///
    /// Default: `1 sec`
    pub aggressive_poll_interval: Duration,

    pub choose_strategy: ChooseStrategy,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(3),
            aggressive_poll_interval: Duration::from_secs(1),
            choose_strategy: ChooseStrategy::Random,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum ChooseStrategy {
    Random,
    RoundRobin,
    /// Choose the endpoint with the lowest latency
    TimeBased,
}

impl ChooseStrategy {
    fn choose<T: Connection + Ord + Clone>(&self, endpoints: &[T]) -> Option<T> {
        use rand::prelude::SliceRandom;

        match self {
            ChooseStrategy::Random => endpoints.choose(&mut rand::thread_rng()).cloned(),
            ChooseStrategy::RoundRobin => {
                let index = ROUND_ROBIN_COUNTER.fetch_add(1, Ordering::Release);

                endpoints.get(index % endpoints.len()).cloned()
            }
            ChooseStrategy::TimeBased => endpoints
                .iter()
                .min_by(|&left, &right| left.cmp(right))
                .cloned(),
        }
    }
}

pub enum LiveCheckResult {
    /// GetTimings request was successful
    Live(Timings),
    /// Keyblock request was successful, but getTimings failed
    Dummy,
    Dead,
}

impl LiveCheckResult {
    fn as_bool(&self) -> bool {
        match self {
            LiveCheckResult::Live(metrics) => metrics.is_reliable(),
            LiveCheckResult::Dummy => true,
            LiveCheckResult::Dead => false,
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

pub struct Answer<D> {
    result: D,
    node_stats: Option<Timings>,
}

impl<D> Answer<D> {
    pub fn into_inner(self) -> D {
        self.result
    }

    pub fn has_state_for(&self, time: u32) -> Result<(), RunError> {
        if let Some(stats) = &self.node_stats {
            return if stats.has_state_for(time) {
                Ok(())
            } else {
                Err(RunError::NoStateForTimeStamp(time))
            };
        }
        // If we don't have stats, we assume that the node is healthy
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("No endpoints available")]
    NoEndpointsAvailable,
    #[error("Error response ({0}): {1}")]
    ErrorResponse(i32, String),
    #[error("Failed to parse response")]
    InvalidResponse,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    #[error("No state for timestamp {0}")]
    NoStateForTimeStamp(u32),
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    #[error("Jrpc error: {0}")]
    JrpcClientError(#[from] ClientError),
    #[cfg(not(feature = "simd"))]
    #[error("JSON error: {0}")]
    ParseError(#[from] serde_json::Error),
    #[cfg(feature = "simd")]
    #[error("JSON error: {0}")]
    ParseError(#[from] simd_json::Error),
    #[error("Invalid message type: {0}")]
    NotInboundMessage(String),
}
