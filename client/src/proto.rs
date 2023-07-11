#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::cmp;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use nekoton::transport::models::ExistingContract;
use nekoton::utils::SimpleClock;
use parking_lot::{Mutex, RwLock};
use reqwest::{StatusCode, Url};
use serde::{Deserialize, Serialize};
use ton_block::{
    Account, CommonMsgInfo, Deserializable, GetRepresentationHash, MaybeDeserialize, MsgAddressInt,
    Serializable, Transaction,
};

use everscale_proto::rpc;

use itertools::Itertools;
use ton_types::UInt256;

use everscale_proto::prost::{bytes, Message};
use everscale_proto::rpc::response::get_contract_state::contract_state::{
    GenTimings, LastTransactionId,
};

static ROUND_ROBIN_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct ProtoClient {
    state: Arc<RpcState>,
    is_capable_of_message_tracking: bool,
}

impl ProtoClient {
    /// `endpoints` - full URLs of the RPC endpoints.
    pub async fn new<I: IntoIterator<Item = Url>>(
        endpoints: I,
        options: ProtoClientOptions,
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
        options: ProtoClientOptions,
    ) -> Result<Self> {
        let mut client = Self {
            state: Arc::new(RpcState {
                endpoints: endpoints
                    .into_iter()
                    .map(|e| ProtoConnection::new(e.to_string(), client.clone()))
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
                .method_is_supported(rpc::request::Call::GetDstTransaction(
                    rpc::request::GetDstTransaction::default(),
                ))
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
        let address = address_to_bytes(address);

        let request = rpc::Request {
            call: Some(rpc::request::Call::GetContractState(
                rpc::request::GetContractState { address },
            )),
        };

        let result = self
            .request(request)
            .await?
            .into_inner()
            .result
            .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::GetContractState(state) => match state.contract_state {
                Some(state) => {
                    let account = deserialize_account_stuff(&state.account)?;

                    let timings = match state
                        .gen_timings
                        .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?
                    {
                        GenTimings::Known(t) => nekoton_abi::GenTimings::Known {
                            gen_lt: t.gen_lt,
                            gen_utime: t.gen_utime,
                        },
                        GenTimings::Unknown(()) => nekoton_abi::GenTimings::Unknown,
                    };

                    let last_transaction_id = match state
                        .last_transaction_id
                        .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?
                    {
                        LastTransactionId::Exact(lt) => {
                            nekoton_abi::LastTransactionId::Exact(nekoton_abi::TransactionId {
                                lt: lt.lt,
                                hash: UInt256::from_slice(lt.hash.as_ref()),
                            })
                        }
                        LastTransactionId::Inexact(lt) => nekoton_abi::LastTransactionId::Inexact {
                            latest_lt: lt.latest_lt,
                        },
                    };

                    Ok(Some(ExistingContract {
                        account,
                        timings,
                        last_transaction_id,
                    }))
                }
                None => Ok(None),
            },
            _ => Err(ProtoClientError::InvalidResponse.into()),
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
        let address = address_to_bytes(address);

        let request = rpc::Request {
            call: Some(rpc::request::Call::GetContractState(
                rpc::request::GetContractState { address },
            )),
        };

        let response = self.request(request).await?;
        let has_state_for = response.has_state_for(time);

        match response.result.result {
            Some(rpc::response::Result::GetContractState(state)) => match state.contract_state {
                Some(state) => {
                    let account = deserialize_account_stuff(&state.account)?;

                    let timings = match state
                        .gen_timings
                        .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?
                    {
                        GenTimings::Known(t) => nekoton_abi::GenTimings::Known {
                            gen_lt: t.gen_lt,
                            gen_utime: t.gen_utime,
                        },
                        GenTimings::Unknown(()) => nekoton_abi::GenTimings::Unknown,
                    };

                    let last_transaction_id = match state
                        .last_transaction_id
                        .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?
                    {
                        LastTransactionId::Exact(lt) => {
                            nekoton_abi::LastTransactionId::Exact(nekoton_abi::TransactionId {
                                lt: lt.lt,
                                hash: UInt256::from_slice(lt.hash.as_ref()),
                            })
                        }
                        LastTransactionId::Inexact(lt) => nekoton_abi::LastTransactionId::Inexact {
                            latest_lt: lt.latest_lt,
                        },
                    };

                    Ok(Some(ExistingContract {
                        account,
                        timings,
                        last_transaction_id,
                    }))
                }
                None => {
                    has_state_for?;
                    Ok(None)
                }
            },
            _ => Err(ProtoClientError::InvalidResponse.into()),
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

    pub async fn broadcast_message(&self, message: ton_block::Message) -> Result<(), RunError> {
        match message.header() {
            CommonMsgInfo::IntMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("IntMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtOutMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("ExtOutMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtInMsgInfo(_) => {}
        }

        let request = rpc::Request {
            call: Some(rpc::request::Call::SendMessage(rpc::request::SendMessage {
                message: bytes::Bytes::from(message.write_to_bytes()?),
            })),
        };

        let result = self
            .request(request)
            .await
            .map(|x| x.result.result)?
            .ok_or::<RunError>(ProtoClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::SendMessage(()) => Ok(()),
            _ => Err(ProtoClientError::InvalidResponse.into()),
        }
    }

    /// Works with any jrpc node, but not as reliable as `send_message`.
    /// You should use `send_message` if possible.
    pub async fn send_message_unrelaible(
        &self,
        message: ton_block::Message,
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

    /// NOTE: This method won't work on regular proto endpoint without database.
    pub async fn send_message(
        &self,
        message: ton_block::Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
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
    pub async fn apply_message(
        &self,
        message: &ton_block::Message,
    ) -> Result<ton_block::Transaction> {
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

    pub async fn get_latest_key_block(&self) -> Result<ton_block::Block, RunError> {
        let request = rpc::Request {
            call: Some(rpc::request::Call::GetLatestKeyBlock(())),
        };

        let response = self.request(request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetLatestKeyBlock(key_block)) => Ok(
                ton_block::Block::construct_from_bytes(key_block.block.as_ref())?,
            ),
            _ => Err(ProtoClientError::InvalidResponse.into()),
        }
    }

    pub async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig> {
        let block = self.get_latest_key_block().await?;

        let extra = block.read_extra()?;

        let master = extra.read_custom()?.context("No masterchain block extra")?;

        let params = master.config().context("Invalid config")?.clone();

        let config = ton_executor::BlockchainConfig::with_config(params, block.global_id)
            .context("Invalid config")?;

        Ok(config)
    }

    pub async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        let message_hash = bytes::Bytes::copy_from_slice(message_hash);
        let request = rpc::Request {
            call: Some(rpc::request::Call::GetDstTransaction(
                rpc::request::GetDstTransaction { message_hash },
            )),
        };

        let response = self.request(request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetRawTransaction(tx)) => match tx.transaction {
                Some(bytes) => Ok(Some(Transaction::construct_from_bytes(bytes.as_ref())?)),
                None => Ok(None),
            },
            _ => Err(ProtoClientError::InvalidResponse.into()),
        }
    }

    pub async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        let account = address_to_bytes(account);

        let request = rpc::Request {
            call: Some(rpc::request::Call::GetTransactionsList(
                rpc::request::GetTransactionsList {
                    account,
                    last_transaction_lt,
                    limit: limit as u32,
                },
            )),
        };

        let response = self.request(request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetTransactionsList(txs)) => txs
                .transactions
                .into_iter()
                .map(|x| decode_raw_transaction(&x))
                .collect::<Result<_>>(),
            _ => Err(ProtoClientError::InvalidResponse.into()),
        }
    }

    pub async fn get_raw_transaction(&self, tx_hash: UInt256) -> Result<Option<Transaction>> {
        if !self.is_capable_of_message_tracking {
            anyhow::bail!("This method is not supported by light nodes")
        }

        let request = rpc::Request {
            call: Some(rpc::request::Call::GetTransaction(
                rpc::request::GetTransaction {
                    id: bytes::Bytes::copy_from_slice(tx_hash.as_slice()),
                },
            )),
        };

        let response = self.request(request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetRawTransaction(tx)) => match tx.transaction {
                Some(bytes) => Ok(Some(Transaction::construct_from_bytes(bytes.as_ref())?)),
                None => Ok(None),
            },
            _ => Err(ProtoClientError::InvalidResponse.into()),
        }
    }

    pub async fn request(&self, request: rpc::Request) -> Result<Answer, RunError> {
        const NUM_RETRIES: usize = 10;

        for tries in 0..=NUM_RETRIES {
            let client = self
                .state
                .get_client()
                .await
                .ok_or::<RunError>(ProtoClientError::NoEndpointsAvailable.into())?;

            let response = match client.request(request.clone()).await {
                Ok(a) => a,
                Err(e) => {
                    // TODO: tracing::error!(request, "Error while sending request to endpoint: {e:?}");
                    self.state.remove_endpoint(&client.endpoint);

                    if tries == NUM_RETRIES {
                        return Err(e.into());
                    }
                    tokio::time::sleep(self.state.options.aggressive_poll_interval).await;

                    continue;
                }
            };

            match response {
                ProtoAnswer::Result(result) => {
                    return Ok(Answer {
                        result,
                        node_stats: client.get_stats(),
                    })
                }
                ProtoAnswer::Error(e) => {
                    if tries == NUM_RETRIES {
                        return Err(ProtoClientError::ErrorResponse(e.code, e.message).into());
                    }

                    tokio::time::sleep(self.state.options.aggressive_poll_interval).await;
                }
            }
        }

        unreachable!()
    }
}

pub struct Answer {
    result: rpc::Response,
    node_stats: Option<rpc::response::GetTimings>,
}

impl Answer {
    pub fn into_inner(self) -> rpc::Response {
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

fn decode_raw_transaction(bytes: &bytes::Bytes) -> Result<Transaction> {
    let cell = ton_types::deserialize_tree_of_cells(&mut bytes.as_ref())?;
    Transaction::construct_from_cell(cell)
}

fn address_to_bytes(address: &MsgAddressInt) -> bytes::Bytes {
    let mut bytes = Vec::with_capacity(33);
    bytes.push(address.workchain_id() as u8);
    bytes.extend(address.address().get_bytestring_on_stack(0).to_vec());

    bytes::Bytes::from(bytes)
}

fn deserialize_account_stuff(bytes: &bytes::Bytes) -> Result<ton_block::AccountStuff> {
    ton_types::deserialize_tree_of_cells(&mut bytes.as_ref()).and_then(|cell| {
        let slice = &mut ton_types::SliceData::load_cell(cell)?;
        Ok(ton_block::AccountStuff {
            addr: Deserializable::construct_from(slice)?,
            storage_stat: Deserializable::construct_from(slice)?,
            storage: ton_block::AccountStorage {
                last_trans_lt: Deserializable::construct_from(slice)?,
                balance: Deserializable::construct_from(slice)?,
                state: Deserializable::construct_from(slice)?,
                init_code_hash: if slice.remaining_bits() > 0 {
                    UInt256::read_maybe_from(slice)?
                } else {
                    None
                },
            },
        })
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtoClientOptions {
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

impl Default for ProtoClientOptions {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(1),
            request_timeout: Duration::from_secs(3),
            aggressive_poll_interval: Duration::from_secs(1),
            choose_strategy: ChooseStrategy::Random,
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

struct RpcState {
    endpoints: Vec<ProtoConnection>,
    live_endpoints: RwLock<Vec<ProtoConnection>>,
    options: ProtoClientOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub enum ChooseStrategy {
    Random,
    RoundRobin,
    /// Choose the endpoint with the lowest latency
    TimeBased,
}

impl ChooseStrategy {
    fn choose(&self, endpoints: &[ProtoConnection]) -> Option<ProtoConnection> {
        use rand::prelude::SliceRandom;

        match self {
            ChooseStrategy::Random => endpoints.choose(&mut rand::thread_rng()).cloned(),
            ChooseStrategy::RoundRobin => {
                let index = ROUND_ROBIN_COUNTER.fetch_add(1, Ordering::Release);

                endpoints.get(index % endpoints.len()).cloned()
            }
            ChooseStrategy::TimeBased => endpoints
                .iter()
                .min_by(|left, right| left.cmp(right))
                .cloned(),
        }
    }
}

impl RpcState {
    async fn get_client(&self) -> Option<ProtoConnection> {
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
            HashSet::from_iter(new_endpoints.iter().map(|e| e.endpoint.as_str()));
        let mut old_endpoints = self.live_endpoints.write();
        let old_endpoints_ids =
            HashSet::from_iter(old_endpoints.iter().map(|e| e.endpoint.as_str()));

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
            .retain(|c| c.endpoint.as_ref() != endpoint);

        tracing::warn!(endpoint, "Removed endpoint");
    }
}

#[derive(Clone, Debug)]
struct ProtoConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<rpc::response::GetTimings>>>,
}

impl PartialEq<Self> for ProtoConnection {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint == other.endpoint
    }
}

impl Eq for ProtoConnection {}

impl PartialOrd<Self> for ProtoConnection {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl cmp::Ord for ProtoConnection {
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

impl std::fmt::Display for ProtoConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.endpoint)
    }
}

impl ProtoConnection {
    fn new(endpoint: String, client: reqwest::Client) -> Self {
        ProtoConnection {
            endpoint: Arc::new(endpoint),
            client,
            was_dead: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(Default::default()),
        }
    }

    async fn request(&self, request: rpc::Request) -> Result<ProtoAnswer> {
        let body = request.encode_to_vec();
        let req = self.client.post(self.endpoint.as_str()).body(body);

        let response = req.send().await?;

        let result = match response.status() {
            StatusCode::OK => ProtoAnswer::Result(rpc::Response::decode(response.bytes().await?)?),
            _ => ProtoAnswer::Error(rpc::Error::decode(response.bytes().await?)?),
        };

        Ok(result)
    }

    fn update_was_dead(&self, is_dead: bool) {
        self.was_dead.store(is_dead, Ordering::Release);
    }

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

    async fn is_alive_inner(&self) -> LiveCheckResult {
        let request = rpc::Request {
            call: Some(rpc::request::Call::GetTimings(())),
        };

        let response = match self.request(request).await {
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

        match response {
            ProtoAnswer::Result(result) => {
                if let Some(rpc::response::Result::GetTimings(timings)) = result.result {
                    let is_reliable = timings.is_reliable();
                    if !is_reliable {
                        let rpc::response::GetTimings {
                            last_mc_block_seqno,
                            last_shard_client_mc_block_seqno,
                            mc_time_diff,
                            shard_client_time_diff,
                            ..
                        } = timings;
                        tracing::warn!(last_mc_block_seqno,last_shard_client_mc_block_seqno,mc_time_diff,shard_client_time_diff, endpoint = ?self.endpoint, "Endpoint is not reliable" );
                    }

                    return LiveCheckResult::Live(timings);
                }
            }
            ProtoAnswer::Error(e) => {
                if e.code != -32601 {
                    tracing::error!(endpoint = ?self.endpoint, "Dead endpoint");
                    return LiveCheckResult::Dead;
                }
            }
        }

        // falback to keyblock request
        let request = rpc::Request {
            call: Some(rpc::request::Call::GetLatestKeyBlock(())),
        };

        match self.request(request).await {
            Ok(res) => {
                if let ProtoAnswer::Result(_) = res {
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

    async fn method_is_supported(&self, method: rpc::request::Call) -> Result<bool> {
        let body = rpc::Request { call: Some(method) }.encode_to_vec();
        let req = self.client.post(self.endpoint.as_str()).body(body);

        let response = req.send().await?;
        let result = match response.status() {
            StatusCode::OK => ProtoAnswer::Result(rpc::Response::decode(response.bytes().await?)?),
            _ => ProtoAnswer::Error(rpc::Error::decode(response.bytes().await?)?),
        };

        let res = match result {
            ProtoAnswer::Result(_) => true,
            ProtoAnswer::Error(e) => {
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

    fn get_stats(&self) -> Option<rpc::response::GetTimings> {
        self.stats.lock().clone()
    }

    fn set_stats(&self, stats: Option<rpc::response::GetTimings>) {
        *self.stats.lock() = stats;
    }
}

enum LiveCheckResult {
    /// GetTimings request was successful
    Live(rpc::response::GetTimings),
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

#[derive(thiserror::Error, Debug)]
pub enum ProtoClientError {
    #[error("No endpoints available")]
    NoEndpointsAvailable,
    #[error("Error response ({0}): {1}")]
    ErrorResponse(i32, String),
    #[error("Failed to parse response")]
    InvalidResponse,
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

pub enum ProtoAnswer {
    Result(rpc::Response),
    Error(rpc::Error),
}

#[derive(Serialize, Debug, Deserialize, PartialEq, Eq)]
pub struct ProtoRpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    #[error("No state for timestamp {0}")]
    NoStateForTimeStamp(u32),
    #[error("Network error: {0}")]
    NetworkError(#[from] reqwest::Error),
    #[error("Proto error: {0}")]
    ProtoClientError(#[from] ProtoClientError),
    #[error("JSON error: {0}")]
    ParseError(#[from] serde_json::Error),
    #[error("Invalid message type: {0}")]
    NotInboundMessage(String),
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test() {
        tracing_subscriber::fmt::init();

        let rpc = [
            "http://127.0.0.1:8081",
            "http://34.78.198.249:8081/rpc",
            "https://jrpc.everwallet.net/rpc",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect::<Vec<_>>();

        let balanced_client = ProtoClient::new(
            rpc,
            ProtoClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        for _ in 0..10 {
            let response = balanced_client.get_latest_key_block().await.unwrap();
            tracing::info!("response is ok: {response:?}");
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
    async fn test_all_dead() {
        let pr = ProtoClient::new(
            [
                "https://lolkek228.dead/rpc".parse().unwrap(),
                "http://127.0.0.1:8081".parse().unwrap(),
                "http://127.0.0.1:12333".parse().unwrap(),
            ],
            ProtoClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .is_err();

        assert!(pr);
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

    async fn get_client() -> ProtoClient {
        let pr = ProtoClient::new(
            [
                "https://jrpc.everwallet.net/rpc".parse().unwrap(),
                "http://127.0.0.1:8081".parse().unwrap(),
            ],
            ProtoClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        pr
    }
}
