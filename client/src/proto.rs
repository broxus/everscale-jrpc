#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use nekoton::utils::SimpleClock;
use parking_lot::Mutex;
use reqwest::{StatusCode, Url};
use ton_block::{
    Account, CommonMsgInfo, Deserializable, GetRepresentationHash, MaybeDeserialize, MsgAddressInt,
    Serializable, Transaction,
};
use ton_types::UInt256;

use everscale_rpc_models::Timings;

use everscale_proto::prost::{bytes, Message};
use everscale_proto::{
    rpc,
    rpc::response::get_contract_state::contract_state::{GenTimings, LastTransactionId},
};

use crate::*;

#[derive(Clone)]
pub struct ProtoClient {
    state: Arc<State>,
    is_capable_of_message_tracking: bool,
}

impl ProtoClient {
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
                    .map(|e| Arc::new(ProtoConnection::new(e.to_string(), client.clone())) as _)
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
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::GetContractState(state) => match state.contract_state {
                Some(state) => {
                    let account = deserialize_account_stuff(&state.account)?;

                    let timings = match state
                        .gen_timings
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                    {
                        GenTimings::Known(t) => nekoton_abi::GenTimings::Known {
                            gen_lt: t.gen_lt,
                            gen_utime: t.gen_utime,
                        },
                        GenTimings::Unknown(()) => nekoton_abi::GenTimings::Unknown,
                    };

                    let last_transaction_id = match state
                        .last_transaction_id
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
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
            _ => Err(ClientError::InvalidResponse.into()),
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
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                    {
                        GenTimings::Known(t) => nekoton_abi::GenTimings::Known {
                            gen_lt: t.gen_lt,
                            gen_utime: t.gen_utime,
                        },
                        GenTimings::Unknown(()) => nekoton_abi::GenTimings::Unknown,
                    };

                    let last_transaction_id = match state
                        .last_transaction_id
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
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
            _ => Err(ClientError::InvalidResponse.into()),
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
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::SendMessage(()) => Ok(()),
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    /// Works with any rpc node, but not as reliable as `send_message`.
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
            _ => Err(ClientError::InvalidResponse.into()),
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
            _ => Err(ClientError::InvalidResponse.into()),
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
            _ => Err(ClientError::InvalidResponse.into()),
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
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    pub async fn request(&self, request: rpc::Request) -> Result<Answer<rpc::Response>, RunError> {
        let request = request.encode_to_vec();
        const NUM_RETRIES: usize = 10;

        for tries in 0..=NUM_RETRIES {
            let client = self
                .state
                .get_client()
                .await
                .ok_or::<RunError>(ClientError::NoEndpointsAvailable.into())?;

            let response = match client.request(request.clone()).await {
                Ok(res) => decode_answer(res).await,
                Err(e) => Err(e.into()),
            };

            let response = match response {
                Ok(a) => a,
                Err(e) => {
                    // TODO: tracing::error!(request, "Error while sending request to endpoint: {e:?}");
                    self.state.remove_endpoint(&client.endpoint());

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
                    });
                }
                ProtoAnswer::Error(e) => {
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

#[derive(Clone, Debug)]
struct ProtoConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<Timings>>>,
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
}

#[async_trait::async_trait]
impl Connection for ProtoConnection {
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

    async fn is_alive_inner(&self) -> LiveCheckResult {
        let request = rpc::Request {
            call: Some(rpc::request::Call::GetTimings(())),
        }
        .encode_to_vec();

        let response = match self.request(request).await {
            Ok(res) => decode_answer(res).await,
            Err(e) => Err(e.into()),
        };

        let response = match response {
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
                if let Some(rpc::response::Result::GetTimings(t)) = result.result {
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
        }
        .encode_to_vec();

        let response = match self.request(request).await {
            Ok(res) => decode_answer(res).await,
            Err(e) => Err(e.into()),
        };

        match response {
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

    async fn method_is_supported(&self, method: &str) -> Result<bool> {
        let body = match method {
            "getDstTransaction" => rpc::Request {
                call: Some(rpc::request::Call::GetDstTransaction(
                    rpc::request::GetDstTransaction::default(),
                )),
            },
            _ => return Ok(false),
        };

        let req = self
            .client
            .post(self.endpoint.as_str())
            .body(body.encode_to_vec());

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

    async fn request(&self, request: Vec<u8>) -> Result<reqwest::Response, reqwest::Error> {
        let req = self.client.post(self.endpoint.as_str()).body(request);
        let res = req.send().await?;
        Ok(res)
    }
}

async fn decode_answer(response: reqwest::Response) -> Result<ProtoAnswer> {
    let res = match response.status() {
        StatusCode::OK => ProtoAnswer::Result(rpc::Response::decode(response.bytes().await?)?),
        _ => ProtoAnswer::Error(rpc::Error::decode(response.bytes().await?)?),
    };

    Ok(res)
}

pub enum ProtoAnswer {
    Result(rpc::Response),
    Error(rpc::Error),
}
