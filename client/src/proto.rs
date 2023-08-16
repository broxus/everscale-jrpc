#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use parking_lot::Mutex;
use reqwest::StatusCode;
use ton_block::{CommonMsgInfo, Deserializable, MsgAddressInt, Serializable, Transaction};
use ton_types::UInt256;

use everscale_rpc_models::Timings;
use everscale_rpc_proto::models::ProtoAnswer;
use everscale_rpc_proto::prost::{bytes, Message};
use everscale_rpc_proto::rpc;
use everscale_rpc_proto::utils;

use crate::*;

pub type ProtoClient = ProtoClientImpl<ProtoConnection>;

#[derive(Clone)]
pub struct ProtoClientImpl<T: Connection + Ord + Clone> {
    state: Arc<State<T>>,
    is_capable_of_message_tracking: bool,
}

#[async_trait::async_trait]
impl<T> Client<T> for ProtoClientImpl<T>
where
    T: Connection + Ord + Clone + 'static,
{
    fn construct_from_state(state: Arc<State<T>>, is_capable_of_message_tracking: bool) -> Self {
        Self {
            state,
            is_capable_of_message_tracking,
        }
    }

    fn is_capable_of_message_tracking(&self) -> bool {
        self.is_capable_of_message_tracking
    }

    async fn get_blockchain_config(&self) -> Result<ton_executor::BlockchainConfig> {
        let block = self.get_latest_key_block().await?;

        let extra = block.read_extra()?;

        let master = extra.read_custom()?.context("No masterchain block extra")?;

        let params = master.config().context("Invalid config")?.clone();

        let config = ton_executor::BlockchainConfig::with_config(params, block.global_id)
            .context("Invalid config")?;

        Ok(config)
    }

    async fn broadcast_message(&self, message: ton_block::Message) -> Result<(), RunError> {
        match message.header() {
            CommonMsgInfo::IntMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("IntMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtOutMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("ExtOutMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtInMsgInfo(_) => {}
        }

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::SendMessage(rpc::request::SendMessage {
                message: bytes::Bytes::from(message.write_to_bytes()?),
            })),
        });

        let result = self
            .request(&request)
            .await
            .map(|x| x.result.result)?
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::SendMessage(()) => Ok(()),
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        let message_hash = bytes::Bytes::copy_from_slice(message_hash);
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetDstTransaction(
                rpc::request::GetDstTransaction { message_hash },
            )),
        });

        let response = self.request(&request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetRawTransaction(tx)) => match tx.transaction {
                Some(bytes) => Ok(Some(Transaction::construct_from_bytes(bytes.as_ref())?)),
                None => Ok(None),
            },
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        let address = utils::addr_to_bytes(address);

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetContractState(
                rpc::request::GetContractState { address },
            )),
        });

        let result = self
            .request(&request)
            .await?
            .into_inner()
            .result
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::GetContractState(state) => match state.contract_state {
                Some(state) => {
                    let account = utils::deserialize_account_stuff(&state.account)?;

                    let timings = state
                        .gen_timings
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                        .into();

                    let last_transaction_id = state
                        .last_transaction_id
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                        .into();

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

    /// Works like `get_contract_state`, but also checks that node has state older than `time`.
    /// If state is not fresh, returns `Err`.
    /// This method should be used with `ChooseStrategy::TimeBased`.
    async fn get_contract_state_with_time_check(
        &self,
        address: &MsgAddressInt,
        time: u32,
    ) -> Result<Option<ExistingContract>, RunError> {
        let address = utils::addr_to_bytes(address);

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetContractState(
                rpc::request::GetContractState { address },
            )),
        });

        let response = self.request(&request).await?;
        let has_state_for = response.has_state_for(time);

        match response.result.result {
            Some(rpc::response::Result::GetContractState(state)) => match state.contract_state {
                Some(state) => {
                    let account = utils::deserialize_account_stuff(&state.account)?;

                    let timings = state
                        .gen_timings
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                        .into();

                    let last_transaction_id = state
                        .last_transaction_id
                        .ok_or::<RunError>(ClientError::InvalidResponse.into())?
                        .into();

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
}

impl<T: Connection + Ord + Clone + 'static> ProtoClientImpl<T> {
    pub async fn get_latest_key_block(&self) -> Result<ton_block::Block, RunError> {
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetLatestKeyBlock(())),
        });

        let response = self.request(&request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetLatestKeyBlock(key_block)) => Ok(
                ton_block::Block::construct_from_bytes(key_block.block.as_ref())?,
            ),
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    pub async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        let account = utils::addr_to_bytes(account);

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetTransactionsList(
                rpc::request::GetTransactionsList {
                    account,
                    last_transaction_lt,
                    limit: limit as u32,
                },
            )),
        });

        let response = self.request(&request).await?.into_inner();
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

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetTransaction(
                rpc::request::GetTransaction {
                    id: bytes::Bytes::copy_from_slice(tx_hash.as_slice()),
                },
            )),
        });

        let response = self.request(&request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetRawTransaction(tx)) => match tx.transaction {
                Some(bytes) => Ok(Some(Transaction::construct_from_bytes(bytes.as_ref())?)),
                None => Ok(None),
            },
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    pub async fn request<'a, S>(
        &self,
        request: &RpcRequest<'a, S>,
    ) -> Result<Answer<rpc::Response>, RunError>
    where
        S: Serialize + Send + Sync + Clone,
    {
        const NUM_RETRIES: usize = 10;

        for tries in 0..=NUM_RETRIES {
            let client = self
                .state
                .get_client()
                .await
                .ok_or::<RunError>(ClientError::NoEndpointsAvailable.into())?;

            let response = match client.request(request).await {
                Ok(res) => ProtoAnswer::parse_response(res).await,
                Err(e) => Err(e.into()),
            };

            let response = match response {
                Ok(a) => a,
                Err(e) => {
                    if let RpcRequest::PROTO(req) = request {
                        tracing::error!(
                            error = ?req.call,
                            "Error while sending PROTO request to endpoint: {e:?}"
                        );
                    }

                    self.state.remove_endpoint(client.endpoint());

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

#[derive(Clone, Debug)]
pub struct ProtoConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<Timings>>>,
}

impl PartialEq<Self> for ProtoConnection {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint() == other.endpoint()
    }
}

impl Eq for ProtoConnection {}

impl PartialOrd<Self> for ProtoConnection {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ProtoConnection {
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
        f.write_str(self.endpoint())
    }
}

#[async_trait::async_trait]
impl Connection for ProtoConnection {
    fn new(endpoint: String, client: reqwest::Client) -> Self {
        ProtoConnection {
            endpoint: Arc::new(endpoint),
            client,
            was_dead: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(Default::default()),
        }
    }

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
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetTimings(())),
        });

        let response = match self.request(&request).await {
            Ok(res) => ProtoAnswer::parse_response(res).await,
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
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetLatestKeyBlock(())),
        });

        let response = match self.request(&request).await {
            Ok(res) => ProtoAnswer::parse_response(res).await,
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
}
