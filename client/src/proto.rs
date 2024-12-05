#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use nekoton::transport::models::ExistingContract;
use parking_lot::Mutex;
use reqwest::StatusCode;
use ton_block::{
    CommonMsgInfo, ConfigParams, Deserializable, MsgAddressInt, Serializable, Transaction,
};
use ton_types::UInt256;

use everscale_rpc_models::proto::ProtoAnswer;
use everscale_rpc_models::Timings;
use nekoton_proto::prost::{bytes, Message};
use nekoton_proto::protos::rpc;
use nekoton_proto::utils;

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
        Ok(self.get_bc_config().await?)
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
        last_transaction_lt: Option<u64>,
    ) -> Result<Option<ExistingContract>> {
        let address = utils::addr_to_bytes(address);

        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetContractState(
                rpc::request::GetContractState {
                    address,
                    last_transaction_lt,
                },
            )),
        });

        let result = self
            .request(&request)
            .await?
            .into_inner()
            .result
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?;

        match result {
            rpc::response::Result::GetContractState(state) => match state.state {
                Some(rpc::response::get_contract_state::State::NotExists(_)) => Ok(None),
                Some(rpc::response::get_contract_state::State::Exists(state)) => {
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
                Some(rpc::response::get_contract_state::State::Unchanged(_)) => {
                    Err(anyhow::anyhow!("Contract state is unchanged"))
                }
                None => Err(ClientError::InvalidResponse.into()),
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
                rpc::request::GetContractState {
                    address,
                    last_transaction_lt: None,
                },
            )),
        });

        let response = self.request(&request).await?;
        let has_state_for = response.has_state_for(time);

        match response
            .into_inner()
            .result
            .ok_or::<RunError>(ClientError::InvalidResponse.into())?
        {
            rpc::response::Result::GetContractState(state) => match state.state {
                Some(rpc::response::get_contract_state::State::NotExists(
                    rpc::response::get_contract_state::NotExist {
                        gen_timings: Some(timings),
                    },
                )) => {
                    match timings {
                        rpc::response::get_contract_state::not_exist::GenTimings::Known(
                            rpc::response::get_contract_state::Timings { gen_utime, .. },
                        ) => {
                            if gen_utime < time {
                                return Err(RunError::NoStateForTimeStamp(time));
                            }
                        }
                        rpc::response::get_contract_state::not_exist::GenTimings::Unknown(()) => {
                            has_state_for?
                        }
                    }

                    Ok(None)
                }
                Some(rpc::response::get_contract_state::State::Exists(state)) => {
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
                Some(rpc::response::get_contract_state::State::Unchanged(_)) => Err(
                    RunError::Generic(anyhow::anyhow!("Contract state is unchanged")),
                ),
                _ => Err(ClientError::InvalidResponse.into()),
            },
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    async fn get_account_by_code_hash(
        &self,
        code_hash: [u8; 32],
        continuation: Option<&MsgAddressInt>,
        limit: u8,
    ) -> Result<Vec<MsgAddressInt>> {
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetAccountsByCodeHash(
                rpc::request::GetAccountsByCodeHash {
                    code_hash: bytes::Bytes::copy_from_slice(&code_hash),
                    continuation: continuation.map(utils::addr_to_bytes),
                    limit: limit as u32,
                },
            )),
        });

        let response = self.request(&request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetAccounts(accounts)) => Ok(accounts
                .account
                .into_iter()
                .filter_map(|x| utils::bytes_to_addr(&x).ok())
                .collect()),
            _ => Err(ClientError::InvalidResponse.into()),
        }
    }

    async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        self.get_transactions(limit, account, last_transaction_lt)
            .await
    }

    async fn get_keyblock(&self) -> Result<ton_block::Block> {
        let res = self.get_latest_key_block().await?;
        Ok(res)
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

    async fn get_bc_config(&self) -> Result<ton_executor::BlockchainConfig, RunError> {
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetBlockchainConfig(())),
        });

        let response = self.request(&request).await?.into_inner();
        match response.result {
            Some(rpc::response::Result::GetBlockchainConfig(blockchain_config)) => {
                let config_params = decode_config_params(&blockchain_config.config)?;
                Ok(ton_executor::BlockchainConfig::with_config(
                    config_params,
                    blockchain_config.global_id,
                )?)
            }
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
                Ok(res) => parse_response(res).await,
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

fn decode_config_params(bytes: &bytes::Bytes) -> Result<ConfigParams> {
    let cell = ton_types::deserialize_tree_of_cells(&mut bytes.as_ref())?;
    ConfigParams::construct_from_cell(cell)
}

#[derive(Clone, Debug)]
pub struct ProtoConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<Timings>>>,
    reliability_params: Arc<ReliabilityParams>,
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
    fn new(
        endpoint: String,
        client: reqwest::Client,
        reliability_params: ReliabilityParams,
    ) -> Self {
        ProtoConnection {
            endpoint: Arc::new(endpoint),
            client,
            was_dead: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(Default::default()),
            reliability_params: Arc::new(reliability_params),
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

    fn get_reliability_params(&self) -> &ReliabilityParams {
        &self.reliability_params
    }

    async fn is_alive_inner(&self) -> LiveCheckResult {
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetTimings(())),
        });

        let response = match self.request(&request).await {
            Ok(res) => parse_response(res).await,
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
                    let params = self.get_reliability_params();
                    let is_reliable = t.is_reliable(
                        params.mc_acceptable_time_diff_sec,
                        params.sc_acceptable_time_diff_sec,
                        params.acceptable_blocks_diff,
                    );
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

        // fallback to keyblock request
        let request: RpcRequest<()> = RpcRequest::PROTO(rpc::Request {
            call: Some(rpc::request::Call::GetLatestKeyBlock(())),
        });

        let response = match self.request(&request).await {
            Ok(res) => parse_response(res).await,
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
        let result = parse_response(response).await?;

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

async fn parse_response(res: reqwest::Response) -> Result<ProtoAnswer> {
    match res.status() {
        StatusCode::OK => ProtoAnswer::decode_success(res.bytes().await?),
        StatusCode::UNPROCESSABLE_ENTITY => ProtoAnswer::decode_error(res.bytes().await?),
        _ => anyhow::bail!(res.status().to_string()),
    }
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
            "http://34.78.198.249:8081/proto",
            "https://jrpc.everwallet.net/proto",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect::<Vec<_>>();

        let balanced_client = ProtoClient::new(
            rpc,
            ClientOptions {
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
            None,
        )
        .await
        .unwrap()
        .unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "-1:efd5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
            )
            .unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    }

    #[tokio::test]
    async fn test_key_block() {
        let pr = get_client().await;

        pr.get_latest_key_block().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_blockchain_config() {
        let pr = get_client().await;

        let config = pr.get_blockchain_config().await.unwrap();

        assert_eq!(config.global_id(), 42);
        assert_ne!(config.capabilites(), 0);
    }

    #[tokio::test]
    async fn test_all_dead() {
        let pr = JrpcClient::new(
            [
                "https://lolkek228.dead/rpc".parse().unwrap(),
                "http://127.0.0.1:8081".parse().unwrap(),
                "http://127.0.0.1:12333".parse().unwrap(),
            ],
            ClientOptions {
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
        ProtoClient::new(
            [
                "https://jrpc.everwallet.net/proto".parse().unwrap(),
                "http://127.0.0.1:8081".parse().unwrap(),
            ],
            ClientOptions {
                probe_interval: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }
}
