#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use ton_block::{CommonMsgInfo, Deserializable, Message, MsgAddressInt, Transaction};
use ton_types::UInt256;

use everscale_rpc_models::{jrpc, Timings};

use crate::*;

pub type JrpcClient = JrpcClientImpl<JrpcConnection>;

#[derive(Clone)]
pub struct JrpcClientImpl<T: Connection + Ord + Clone> {
    state: Arc<State<T>>,
    is_capable_of_message_tracking: bool,
}

#[async_trait::async_trait]
impl<T> Client<T> for JrpcClientImpl<T>
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
        let key_block = self.get_latest_key_block().await?;
        let block = key_block.block;

        let extra = block.read_extra()?;

        let master = extra.read_custom()?.context("No masterchain block extra")?;

        let params = master.config().context("Invalid config")?.clone();

        let config = ton_executor::BlockchainConfig::with_config(params, block.global_id)
            .context("Invalid config")?;

        Ok(config)
    }

    async fn broadcast_message(&self, message: Message) -> Result<(), RunError> {
        match message.header() {
            CommonMsgInfo::IntMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("IntMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtOutMsgInfo(_) => {
                return Err(RunError::NotInboundMessage("ExtOutMsgInfo".to_string()));
            }
            CommonMsgInfo::ExtInMsgInfo(_) => {}
        }

        let params = &jrpc::SendMessageRequest { message };
        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "sendMessage",
            params,
        });

        self.request(&request).await.map(|x| x.result)
    }

    async fn get_dst_transaction(&self, message_hash: &[u8]) -> Result<Option<Transaction>> {
        let message_hash = message_hash.try_into().context("Invalid message hash")?;
        let params = &jrpc::GetDstTransactionRequest { message_hash };
        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getDstTransaction",
            params,
        });

        let result: Vec<u8> = match self.request(&request).await?.into_inner() {
            Some(s) => base64::decode::<String>(s)?,
            None => return Ok(None),
        };
        let result = Transaction::construct_from_bytes(result.as_slice())?;

        Ok(Some(result))
    }

    async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        let params = &jrpc::GetContractStateRequestRef { address };
        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getContractState",
            params,
        });

        match self.request(&request).await?.into_inner() {
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

    /// Works like `get_contract_state`, but also checks that node has state older than `time`.
    /// If state is not fresh, returns `Err`.
    /// This method should be used with `ChooseStrategy::TimeBased`.
    async fn get_contract_state_with_time_check(
        &self,
        address: &MsgAddressInt,
        time: u32,
    ) -> Result<Option<ExistingContract>, RunError> {
        let params = &jrpc::GetContractStateRequestRef { address };

        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getContractState",
            params,
        });

        let response = self.request(&request).await?;
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
}

impl<T: Connection + Ord + Clone + 'static> JrpcClientImpl<T> {
    pub async fn get_latest_key_block(&self) -> Result<jrpc::GetLatestKeyBlockResponse, RunError> {
        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getLatestKeyBlock",
            params: &(),
        });

        self.request(&request).await.map(|x| x.result)
    }

    pub async fn get_transactions(
        &self,
        limit: u8,
        account: &MsgAddressInt,
        last_transaction_lt: Option<u64>,
    ) -> Result<Vec<Transaction>> {
        let params = &jrpc::GetTransactionsListRequestRef {
            account,
            limit,
            last_transaction_lt,
        };

        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getTransactionsList",
            params,
        });

        let data: Vec<String> = self.request(&request).await?.into_inner();
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

        let params = &jrpc::GetTransactionRequestRef {
            id: tx_hash.as_slice(),
        };

        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getTransaction",
            params,
        });

        let data: Option<String> = self.request(&request).await?.into_inner();
        let raw_transaction = match data {
            Some(data) => decode_raw_transaction(&data)?,
            None => return Ok(None),
        };

        Ok(Some(raw_transaction))
    }

    pub async fn request<'a, S, D>(
        &self,
        request: &RpcRequest<'a, S>,
    ) -> Result<Answer<D>, RunError>
    where
        S: Serialize + Send + Sync + Clone,
        for<'de> D: Deserialize<'de>,
    {
        const NUM_RETRIES: usize = 10;

        for tries in 0..=NUM_RETRIES {
            let client = self
                .state
                .get_client()
                .await
                .ok_or::<RunError>(ClientError::NoEndpointsAvailable.into())?;

            let res = match client.request(request).await {
                Ok(res) => res.json::<JsonRpcResponse>().await,
                Err(e) => Err(e),
            };

            let response = match res {
                Ok(a) => a,
                Err(e) => {
                    if let RpcRequest::JRPC(req) = request {
                        tracing::error!(
                            req.method,
                            "Error while sending JRPC request to endpoint: {e:?}"
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

            match response.result {
                JsonRpcAnswer::Result(result) => {
                    return Ok(Answer {
                        #[cfg(not(feature = "simd"))]
                        result: serde_json::from_value(result)?,
                        #[cfg(feature = "simd")]
                        result: simd_json::serde::from_owned_value(result)?,
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
pub struct JrpcConnection {
    endpoint: Arc<String>,
    client: reqwest::Client,
    was_dead: Arc<AtomicBool>,
    stats: Arc<Mutex<Option<Timings>>>,
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
    fn new(endpoint: String, client: reqwest::Client) -> Self {
        JrpcConnection {
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
        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getTimings",
            params: &(),
        });

        let res = match self.request(&request).await {
            Ok(res) => res.json::<JsonRpcResponse>().await,
            Err(e) => Err(e),
        };

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
                #[cfg(not(feature = "simd"))]
                let timings: Result<jrpc::GetTimingsResponse, _> = serde_json::from_value(v);

                #[cfg(feature = "simd")]
                let timings: Result<jrpc::GetTimingsResponse, _> =
                    simd_json::serde::from_owned_value(v);

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

        let request: RpcRequest<_> = RpcRequest::JRPC(JrpcRequest {
            method: "getLatestKeyBlock",
            params: &(),
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
        let req = self.client.post(self.endpoint.as_str()).json(&JrpcRequest {
            method,
            params: &(),
        });

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
pub struct JrpcRequest<'a, T> {
    method: &'a str,
    params: &'a T,
}

impl<'a, T: Serialize> JrpcRequest<'a, T> {
    pub fn new(method: &'a str, params: &'a T) -> Self {
        Self { method, params }
    }
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
        s.serialize_field("params", self.params)?;
        s.end()
    }
}

#[derive(Serialize, Debug, Deserialize)]
/// A JSON-RPC response.
pub struct JsonRpcResponse {
    #[serde(flatten)]
    pub result: JsonRpcAnswer,
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
/// JsonRpc [response object](https://www.jsonrpc.org/specification#response_object)
pub enum JsonRpcAnswer {
    #[cfg(not(feature = "simd"))]
    Result(serde_json::Value),
    #[cfg(feature = "simd")]
    Result(simd_json::owned::Value),
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
        tracing_subscriber::fmt::init();

        let rpc = [
            "http://127.0.0.1:8081",
            "http://34.78.198.249:8081/rpc",
            "https://jrpc.everwallet.net/rpc",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect::<Vec<_>>();

        let balanced_client = JrpcClient::new(
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

    async fn get_client() -> JrpcClient {
        JrpcClient::new(
            [
                "https://jrpc.everwallet.net/rpc".parse().unwrap(),
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

    #[test]
    fn test_serde() {
        let mut err = br#"
        {
	        "jsonrpc": "2.0",
	        "error": {
	        	"code": -32601,
	        	"message": "Method `getContractState1` not found",
	        	"data": null
	        },
	        "id": 1
        }"#
        .to_vec();

        #[cfg(not(feature = "simd"))]
        let resp: JsonRpcResponse = serde_json::from_slice(&mut err).unwrap();
        #[cfg(feature = "simd")]
        let resp: JsonRpcResponse = simd_json::serde::from_slice(&mut err).unwrap();
        match resp.result {
            JsonRpcAnswer::Error(e) => {
                assert_eq!(e.code, -32601);
            }
            _ => panic!("expected error"),
        }
    }
}
