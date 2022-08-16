#![deny(clippy::dbg_macro)]
use std::cmp::Ordering;
use std::{
    collections::HashSet,
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use everscale_jrpc_models::ContractStateResponse;
use futures::StreamExt;
use nekoton::transport::models::ExistingContract;
use nekoton_utils::SimpleClock;
use parking_lot::RwLock;
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use ton_block::{Message, MsgAddressInt};

#[derive(Debug, Clone)]
pub struct LoadBalancedRpc {
    state: Arc<RpcState>,
}

pub struct LoadBalancedRpcOptions {
    /// How often the probe should update health statuses.
    pub prove_interval: Duration,
}

impl LoadBalancedRpc {
    /// [endpoints] full URLs of the RPC endpoints.
    pub async fn new<I: IntoIterator<Item = Url>>(
        endpoints: I,
        options: LoadBalancedRpcOptions,
    ) -> Result<Self> {
        let client = reqwest::ClientBuilder::new().gzip(true).build()?;
        let client = Self {
            state: Arc::new(RpcState {
                endpoints: endpoints
                    .into_iter()
                    .map(|e| RpcClientInner::with_client(e.to_string(), client.clone()))
                    .collect(),
                live_endpoints: Default::default(),
            }),
        };

        let state = client.state.clone();
        state.update_endpoints().await;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(options.prove_interval).await;
                state.update_endpoints().await;
            }
        });

        Ok(client)
    }

    pub async fn request<T: Serialize>(&self, req: JrpcRequest<'_, T>) -> JsonRpcRepsonse {
        let client = match self.state.get_client() {
            Some(client) => client,
            None => {
                return JsonRpcRepsonse {
                    jsonrpc: "2.0".to_string(),
                    result: JsonRpcAnswer::Error(JsonRpcError {
                        code: -32603,
                        message: "No endpoint available".to_string(),
                    }),
                    id: req.id,
                }
            }
        };

        let id = req.id;
        match client.request(req).await {
            Ok(a) => a,
            Err(e) => {
                log::error!("Error while sending request to endpoint: {e:?}");
                self.state.remove_endpoint(&client.endpoint);

                JsonRpcRepsonse {
                    jsonrpc: "2.0".to_string(),
                    result: JsonRpcAnswer::Error(JsonRpcError {
                        code: -32603,
                        message: "Internal error".to_string(),
                    }),
                    id,
                }
            }
        }
    }

    pub async fn get_contract_state(
        &self,
        contract_address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        #[derive(Serialize)]
        struct Request {
            address: String,
        }

        let req = Request {
            address: contract_address.to_string(),
        };

        let req = JrpcRequest {
            id: 13,
            method: "getContractState",
            params: req,
        };

        let response = self.request(req).await;
        let parsed: ContractStateResponse = response.unwrap()?;
        let response = match parsed {
            ContractStateResponse::NotExists => None,
            ContractStateResponse::Exists {
                account,
                timings,
                last_transaction_id,
            } => Some(ExistingContract {
                account,
                timings,
                last_transaction_id,
            }),
        };
        Ok(response)
    }

    pub async fn run_local(
        &self,
        contract_address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton_abi::ExecutionOutput>> {
        use nekoton_abi::FunctionExt;

        let state = match self.get_contract_state(contract_address).await? {
            Some(a) => a,
            None => return Ok(None),
        };

        function
            .clone()
            .run_local(&SimpleClock, state.account, input)
            .map(Some)
    }

    pub async fn send_message(&self, message: Message) -> Result<()> {
        anyhow::ensure!(
            message.is_inbound_external(),
            "Only inbound external messages are allowed"
        );
        let req = everscale_jrpc_models::SendMessageRequest { message };

        let response = self
            .request(JrpcRequest {
                id: 13,
                method: "sendMessage",
                params: req,
            })
            .await;
        match response.result {
            JsonRpcAnswer::Result(_) => Ok(()),
            JsonRpcAnswer::Error(e) => {
                anyhow::bail!("Error while sending message: {e:?}")
            }
        }
    }

    pub async fn send_with_confirmation(
        &self,
        message: Message,
        options: SendOptions,
    ) -> Result<SendStatus> {
        let dst = message
            .dst()
            .context("Only inbound external messages are allowed")?;
        let initial_state = self
            .get_contract_state(&dst)
            .await
            .context("Failed to get dst state")?;

        log::debug!(
            "Initial state. Contract exists: {}",
            initial_state.is_some()
        );
        self.send_message(message).await?;
        log::debug!("Message sent");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;
            let state = self.get_contract_state(&dst).await;
            let state = match (options.error_action, state) {
                (TransportErrorAction::Poll, Err(e)) => {
                    log::error!("Error while polling for message: {e:?}. Continue polling",);
                    continue;
                }
                (TransportErrorAction::Return, Err(e)) => {
                    return Err(e);
                }
                (_, Ok(res)) => res,
            };

            if state.partial_cmp(&initial_state) == Some(Ordering::Greater) {
                return Ok(SendStatus::Confirmed);
            }
            if start.elapsed() > options.ttl {
                return Ok(SendStatus::Expired);
            }
            log::debug!("Message not confirmed yet");
        }
    }
}

pub struct SendOptions {
    /// action to perform if an error occurs during waiting for message delivery.
    pub error_action: TransportErrorAction,
    /// time after which the message is considered as expired.
    pub ttl: Duration,
    /// how often the message is checked for delivery.
    pub poll_interval: Duration,
}

#[derive(Copy, Clone)]
pub enum TransportErrorAction {
    /// Poll endlessly until message is delivered or expired
    Poll,
    /// Fail immediately
    Return,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendStatus {
    Confirmed,
    Expired,
}

struct RpcState {
    endpoints: Vec<RpcClientInner>,
    live_endpoints: RwLock<Vec<RpcClientInner>>,
}

impl Debug for RpcState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcState")
            .field("endpoints", &self.endpoints)
            .field("live_endpoints", &self.live_endpoints)
            .finish()
    }
}

impl RpcState {
    fn get_client(&self) -> Option<RpcClientInner> {
        use rand::prelude::SliceRandom;

        let live_endpoints = self.live_endpoints.read();
        live_endpoints.choose(&mut rand::thread_rng()).cloned()
    }

    async fn update_endpoints(&self) {
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
    }

    fn remove_endpoint(&self, endpoint: &str) {
        self.live_endpoints
            .write()
            .retain(|c| c.endpoint.as_ref() != endpoint);

        log::warn!("Removed endpoint {endpoint} from the list of endpoints");
    }
}

#[derive(Clone, Debug)]
struct RpcClientInner {
    endpoint: Arc<String>,
    client: reqwest::Client,
}

impl std::fmt::Display for RpcClientInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RpcClientInner")
            .field(&self.endpoint)
            .finish()
    }
}

impl RpcClientInner {
    fn with_client(endpoint: String, client: reqwest::Client) -> Self {
        log::info!("Created new RPC client for endpoint: {}", endpoint);
        RpcClientInner {
            endpoint: Arc::new(endpoint),
            client,
        }
    }

    async fn request<T: Serialize>(&self, request: JrpcRequest<'_, T>) -> Result<JsonRpcRepsonse> {
        let req = self.client.post(self.endpoint.as_str()).json(&request);
        // let res = req.send().await?.json::<JsonRpcRepsonse>().await?;
        // Ok(res)
        let res = req.send().await?.text().await?;
        Ok(serde_json::from_str(&res)?)
    }

    async fn is_alive(&self) -> bool {
        match self
            .request(JrpcRequest {
                id: 1337,
                method: "getLatestKeyBlock",
                params: (),
            })
            .await
        {
            Ok(res) => matches!(res.result, JsonRpcAnswer::Result(_)),
            Err(e) => {
                log::error!("{} seems to be dead: {e:?}", self.endpoint);
                false
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JrpcRequest<'a, T> {
    pub id: i64,
    pub method: &'a str,
    pub params: T,
}

impl<T: Serialize> Serialize for JrpcRequest<'_, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut s = serializer.serialize_struct("JrpcRequest", 4)?;
        s.serialize_field("jsonrpc", "2.0")?;
        s.serialize_field("id", &self.id)?;
        s.serialize_field("method", self.method)?;
        s.serialize_field("params", &self.params)?;
        s.end()
    }
}

#[derive(Serialize)]
struct Jrpc<T> {
    jsonrpc: &'static str,
    id: i32,
    method: &'static str,
    params: T,
}

#[derive(Serialize, Debug, Deserialize)]
/// A JSON-RPC response.
pub struct JsonRpcRepsonse {
    jsonrpc: String,
    pub result: JsonRpcAnswer,
    /// The request ID.
    id: i64,
}

impl JsonRpcRepsonse {
    pub fn unwrap<T>(self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        match self.result {
            JsonRpcAnswer::Result(x) => Ok(serde_json::from_value(x)?),
            JsonRpcAnswer::Error(x) => anyhow::bail!("{:?}", x),
        }
    }
}

#[derive(Serialize, Debug, Deserialize)]
#[serde(untagged)]
/// JsonRpc [response object](https://www.jsonrpc.org/specification#response_object)
pub enum JsonRpcAnswer {
    Result(Value),
    Error(JsonRpcError),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcError {
    code: i32,
    message: String,
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
            "https://extension-api.broxus.com/rpc",
        ]
        .iter()
        .map(|x| x.parse().unwrap())
        .collect::<Vec<_>>();

        let balanced_client = LoadBalancedRpc::new(
            rpc,
            LoadBalancedRpcOptions {
                prove_interval: Duration::from_secs(10),
            },
        )
        .await
        .unwrap();

        let request = JrpcRequest {
            id: 1337,
            method: "getLatestKeyBlock",
            params: (),
        };

        for _ in 0..100 {
            let response = balanced_client.request(request.clone()).await;
            log::info!(
                "response is ok: {}",
                matches!(response.result, JsonRpcAnswer::Result(_))
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    #[tokio::test]
    async fn test_get() {
        let pr = LoadBalancedRpc::new(
            ["https://extension-api.broxus.com/rpc".parse().unwrap()],
            LoadBalancedRpcOptions {
                prove_interval: Duration::from_secs(10),
            },
        )
        .await
        .unwrap();

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
}
