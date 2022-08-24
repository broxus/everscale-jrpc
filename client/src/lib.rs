#![deny(clippy::dbg_macro)]
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use everscale_jrpc_models::*;
use futures::StreamExt;
use nekoton::transport::models::ExistingContract;
use parking_lot::RwLock;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use ton_block::{Message, MsgAddressInt};

#[derive(Clone)]
pub struct JrpcClient {
    state: Arc<State>,
}

impl JrpcClient {
    /// [endpoints] full URLs of the RPC endpoints.
    pub async fn new<I: IntoIterator<Item = Url>>(
        endpoints: I,
        options: JrpcClientOptions,
    ) -> Result<Self> {
        let client = reqwest::ClientBuilder::new().gzip(true).build()?;
        let client = Self {
            state: Arc::new(State {
                endpoints: endpoints
                    .into_iter()
                    .map(|e| JrpcConnection::new(e.to_string(), client.clone()))
                    .collect(),
                live_endpoints: Default::default(),
            }),
        };

        let state = client.state.clone();
        state.update_endpoints().await;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(options.probe_interval).await;
                state.update_endpoints().await;
            }
        });

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
        use nekoton::utils::SimpleClock;

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

    pub async fn send_message(&self, message: Message, options: SendOptions) -> Result<SendStatus> {
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

        self.broadcast_message(message).await?;
        log::debug!("Message broadcasted");

        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(options.poll_interval).await;

            let state = self.get_contract_state(&dst).await;
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
                return Ok(SendStatus::Confirmed);
            }
            if start.elapsed() > options.ttl {
                return Ok(SendStatus::Expired);
            }
            log::debug!("Message not confirmed yet");
        }
    }

    pub async fn get_latest_key_block(&self) -> Result<BlockResponse> {
        self.request("getLatestKeyBlock", ()).await
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

pub struct JrpcClientOptions {
    /// How often the probe should update health statuses.
    ///
    /// Default: `60 sec`
    pub probe_interval: Duration,
}

impl Default for JrpcClientOptions {
    fn default() -> Self {
        Self {
            probe_interval: Duration::from_secs(64),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendStatus {
    Confirmed,
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

        let req = self
            .client
            .post(self.endpoint.as_str())
            .json(&JrpcRequest { method, params });

        #[derive(Debug, Deserialize)]
        struct JsonRpcResponse<T> {
            result: JsonRpcAnswer<T>,
        }

        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum JsonRpcAnswer<T> {
            Result(T),
            Error(JsonRpcError),
        }

        #[derive(Debug, Deserialize)]
        struct JsonRpcError {
            code: i32,
            message: String,
        }

        // let res = req.send().await?.json::<JsonRpcRepsonse>().await?;
        // Ok(res)
        let JsonRpcResponse { result } = req.send().await?.json().await?;
        match result {
            JsonRpcAnswer::Result(result) => Ok(result),
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
}

#[derive(thiserror::Error, Debug)]
enum JrpcClientError {
    #[error("No endpoints available")]
    NoEndpointsAvailable,
    #[error("Error response ({0}): {1}")]
    ErrorResponse(i32, String),
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
            },
        )
        .await
        .unwrap();

        for _ in 0..100 {
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

    async fn get_client() -> JrpcClient {
        let pr = JrpcClient::new(
            ["https://jrpc.everwallet.net/rpc".parse().unwrap()],
            JrpcClientOptions {
                probe_interval: Duration::from_secs(10),
            },
        )
        .await
        .unwrap();
        pr
    }
}
