#![warn(clippy::dbg_macro)]
#![allow(clippy::large_enum_variant)]

use std::cmp;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use ton_block::Transaction;

use everscale_rpc_models::Timings;

pub mod jrpc;
pub mod proto;

static ROUND_ROBIN_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[async_trait::async_trait]
pub trait Connection: Send + Sync {
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

    async fn is_alive_inner(&self) -> LiveCheckResult;

    async fn method_is_supported(&self, method: &str) -> Result<bool>;

    async fn request(&self, request: Vec<u8>) -> Result<reqwest::Response, reqwest::Error>;
}

impl PartialEq<Self> for dyn Connection {
    fn eq(&self, other: &Self) -> bool {
        self.endpoint() == other.endpoint()
    }
}

impl Eq for dyn Connection {}

impl PartialOrd<Self> for dyn Connection {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn Connection {
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

impl std::fmt::Display for dyn Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.endpoint())
    }
}

struct State {
    endpoints: Vec<Arc<dyn Connection>>,
    live_endpoints: RwLock<Vec<Arc<dyn Connection>>>,
    options: ClientOptions,
}

impl State {
    async fn get_client(&self) -> Option<Arc<dyn Connection>> {
        for _ in 0..10 {
            let client = {
                let live_endpoints = self.live_endpoints.read();
                self.options.choose_strategy.choose(live_endpoints.clone())
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
    fn choose(&self, endpoints: Vec<Arc<dyn Connection>>) -> Option<Arc<dyn Connection>> {
        use rand::prelude::SliceRandom;

        //unimplemented!()

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
    #[error("JSON error: {0}")]
    ParseError(#[from] serde_json::Error),
    #[error("Invalid message type: {0}")]
    NotInboundMessage(String),
}
