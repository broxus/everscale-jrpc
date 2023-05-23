//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use everscale_jrpc_server::*;
//! use async_trait::async_trait;
//! struct ExampleSubscriber {
//!     jrpc_state: Arc<JrpcState>,
//! }
//!
//! #[async_trait]
//! impl ton_indexer::Subscriber for ExampleSubscriber {
//!     async fn process_block(&self, ctx: ton_indexer::ProcessBlockContext<'_>) -> Result<()> {
//!         if let Some(shard_state) = ctx.shard_state_stuff() {
//!             self.jrpc_state.handle_block(ctx.block_stuff(), shard_state)?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! async fn test(
//!     config: ton_indexer::NodeConfig,
//!     global_config: ton_indexer::GlobalConfig,
//!     listen_address: std::net::SocketAddr,
//! ) -> Result<()> {
//!     let jrpc_state = Arc::new(JrpcState::new(None));
//!     let subscriber: Arc<dyn ton_indexer::Subscriber> = Arc::new(ExampleSubscriber {
//!         jrpc_state: jrpc_state.clone(),
//!     });
//!
//!     let engine = ton_indexer::Engine::new(config, global_config, vec![subscriber]).await?;
//!
//!     engine.start().await?;
//!
//!     let jrpc = JrpcServer::with_state(jrpc_state).build(&engine, listen_address).await?;
//!     tokio::spawn(jrpc);
//!
//!     // ...
//!
//!     Ok(())
//! }
//! ```

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::State;
use axum::routing::{get, post};
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use axum_jrpc::JsonRpcResponse;

pub use everscale_jrpc_models as models;
use everscale_jrpc_models::*;

use self::state::Counters;
pub use self::state::{JrpcMetrics, JrpcState, JrpcStorageOptions};

mod state;

pub struct JrpcServer {
    state: Arc<JrpcState>,
}

impl JrpcServer {
    pub fn with_state(state: Arc<JrpcState>) -> Self {
        Self { state }
    }

    /// Initializes state and returns server future
    pub async fn build(
        self,
        engine: &Arc<ton_indexer::Engine>,
        listen_address: SocketAddr,
    ) -> Result<impl Future<Output = ()> + Send + 'static> {
        self.state.initialize(engine).await?;

        let service = tower::ServiceBuilder::new();
        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        let router = axum::Router::new()
            .route("/", get(health_check))
            .route("/", post(jrpc_router))
            .route("/rpc", post(jrpc_router))
            .layer(service)
            .with_state(self.state);

        let future = axum::Server::try_bind(&listen_address)?
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(60).into())
            .serve(router.into_make_service());

        Ok(async move { future.await.unwrap() })
    }
}

async fn health_check() -> impl axum::response::IntoResponse {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis()
        .to_string()
}

async fn jrpc_router(
    State(ctx): State<Arc<JrpcState>>,
    req: axum_jrpc::JsonRpcExtractor,
) -> axum_jrpc::JrpcResult {
    let counters = ctx.counters();
    counters.increase_total();

    let answer_id = req.get_answer_id();
    let method = req.method();
    let answer = match method {
        "getLatestKeyBlock" => match ctx.get_last_key_block() {
            Ok(b) => JsonRpcResponse::success(answer_id, b.as_ref()),
            Err(e) => make_error(answer_id, e, counters),
        },
        "getContractState" => {
            let req: GetContractStateRequest = req.parse_params()?;
            match ctx.get_contract_state(&req.address) {
                Ok(state) => JsonRpcResponse::success(answer_id, state),
                Err(e) => make_error(answer_id, e, counters),
            }
        }
        "getTransactionsList" => {
            let now = std::time::Instant::now();
            let req: GetTransactionsListRequest = req.parse_params()?;
            match ctx.get_transactions(&req.account, req.last_transaction_lt, req.limit) {
                Ok(txs) => {
                    tracing::info!(elapsed_ns = now.elapsed().as_nanos());
                    JsonRpcResponse::success(answer_id, txs)
                }
                Err(e) => {
                    tracing::error!(error = ?e, method = "getTransactionsList");
                    make_error(answer_id, QueryError::StorageError, counters)
                }
            }
        }
        "getTransaction" => {
            let req: GetTransactionRequest = req.parse_params()?;
            match ctx.get_transaction(&req.id) {
                Ok(tx) => JsonRpcResponse::success(answer_id, tx.map(base64::encode)),
                Err(e) => {
                    tracing::error!(error = ?e, method = "getTransaction");
                    make_error(answer_id, QueryError::StorageError, counters)
                }
            }
        }
        "getDstTransaction" => {
            let req: GetDstTransactionRequest = req.parse_params()?;
            match ctx.get_dst_transaction(&req.message_hash) {
                Ok(tx) => JsonRpcResponse::success(answer_id, tx.map(base64::encode)),
                Err(e) => {
                    tracing::error!(error = ?e, method = "getDstTransaction");
                    make_error(answer_id, QueryError::StorageError, counters)
                }
            }
        }
        "sendMessage" => {
            let req: SendMessageRequest = req.parse_params()?;
            match ctx.send_message(req.message).await {
                Ok(_) => JsonRpcResponse::success(answer_id, ()),
                Err(e) => make_error(answer_id, e, counters),
            }
        }
        "getStatus" => JsonRpcResponse::success(
            answer_id,
            StatusResponse {
                ready: ctx.is_ready(),
            },
        ),
        "getTimings" => match ctx.timings() {
            Ok(stats) => JsonRpcResponse::success(answer_id, stats),
            Err(e) => make_error(answer_id, e, counters),
        },
        m => {
            counters.increase_not_found();
            req.method_not_found(m)
        }
    };

    Ok(answer)
}

fn make_error(answer_id: i64, error: QueryError, counters: &Counters) -> JsonRpcResponse {
    counters.increase_errors();
    JsonRpcResponse::error(answer_id, error.into())
}

pub type QueryResult<T> = Result<T, QueryError>;

#[derive(thiserror::Error, Clone, Debug)]
pub enum QueryError {
    #[error("Connection error")]
    ConnectionError,
    #[error("Failed to serialize message")]
    FailedToSerialize,
    #[error("Invalid account state")]
    InvalidAccountState,
    #[error("External message expected")]
    ExternalMessageExpected,
    #[error("Storage error")]
    StorageError,
    #[error("Not ready")]
    NotReady,
}

impl QueryError {
    pub fn code(&self) -> i64 {
        match self {
            QueryError::ConnectionError => -32001,
            QueryError::FailedToSerialize => -32002,
            QueryError::InvalidAccountState => -32004,
            QueryError::ExternalMessageExpected => -32005,
            QueryError::StorageError => -32006,
            QueryError::NotReady => -32007,
        }
    }
}

impl From<QueryError> for JsonRpcError {
    fn from(error: QueryError) -> JsonRpcError {
        let code = error.code();
        let message = error.to_string();
        let reason = JsonRpcErrorReason::ServerError(code as i32);
        JsonRpcError::new(reason, message, serde_json::Value::Null)
    }
}
