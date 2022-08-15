//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use everscale_jrpc_server::*;
//!
//! struct ExampleSubscriber {
//!     jrpc_state: Arc<JrpcState>,
//! }
//!
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
//!     let jrpc_state = Arc::new(JrpcState::default());
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

use anyhow::Result;
use axum::routing::post;
use axum::Extension;
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use axum_jrpc::JsonRpcResponse;
use everscale_jrpc_models::*;

use self::state::Counters;
pub use self::state::{JrpcMetrics, JrpcState};
pub use everscale_jrpc_models as models;

mod state;

pub struct JrpcServer {
    route: String,
    state: Arc<JrpcState>,
}

impl JrpcServer {
    pub fn with_state(state: Arc<JrpcState>) -> Self {
        Self {
            route: "/rpc".to_string(),
            state,
        }
    }

    pub fn with_route(mut self, route: &str) -> Self {
        self.route = route.to_owned();
        self
    }

    /// Initializes state and returns server future
    pub async fn build(
        self,
        engine: &Arc<ton_indexer::Engine>,
        listen_address: SocketAddr,
    ) -> Result<impl Future<Output = ()> + Send + 'static> {
        self.state.initialize(engine).await?;

        let service = tower::ServiceBuilder::new().layer(Extension(self.state));
        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        let router = axum::Router::new()
            .route(&self.route, post(jrpc_router))
            .layer(service);

        let future = axum::Server::try_bind(&listen_address)?.serve(router.into_make_service());

        Ok(async move { future.await.unwrap() })
    }
}

async fn jrpc_router(
    Extension(ctx): Extension<Arc<JrpcState>>,
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
            let request: GetContractStateRequest = req.parse_params()?;
            match ctx.get_contract_state(&request.address) {
                Ok(state) => JsonRpcResponse::success(answer_id, state),
                Err(e) => make_error(answer_id, e, counters),
            }
        }
        "sendMessage" => {
            let request: SendMessageRequest = req.parse_params()?;
            match ctx.send_message(request.message).await {
                Ok(_) => JsonRpcResponse::success(answer_id, ()),
                Err(e) => make_error(answer_id, e, counters),
            }
        }
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

type QueryResult<T> = Result<T, QueryError>;

#[derive(thiserror::Error, Clone, Debug)]
enum QueryError {
    #[error("Connection error")]
    ConnectionError,
    #[error("Failed to serialize message")]
    FailedToSerialize,
    #[error("Invalid account state")]
    InvalidAccountState,
    #[error("External message expected")]
    ExternalMessageExpected,
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
