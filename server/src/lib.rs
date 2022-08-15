use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::routing::post;
use axum::Extension;
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use axum_jrpc::JsonRpcResponse;
use everscale_jrpc_models::*;

pub use self::state::JrpcState;

mod state;

pub struct JrpcBuilder {
    route: String,
    allow_partial_state: bool,
}

impl JrpcBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_route(mut self, route: &str) -> Self {
        self.route = route.to_owned();
        self
    }

    /// Allow running JRPC with partially initialized state
    pub fn allow_partial_state(mut self) -> Self {
        self.allow_partial_state = true;
        self
    }

    pub fn build(self) -> (Arc<JrpcState>, Server) {
        let state = Arc::new(JrpcState::default());
        let server = Server {
            route: self.route,
            allow_partial_state: self.allow_partial_state,
            state: state.clone(),
        };
        (state, server)
    }
}

impl Default for JrpcBuilder {
    fn default() -> Self {
        Self {
            route: "/rpc".to_owned(),
            allow_partial_state: false,
        }
    }
}

pub struct Server {
    route: String,
    allow_partial_state: bool,
    state: Arc<JrpcState>,
}

impl Server {
    pub async fn serve(self, listen_address: SocketAddr) -> Result<()> {
        if !self.state.is_initialized() {
            if self.allow_partial_state {
                log::warn!("Starting JRPC server with partially initialized state");
            } else {
                anyhow::bail!("Partially initialized state");
            }
        }

        let service = tower::ServiceBuilder::new().layer(Extension(self.state));
        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        let router = axum::Router::new()
            .route(&self.route, post(jrpc_router))
            .layer(service);

        axum::Server::bind(&listen_address)
            .serve(router.into_make_service())
            .await?;

        Ok(())
    }
}

async fn jrpc_router(
    Extension(ctx): Extension<Arc<JrpcState>>,
    req: axum_jrpc::JsonRpcExtractor,
) -> axum_jrpc::JrpcResult {
    let answer_id = req.get_answer_id();
    let method = req.method();
    let answer = match method {
        "getLatestKeyBlock" => match ctx.get_last_key_block() {
            Ok(b) => JsonRpcResponse::success(answer_id, b.as_ref()),
            Err(e) => JsonRpcResponse::error(answer_id, e.into()),
        },
        "getContractState" => {
            let request: GetContractStateRequest = req.parse_params()?;
            match ctx.get_contract_state(&request.address) {
                Ok(state) => JsonRpcResponse::success(answer_id, state),
                Err(e) => JsonRpcResponse::error(answer_id, e.into()),
            }
        }
        "sendMessage" => {
            let request: SendMessageRequest = req.parse_params()?;
            match ctx.send_message(request.message).await {
                Ok(_) => JsonRpcResponse::success(answer_id, ()),
                Err(e) => JsonRpcResponse::error(answer_id, e.into()),
            }
        }
        m => req.method_not_found(m),
    };

    Ok(answer)
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
    #[error("Invalid block")]
    InvalidBlock,
    #[error("Unknown")]
    Unknown,
    #[error("Not ready")]
    NotReady,
    #[error("External message expected")]
    ExternalMessageExpected,
}

impl QueryError {
    pub fn code(&self) -> i64 {
        match self {
            QueryError::ConnectionError => -32001,
            QueryError::FailedToSerialize => -32002,
            QueryError::InvalidAccountState => -32004,
            QueryError::InvalidBlock => -32006,
            QueryError::NotReady => -32007,
            QueryError::Unknown => -32603,
            QueryError::ExternalMessageExpected => -32005,
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
