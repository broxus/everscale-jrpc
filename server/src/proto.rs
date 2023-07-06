use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use axum::body;
use axum::body::{Full, HttpBody};
use axum::extract::{FromRequest, State};
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::IntoResponse;
use axum::BoxError;
use bytes::Bytes;
use everscale_proto::prost::Message;
use everscale_proto::rpc;
use ton_block::Serializable;

use crate::server::Server;
use crate::utils::{QueryError, QueryResult};
use crate::{Counters, ServerState};

pub struct ProtoServer {
    state: Arc<ServerState>,
    capabilities_response: rpc::response::GetCapabilities,
    key_block_response: Arc<ArcSwapOption<rpc::response::GetLatestKeyBlock>>,
    config_response: Arc<ArcSwapOption<rpc::response::GetBlockchainConfig>>,
}

impl ProtoServer {
    pub fn new(state: Arc<ServerState>) -> Result<Arc<Self>> {
        // Prepare capabilities response as it doesn't change anymore
        let capabilities_response = {
            let mut capabilities = vec![
                "getCapabilities",
                "getLatestKeyBlock",
                "getBlockchainConfig",
                "getStatus",
                "getTimings",
                "getContractState",
                "sendMessage",
            ];

            if state.config.api_config.is_full() {
                capabilities.extend_from_slice(&[
                    "getTransactionsList",
                    "getTransaction",
                    "getDstTransaction",
                    "getAccountsByCodeHash",
                ]);
            }

            rpc::response::GetCapabilities {
                capabilities: capabilities.into_iter().map(|s| s.to_string()).collect(),
            }
        };

        // Prepare key block response listener
        fn serialize_block(
            block: &ton_block::Block,
        ) -> Result<(
            Arc<rpc::response::GetLatestKeyBlock>,
            Arc<rpc::response::GetBlockchainConfig>,
        )> {
            let extra = block.read_extra()?;
            let custom = extra
                .read_custom()?
                .context("No custom found for key block")?;
            let config = custom.config().context("No config found for key block")?;

            let key_block_response = rpc::response::GetLatestKeyBlock {
                block: Bytes::from(block.write_to_bytes()?),
            };

            let config_response = rpc::response::GetBlockchainConfig {
                global_id: block.global_id,
                config: Bytes::from(config.write_to_bytes()?),
            };

            Ok((Arc::new(key_block_response), Arc::new(config_response)))
        }

        let mut key_block_rx = state.runtime_storage.subscribe_to_key_blocks();
        let (key_block_response, config_response) = match &*key_block_rx.borrow_and_update() {
            Some(block) => {
                let (key_block, config) = serialize_block(block)?;
                (
                    Arc::new(ArcSwapOption::new(Some(key_block))),
                    Arc::new(ArcSwapOption::new(Some(config))),
                )
            }
            None => Default::default(),
        };

        tokio::spawn({
            let key_block_response = Arc::downgrade(&key_block_response);
            let config_response = Arc::downgrade(&config_response);
            async move {
                while key_block_rx.changed().await.is_ok() {
                    let (Some(key_block_response), Some(config_response)) = (
                        key_block_response.upgrade(),
                        config_response.upgrade()
                    ) else {
                        return;
                    };

                    let data = key_block_rx
                        .borrow_and_update()
                        .as_ref()
                        .map(serialize_block);

                    match data {
                        Some(Ok((key_block, config))) => {
                            key_block_response.store(Some(key_block));
                            config_response.store(Some(config));
                        }
                        Some(Err(e)) => tracing::error!("failed to update key block: {e:?}"),
                        None => continue,
                    }
                }
            }
        });

        // Done
        Ok(Arc::new(Self {
            state,
            capabilities_response,
            key_block_response,
            config_response,
        }))
    }
}

pub async fn proto_router(
    State(ctx): State<Arc<Server>>,
    Protobuf(req): Protobuf<rpc::Request>,
) -> axum::response::Response {
    struct Request<'a> {
        req: rpc::Request,
        counters: &'a Counters,
    }

    impl<'a> Request<'a> {
        fn new(req: rpc::Request, counters: &'a Counters) -> Self {
            counters.increase_total();
            Self { req, counters }
        }

        fn method(&mut self) -> Option<rpc::request::Call> {
            self.req.call.take()
        }

        fn fill(self, res: QueryResult<rpc::response::Result>) -> axum::response::Response {
            match &res {
                Ok(result) => {
                    let response = rpc::Response {
                        id: self.req.id,
                        result: Some(result.clone()),
                    };
                    (StatusCode::OK, Protobuf(response)).into_response()
                }
                Err(e) => {
                    self.counters.increase_errors();
                    (*e).with_id(self.req.id).into_response()
                }
            }
        }

        fn not_found(self) -> axum::response::Response {
            self.counters.increase_not_found();
            QueryError::MethodNotFound
                .with_id(self.req.id)
                .into_response()
        }
    }

    let mut req = Request::new(req, ctx.state().counters());

    match req.method() {
        Some(call) => match call {
            rpc::request::Call::GetStatus(_) => req.fill(ctx.proto().get_status()),
            rpc::request::Call::GetCapabilities(_) => req.fill(ctx.proto().get_capabilities()),
            rpc::request::Call::GetLatestKeyBlock(_) => {
                req.fill(ctx.proto().get_latest_key_block())
            }
            rpc::request::Call::GetBlockchainConfig(_) => {
                req.fill(ctx.proto().get_blockchain_config())
            }
            _ => unimplemented!(),
            /*rpc::request::Call::SendMessage(p) => req.fill(ctx.send_message(p)),
            rpc::request::Call::GetTransaction(p) => req.fill(ctx.get_transaction(p)),
            rpc::request::Call::GetDstTransaction(p) => req.fill(ctx.get_dst_transaction(p)),
            rpc::request::Call::GetTransactionsList(p) => req.fill(ctx.get_transactions_list(p)),
            rpc::request::Call::GetContractState(_) => unimplemented!(),
            rpc::request::Call::GetAccountsByCodeHash(p) => {
                req.fill(ctx.get_accounts_by_code_hash(p))
            }*/
        },
        None => req.not_found(),
    }
}

// === impl ProtoServer ===

impl ProtoServer {
    fn get_capabilities(&self) -> QueryResult<rpc::response::Result> {
        Ok(rpc::response::Result::GetCapabilities(
            self.capabilities_response.clone(),
        ))
    }

    fn get_status(&self) -> QueryResult<rpc::response::Result> {
        Ok(rpc::response::Result::GetStatus(rpc::response::GetStatus {
            ready: self.state.is_ready(),
        }))
    }

    fn get_latest_key_block(&self) -> QueryResult<rpc::response::Result> {
        // TODO: generate stub key block from zerostate
        match self.key_block_response.load_full() {
            Some(key_block) => Ok(rpc::response::Result::GetLatestKeyBlock(
                key_block.as_ref().clone(),
            )),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_blockchain_config(&self) -> QueryResult<rpc::response::Result> {
        match self.config_response.load_full() {
            Some(config) => Ok(rpc::response::Result::GetBlockchainConfig(
                config.as_ref().clone(),
            )),
            None => Err(QueryError::NotReady),
        }
    }
}

pub struct Protobuf<T>(pub T);

#[axum::async_trait]
impl<S, B, T> FromRequest<S, B> for Protobuf<T>
where
    T: Message + Default,
    S: Send + Sync,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = StatusCode;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = match Bytes::from_request(req, state).await {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!("Failed to read body: {}", err);
                return Err(StatusCode::BAD_REQUEST);
            }
        };
        let message = match T::decode(bytes) {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!("Failed to decode protobuf request: {}", err);
                return Err(StatusCode::BAD_REQUEST);
            }
        };
        Ok(Protobuf(message))
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: Message,
{
    fn into_response(self) -> axum::response::Response {
        let buf = self.0.encode_to_vec();
        let mut res = axum::response::Response::new(body::boxed(Full::from(buf)));
        res.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        res
    }
}
