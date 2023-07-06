use std::borrow::Cow;
use std::sync::atomic::Ordering;
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
use nekoton_abi::LastTransactionId;
use serde::Serialize;
use ton_block::{Deserializable, MsgAddressInt, Serializable};

use crate::server::Server;
use crate::storage::ShardAccountFromCache;
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
                        result: Some(result.clone()),
                    };
                    (StatusCode::OK, Protobuf(response)).into_response()
                }
                Err(e) => {
                    self.counters.increase_errors();
                    (*e).without_id().into_response()
                }
            }
        }

        fn not_found(self) -> axum::response::Response {
            self.counters.increase_not_found();
            QueryError::MethodNotFound.without_id().into_response()
        }
    }

    let mut req = Request::new(req, ctx.state().counters());

    /*match req.method() {
        "getCapabilities" => req.fill(ctx.jrpc().get_capabilities()),
        "getLatestKeyBlock" => req.fill(ctx.jrpc().get_latest_key_block()),
        "getBlockchainConfig" => req.fill(ctx.jrpc().get_blockchain_config()),
        "getStatus" => req.fill(ctx.jrpc().get_status()),
        "getTimings" => req.fill(ctx.jrpc().get_timings()),
        "getContractState" => req.fill_with_params(|req| ctx.jrpc().get_contract_state(req)),
        "getAccountsByCodeHash" => {
            req.fill_with_params(|req| ctx.jrpc().get_accounts_by_code_hash(req))
        }
        "sendMessage" => req.fill_with_params(|req| ctx.jrpc().send_message(req)),
        "getTransactionsList" => req.fill_with_params(|req| ctx.jrpc().get_transactions_list(req)),
        "getTransaction" => req.fill_with_params(|req| ctx.jrpc().get_transaction(req)),
        "getDstTransaction" => req.fill_with_params(|req| ctx.jrpc().get_dst_transaction(req)),
        _ => req.not_found(),
    }*/

    match req.method() {
        Some(call) => match call {
            rpc::request::Call::GetCapabilities(_) => req.fill(ctx.proto().get_capabilities()),
            rpc::request::Call::GetLatestKeyBlock(_) => {
                req.fill(ctx.proto().get_latest_key_block())
            }
            rpc::request::Call::GetBlockchainConfig(_) => {
                req.fill(ctx.proto().get_blockchain_config())
            }
            rpc::request::Call::GetStatus(_) => req.fill(ctx.proto().get_status()),
            rpc::request::Call::GetTimings(_) => req.fill(ctx.proto().get_timings()),
            rpc::request::Call::GetContractState(param) => {
                req.fill(ctx.proto().get_contract_state(param))
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

    fn get_status(&self) -> QueryResult<rpc::response::Result> {
        Ok(rpc::response::Result::GetStatus(rpc::response::GetStatus {
            ready: self.state.is_ready(),
        }))
    }

    fn get_timings(&self) -> QueryResult<rpc::response::Result> {
        let Some(engine) = self.state.engine.load().upgrade() else {
            return Err(QueryError::NotReady);
        };
        let metrics = engine.metrics().as_ref();

        Ok(rpc::response::Result::GetTimings(
            rpc::response::GetTimings {
                last_mc_block_seqno: metrics.last_mc_block_seqno.load(Ordering::Acquire),
                last_shard_client_mc_block_seqno: metrics
                    .last_shard_client_mc_block_seqno
                    .load(Ordering::Acquire),
                last_mc_utime: metrics.last_mc_utime.load(Ordering::Acquire),
                mc_time_diff: metrics.mc_time_diff.load(Ordering::Acquire),
                shard_client_time_diff: metrics.shard_client_time_diff.load(Ordering::Acquire),
            },
        ))
    }

    fn get_contract_state(
        &self,
        req: rpc::request::GetContractState,
    ) -> QueryResult<rpc::response::Result> {
        let account = parse_address(req.address)?;

        let state = match self.state.runtime_storage.get_contract_state(&account) {
            Ok(ShardAccountFromCache::Found(state)) => state,
            Ok(ShardAccountFromCache::NotFound) => {
                return Ok(rpc::response::Result::GetContractState(
                    rpc::response::GetContractState::default(),
                ))
            }
            Ok(ShardAccountFromCache::NotReady) => {
                return Err(QueryError::NotReady);
            }
            Err(e) => {
                tracing::error!("failed to read shard account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };

        let guard = state.state_handle;

        let account = match ton_block::Account::construct_from_cell(state.data) {
            Ok(ton_block::Account::Account(account)) => account,
            Ok(ton_block::Account::AccountNone) => {
                return Ok(rpc::response::Result::GetContractState(
                    rpc::response::GetContractState::default(),
                ))
            }
            Err(e) => {
                tracing::error!("failed to deserialize account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };

        let account = bytes::Bytes::from(account.write_to_bytes().map_err(|e| {
            tracing::error!("failed to serialize account: {e:?}");
            QueryError::FailedToSerialize
        })?);

        // NOTE: state guard must be dropped after the serialization
        drop(guard);

        let gen_timings = rpc::response::get_contract_state::contract_state::GenTimings::Known(
            rpc::response::get_contract_state::contract_state::Known {
                gen_lt: state.last_transaction_id.lt(),
                gen_utime: state.gen_utime,
            },
        );

        let last_transaction_id = match state.last_transaction_id {
            LastTransactionId::Exact(transaction_id) => {
                rpc::response::get_contract_state::contract_state::LastTransactionId::Exact(
                    rpc::response::get_contract_state::contract_state::Exact {
                        lt: transaction_id.lt,
                        hash: bytes::Bytes::copy_from_slice(transaction_id.hash.as_slice()),
                    },
                )
            }
            LastTransactionId::Inexact { latest_lt } => {
                rpc::response::get_contract_state::contract_state::LastTransactionId::Inexact(
                    rpc::response::get_contract_state::contract_state::Inexact { latest_lt },
                )
            }
        };

        let result = rpc::response::Result::GetContractState(rpc::response::GetContractState {
            contract_state: Some(rpc::response::get_contract_state::ContractState {
                account,
                gen_timings: Some(gen_timings),
                last_transaction_id: Some(last_transaction_id),
            }),
        });

        Ok(result)
    }
}

fn parse_address(bytes: Bytes) -> QueryResult<MsgAddressInt> {
    if bytes.len() == 33 {
        let workchain_id = bytes[0] as i8;
        let address =
            ton_types::AccountId::from(<[u8; 32]>::try_from(&bytes[1..33]).map_err(|e| {
                tracing::error!("failed to deserialize account: {e:?}");
                QueryError::FailedToDeserialize
            })?);

        return MsgAddressInt::with_standart(None, workchain_id, address).map_err(|e| {
            tracing::error!("failed to construct account: {e:?}");
            QueryError::FailedToSerialize
        });
    }

    Err(QueryError::InvalidParams)
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

pub struct ProtoError<'a> {
    code: i32,
    message: Cow<'a, str>,
}

impl<'a> ProtoError<'a> {
    pub fn new(code: i32, message: Cow<'a, str>) -> Self {
        Self { code, message }
    }
}

impl IntoResponse for ProtoError<'_> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl serde::Serialize for ProtoError<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a> {
            error: ErrorHelper<'a>,
        }

        #[derive(Serialize)]
        struct ErrorHelper<'a> {
            code: i32,
            message: &'a str,
            data: (),
        }

        Helper {
            error: ErrorHelper {
                code: self.code,
                message: self.message.as_ref(),
                data: (),
            },
        }
        .serialize(serializer)
    }
}
