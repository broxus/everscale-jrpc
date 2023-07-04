use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use axum::body::{Bytes, Full, HttpBody};
use axum::extract::FromRequest;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{body, BoxError, Router};
use serde::Serialize;
use ton_block::{Deserializable, MsgAddressInt, Serializable};

use everscale_jrpc_models::*;

use everscale_proto::prost::Message;
use everscale_proto::rpc;

use crate::storage::ShardAccountFromCache;
use crate::{Counters, RpcState};

pub struct RpcServer {
    state: Arc<RpcState>,
    key_block_response: Arc<ArcSwapOption<rpc::response::GetLatestKeyBlock>>,
    config_response: Arc<ArcSwapOption<rpc::response::GetBlockchainConfig>>,
}

impl RpcServer {
    pub fn new(state: Arc<RpcState>) -> Result<Arc<Self>> {
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
                block: bytes::Bytes::from(block.write_to_bytes()?),
            };

            let config_response = rpc::response::GetBlockchainConfig {
                global_id: block.global_id,
                config: bytes::Bytes::from(config.write_to_bytes()?),
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
            key_block_response,
            config_response,
        }))
    }

    pub fn serve(self: Arc<Self>) -> Result<impl Future<Output = ()> + Send + 'static> {
        use tower::ServiceBuilder;
        use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
        use tower_http::timeout::TimeoutLayer;

        let listen_address = self.state.config.listen_address;

        // Prepare middleware
        let service = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(
                CorsLayer::new()
                    .allow_headers(AllowHeaders::list([
                        axum::http::header::AUTHORIZATION,
                        axum::http::header::CONTENT_TYPE,
                    ]))
                    .allow_origin(AllowOrigin::any())
                    .allow_methods(AllowMethods::list([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::OPTIONS,
                    ])),
            )
            .layer(TimeoutLayer::new(Duration::from_secs(10)));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        let app = Router::new()
            .route("/", get(health_check))
            .route("/rpc", post(rpc_router))
            .layer(service)
            .with_state(self);

        // Start server
        let future = axum::Server::try_bind(&listen_address)?
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(60).into())
            .serve(app.into_make_service());

        Ok(async move { future.await.unwrap() })
    }
}

const MAX_REQUEST_SIZE: usize = 2 << 17; //256kb

async fn health_check() -> impl axum::response::IntoResponse {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis()
        .to_string()
}

async fn rpc_router(
    State(ctx): State<Arc<RpcServer>>,
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

        fn fill(self, res: QueryResult) -> axum::response::Response {
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

    let mut req = Request::new(req, ctx.state.counters());

    match req.method() {
        Some(call) => match call {
            rpc::request::Call::GetContractState(_) => unimplemented!(),
            rpc::request::Call::GetTransaction(p) => req.fill(ctx.get_transaction(p)),
            rpc::request::Call::GetDstTransaction(p) => req.fill(ctx.get_dst_transaction(p)),
            rpc::request::Call::GetTransactionsList(p) => req.fill(ctx.get_transactions_list(p)),
            rpc::request::Call::GetAccountsByCodeHash(_) => {
                unimplemented!()
            }
            rpc::request::Call::GetStatus(_) => req.fill(ctx.get_status()),
            rpc::request::Call::GetLatestKeyBlock(_) => req.fill(ctx.get_latest_key_block()),
            rpc::request::Call::GetBlockchainConfig(_) => req.fill(ctx.get_blockchain_config()),
            rpc::request::Call::SendMessage(p) => req.fill(ctx.send_message(p)),
        },
        None => req.not_found(),
    }
}

// === impl RpcServer ===

impl RpcServer {
    fn get_status(&self) -> QueryResult {
        Ok(rpc::response::Result::GetStatus(rpc::response::GetStatus {
            ready: self.state.is_ready(),
        }))
    }

    fn get_latest_key_block(&self) -> QueryResult {
        // TODO: generate stub key block from zerostate
        match self.key_block_response.load_full() {
            Some(key_block) => Ok(rpc::response::Result::GetLatestKeyBlock(
                key_block.as_ref().clone(),
            )),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_blockchain_config(&self) -> QueryResult {
        match self.config_response.load_full() {
            Some(config) => Ok(rpc::response::Result::GetBlockchainConfig(
                config.as_ref().clone(),
            )),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_transaction(&self, req: rpc::request::GetTransaction) -> QueryResult {
        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let key = match storage.transactions_by_hash.get(req.id.as_ref()) {
            Ok(Some(key)) => key,
            Ok(None) => {
                return Ok(rpc::response::Result::GetRawTransaction(
                    rpc::response::GetRawTransaction::default(),
                ))
            }
            Err(e) => {
                tracing::error!("failed to resolve transaction by hash: {e:?}");
                return Err(QueryError::StorageError);
            }
        };

        match storage.transactions.get(key) {
            Ok(res) => Ok(rpc::response::Result::GetRawTransaction(
                rpc::response::GetRawTransaction {
                    transaction: res.map(|slice| bytes::Bytes::from(slice.to_vec())),
                },
            )),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
    }

    fn get_dst_transaction(&self, req: rpc::request::GetDstTransaction) -> QueryResult {
        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let key = match storage
            .transactions_by_in_msg
            .get(req.message_hash.as_ref())
        {
            Ok(Some(key)) => key,
            Ok(None) => {
                return Ok(rpc::response::Result::GetRawTransaction(
                    rpc::response::GetRawTransaction::default(),
                ))
            }
            Err(e) => {
                tracing::error!("failed to resolve transaction by incoming message hash: {e:?}");
                return Err(QueryError::StorageError);
            }
        };

        match storage.transactions.get(key) {
            Ok(res) => Ok(rpc::response::Result::GetRawTransaction(
                rpc::response::GetRawTransaction {
                    transaction: res.map(|slice| bytes::Bytes::from(slice.to_vec())),
                },
            )),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
    }

    fn get_transactions_list(&self, req: rpc::request::GetTransactionsList) -> QueryResult {
        use crate::storage::tables;

        const MAX_LIMIT: u32 = 100;

        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let limit = match req.limit {
            0 => {
                return Ok(rpc::response::Result::GetTransactionsList(
                    rpc::response::GetTransactionsList::default(),
                ))
            }
            l if l > MAX_LIMIT => return Err(QueryError::TooBigRange),
            l => l,
        };

        let Some(snapshot) = storage.load_snapshot() else {
            return Err(QueryError::NotReady);
        };

        let mut key = [0u8; { crate::storage::tables::Transactions::KEY_LEN }];
        let account = MsgAddressInt::construct_from_bytes(req.account.as_ref())
            .map_err(|_| QueryError::InvalidParams)?;
        extract_address(&account, &mut key).map_err(|_| QueryError::InvalidParams)?;
        key[33..].copy_from_slice(&req.last_transaction_lt.unwrap_or(u64::MAX).to_be_bytes());

        let mut lower_bound = Vec::with_capacity(tables::Transactions::KEY_LEN);
        lower_bound.extend_from_slice(&key[..33]);
        lower_bound.extend_from_slice(&[0; 8]);

        let mut readopts = storage.transactions.new_read_config();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_lower_bound(lower_bound);

        let transactions_cf = storage.transactions.cf();
        let mut iter = storage
            .inner
            .raw()
            .raw_iterator_cf_opt(&transactions_cf, readopts);
        iter.seek_for_prev(key);

        let mut result = Vec::with_capacity(std::cmp::min(8, limit) as usize);

        for _ in 0..limit {
            match iter.value() {
                Some(value) => {
                    result.push(bytes::Bytes::copy_from_slice(value));
                    iter.prev();
                }
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::error!("transactions iterator failed: {e:?}");
                        return Err(QueryError::StorageError);
                    }
                },
            }
        }

        Ok(rpc::response::Result::GetTransactionsList(
            rpc::response::GetTransactionsList {
                transactions: result,
            },
        ))
    }

    fn send_message(&self, req: rpc::request::SendMessage) -> QueryResult {
        let Some(engine) = self.state.engine.load().upgrade() else {
            return Err(QueryError::NotReady);
        };

        let message = ton_block::Message::construct_from_bytes(req.message.as_ref())
            .map_err(|_| QueryError::InvalidParams)?;

        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => header.dst.workchain_id(),
            _ => return Err(QueryError::InvalidMessage),
        };

        let data = message
            .serialize()
            .and_then(|cell| ton_types::serialize_toc(&cell))
            .map_err(|_| QueryError::FailedToSerialize)?;

        engine
            .broadcast_external_message(to, &data)
            .map_err(|_| QueryError::ConnectionError)?;

        Ok(rpc::response::Result::SendMessage(
            rpc::response::SendMessage {},
        ))
    }
}

fn extract_address(address: &MsgAddressInt, target: &mut [u8]) -> Result<()> {
    if let MsgAddressInt::AddrStd(address) = address {
        let account = address.address.get_bytestring_on_stack(0);
        let account = account.as_ref();

        if target.len() >= 33 && account.len() == 32 {
            target[0] = address.workchain_id as u8;
            target[1..33].copy_from_slice(account);
            return Ok(());
        }
    }

    anyhow::bail!("Invalid address")
}

macro_rules! define_query_error {
    ($ident:ident, { $($variant:ident => ($code:literal, $name:literal)),*$(,)? }) => {
        #[derive(Copy, Clone)]
        enum $ident {
            $($variant),*
        }

        impl $ident {
            fn info(&self) -> (i32, &'static str) {
                match self {
                    $(Self::$variant => ($code, $name)),*
                }
            }
        }
    };
}

define_query_error!(QueryError, {
    MethodNotFound => (-32601, "Method not found"),
    InvalidParams => (-32602, "Invalid params"),

    NotReady => (-32001, "Not ready"),
    NotSupported => (-32002, "Not supported"),
    ConnectionError => (-32003, "Connection error"),
    StorageError => (-32004, "Storage error"),
    FailedToSerialize => (-32005, "Failed to serialize"),
    InvalidAccountState => (-32006, "Invalid account state"),
    InvalidMessage => (-32007, "Invalid message"),
    TooBigRange => (-32008, "Too big range"),
});

impl QueryError {
    fn with_id(self, id: i64) -> RpcError<'static> {
        let (code, message) = self.info();
        RpcError {
            id,
            code,
            message: Cow::Borrowed(message),
        }
    }
}

type QueryResult = Result<rpc::response::Result, QueryError>;

struct RpcError<'a> {
    id: i64,
    code: i32,
    message: Cow<'a, str>,
}

impl IntoResponse for RpcError<'_> {
    fn into_response(self) -> axum::response::Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::to_string(&self).unwrap(), // TODO: unwrap
        )
            .into_response()
    }
}

impl serde::Serialize for RpcError<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a> {
            id: i64,
            error: ErrorHelper<'a>,
        }

        #[derive(Serialize)]
        struct ErrorHelper<'a> {
            code: i32,
            message: &'a str,
            data: (),
        }

        Helper {
            id: self.id,
            error: ErrorHelper {
                code: self.code,
                message: self.message.as_ref(),
                data: (),
            },
        }
        .serialize(serializer)
    }
}

struct Protobuf<T>(pub T);

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
