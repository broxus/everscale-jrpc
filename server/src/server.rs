use std::borrow::Cow;
use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use axum::extract::{DefaultBodyLimit, State};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde::Serialize;
use ton_block::{Deserializable, Serializable};

use everscale_jrpc_models::*;

use crate::storage::ShardAccountFromCache;
use crate::{Counters, JrpcState};

pub struct JrpcServer {
    state: Arc<JrpcState>,
    capabilities_response: serde_json::Value,
    key_block_response: Arc<ArcSwapOption<serde_json::Value>>,
}

impl JrpcServer {
    pub fn new(state: Arc<JrpcState>) -> Result<Arc<Self>> {
        // Prepare capabilities response as it doesn't change anymore
        let capabilities_response = {
            let mut capabilities = vec![
                "getCapabilities",
                "getLatestKeyBlock",
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

            serde_json::to_value(capabilities).unwrap()
        };

        // Prepare key block response listener
        let key_block_response = {
            fn serialize_block(block: &ton_block::Block) -> Result<Arc<serde_json::Value>> {
                serde_json::to_value(GetLatestKeyBlockResponse {
                    block: block.clone(),
                })
                .map(Arc::new)
                .map_err(From::from)
            }

            let mut key_block_rx = state.runtime_storage.subscribe_to_key_blocks();
            let key_block = Arc::new(ArcSwapOption::new(
                key_block_rx
                    .borrow_and_update()
                    .as_ref()
                    .map(serialize_block)
                    .transpose()?,
            ));

            tokio::spawn({
                let key_block = Arc::downgrade(&key_block);
                async move {
                    while key_block_rx.changed().await.is_ok() {
                        let Some(key_block) = key_block.upgrade() else {
                            return;
                        };

                        let data = key_block_rx
                            .borrow_and_update()
                            .as_ref()
                            .map(serialize_block);

                        match data {
                            Some(Ok(data)) => key_block.store(Some(data)),
                            Some(Err(e)) => tracing::error!("failed to update key block: {e:?}"),
                            None => continue,
                        }
                    }
                }
            });

            key_block
        };

        // Done
        Ok(Arc::new(Self {
            state,
            capabilities_response,
            key_block_response,
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
                        axum::http::Method::PUT,
                    ])),
            )
            .layer(TimeoutLayer::new(Duration::from_secs(10)));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        // Prepare routes
        let router = axum::Router::new()
            .route("/", get(health_check))
            .route("/", post(jrpc_router))
            .route("/rpc", post(jrpc_router))
            .layer(service)
            .with_state(self);

        // Start server
        let future = axum::Server::try_bind(&listen_address)?
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(60).into())
            .serve(router.into_make_service());

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

async fn jrpc_router(
    State(ctx): State<Arc<JrpcServer>>,
    req: axum_jrpc::JsonRpcExtractor,
) -> axum::response::Response {
    struct Request<'a> {
        req: axum_jrpc::JsonRpcExtractor,
        counters: &'a Counters,
    }

    impl<'a> Request<'a> {
        fn new(req: axum_jrpc::JsonRpcExtractor, counters: &'a Counters) -> Self {
            counters.increase_total();
            Self { req, counters }
        }

        fn method(&self) -> &str {
            &self.req.method
        }

        fn fill_with_params<F, T, R>(mut self, f: F) -> axum::response::Response
        where
            for<'t> F: FnOnce(&'t T) -> QueryResult<R>,
            for<'t> T: serde::Deserialize<'t>,
            R: serde::Serialize,
        {
            match serde_json::from_value::<T>(self.req.parsed.take()) {
                Ok(ref req) => self.fill(f(req)),
                Err(_) => {
                    self.counters.increase_errors();
                    QueryError::InvalidParams
                        .with_id(self.req.id)
                        .into_response()
                }
            }
        }

        fn fill<T>(self, res: QueryResult<T>) -> axum::response::Response
        where
            T: serde::Serialize,
        {
            match &res {
                Ok(result) => JrpcSuccess {
                    id: self.req.id,
                    result,
                }
                .into_response(),
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

    let req = Request::new(req, ctx.state.counters());
    match req.method() {
        "getCapabilities" => req.fill(ctx.get_capabilities()),
        "getLatestKeyBlock" => req.fill(ctx.get_last_key_block()),
        "getStatus" => req.fill(ctx.get_status()),
        "getTimings" => req.fill(ctx.get_timings()),
        "getContractState" => req.fill_with_params(|req| ctx.get_contract_state(req)),
        "sendMessage" => req.fill_with_params(|req| ctx.send_message(req)),
        "getTransactionsList" => req.fill_with_params(|req| ctx.get_transactions_list(req)),
        "getTransaction" => req.fill_with_params(|req| ctx.get_transaction(req)),
        "getDstTransaction" => req.fill_with_params(|req| ctx.get_dst_transaction(req)),
        _ => req.not_found(),
    }
}

// === impl JrpcServer ===

impl JrpcServer {
    fn get_capabilities(&self) -> QueryResult<&serde_json::Value> {
        Ok(&self.capabilities_response)
    }

    fn get_last_key_block(&self) -> QueryResult<ArcJson> {
        // TODO: generate stub key block from zerostate
        match self.key_block_response.load_full() {
            Some(key_block) => Ok(ArcJson(key_block)),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_status(&self) -> QueryResult<GetStatusResponse> {
        Ok(GetStatusResponse {
            ready: self.state.is_ready(),
        })
    }

    fn get_timings(&self) -> QueryResult<GetTimingsResponse> {
        let Some(engine) = self.state.engine.load().upgrade() else {
            return Err(QueryError::NotReady);
        };
        let metrics = engine.metrics().as_ref();

        Ok(GetTimingsResponse {
            last_mc_block_seqno: metrics.last_mc_block_seqno.load(Ordering::Acquire),
            last_shard_client_mc_block_seqno: metrics
                .last_shard_client_mc_block_seqno
                .load(Ordering::Acquire),
            last_mc_utime: metrics.last_mc_utime.load(Ordering::Acquire),
            mc_time_diff: metrics.mc_time_diff.load(Ordering::Acquire),
            shard_client_time_diff: metrics.shard_client_time_diff.load(Ordering::Acquire),
        })
    }

    fn get_contract_state(&self, req: &GetContractStateRequest) -> QueryResult<serde_json::Value> {
        let state = match self.state.runtime_storage.get_contract_state(&req.address) {
            Ok(ShardAccountFromCache::Found(state)) => state,
            Ok(ShardAccountFromCache::NotFound) => {
                return Ok(serde_json::to_value(GetContractStateResponse::NotExists).unwrap());
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
                return Ok(serde_json::to_value(GetContractStateResponse::NotExists).unwrap())
            }
            Err(e) => {
                tracing::error!("failed to deserialize account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };

        let result = serde_json::to_value(GetContractStateResponse::Exists {
            account,
            timings: nekoton_abi::GenTimings::Known {
                gen_lt: state.last_transaction_id.lt(),
                gen_utime: state.gen_utime,
            },
            last_transaction_id: state.last_transaction_id,
        });

        // NOTE: state guard must be dropped after the serialization
        drop(guard);

        result.map_err(|e| {
            tracing::error!("failed to serialize account: {e:?}");
            QueryError::FailedToSerialize
        })
    }

    fn send_message(&self, req: &SendMessageRequest) -> QueryResult<()> {
        let Some(engine) = self.state.engine.load().upgrade() else {
            return Err(QueryError::NotReady);
        };

        let to = match req.message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => header.dst.workchain_id(),
            _ => return Err(QueryError::InvalidMessage),
        };

        let data = req
            .message
            .serialize()
            .and_then(|cell| ton_types::serialize_toc(&cell))
            .map_err(|_| QueryError::FailedToSerialize)?;

        engine
            .broadcast_external_message(to, &data)
            .map_err(|_| QueryError::ConnectionError)
    }

    fn get_transactions_list(&self, req: &GetTransactionsListRequest) -> QueryResult<Vec<String>> {
        use crate::storage::tables;

        const MAX_LIMIT: u8 = 100;

        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let limit = match req.limit {
            0 => return Ok(Vec::new()),
            l if l > MAX_LIMIT => return Err(QueryError::TooBigRange),
            l => l,
        };

        let Some(snapshot) = storage.load_snapshot() else {
            return Err(QueryError::NotReady);
        };

        let mut key = [0u8; { crate::storage::tables::Transactions::KEY_LEN }];
        extract_address(&req.account, &mut key)?;
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
                    result.push(base64::encode(value));
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

        Ok(result)
    }

    fn get_transaction(&self, req: &GetTransactionRequest) -> QueryResult<Option<RawStorageValue>> {
        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let key = match storage.transactions_by_hash.get(req.id.as_slice()) {
            Ok(Some(key)) => key,
            Ok(None) => return Ok(None),
            Err(e) => {
                tracing::error!("failed to resolve transaction by hash: {e:?}");
                return Err(QueryError::StorageError);
            }
        };

        match storage.transactions.get(key) {
            Ok(res) => Ok(res.map(RawStorageValue)),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
    }

    fn get_dst_transaction(
        &self,
        req: &GetDstTransactionRequest,
    ) -> QueryResult<Option<RawStorageValue>> {
        let Some(storage) = &self.state.persistent_storage else {
            return Err(QueryError::NotSupported);
        };

        let key = match storage
            .transactions_by_in_msg
            .get(req.message_hash.as_slice())
        {
            Ok(Some(key)) => key,
            Ok(None) => return Ok(None),
            Err(e) => {
                tracing::error!("failed to resolve transaction by incoming message hash: {e:?}");
                return Err(QueryError::StorageError);
            }
        };

        match storage.transactions.get(key) {
            Ok(res) => Ok(res.map(RawStorageValue)),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
    }
}

fn extract_address(address: &ton_block::MsgAddressInt, target: &mut [u8]) -> QueryResult<()> {
    if let ton_block::MsgAddressInt::AddrStd(address) = address {
        let account = address.address.get_bytestring_on_stack(0);
        let account = account.as_ref();

        if target.len() >= 33 && account.len() == 32 {
            target[0] = address.workchain_id as u8;
            target[1..33].copy_from_slice(account);
            return Ok(());
        }
    }

    Err(QueryError::InvalidParams)
}

struct ArcJson(Arc<serde_json::Value>);

impl serde::Serialize for ArcJson {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

struct RawStorageValue<'a>(weedb::rocksdb::DBPinnableSlice<'a>);

impl serde::Serialize for RawStorageValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
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
    fn with_id(self, id: i64) -> JrpcError<'static> {
        let (code, message) = self.info();
        JrpcError {
            id,
            code,
            message: Cow::Borrowed(message),
        }
    }
}

type QueryResult<T> = Result<T, QueryError>;

struct JrpcSuccess<T> {
    id: i64,
    result: T,
}

impl<T: serde::Serialize> IntoResponse for JrpcSuccess<T> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl<T: serde::Serialize> serde::Serialize for JrpcSuccess<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a, T> {
            jsonrpc: &'static str,
            id: i64,
            result: &'a T,
        }

        Helper {
            jsonrpc: JSONRPC,
            id: self.id,
            result: &self.result,
        }
        .serialize(serializer)
    }
}

struct JrpcError<'a> {
    id: i64,
    code: i32,
    message: Cow<'a, str>,
}

impl IntoResponse for JrpcError<'_> {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl serde::Serialize for JrpcError<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Helper<'a> {
            jsonrpc: &'static str,
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
            jsonrpc: JSONRPC,
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

const JSONRPC: &str = "2.0";
