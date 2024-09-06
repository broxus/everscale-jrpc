use std::borrow::Cow;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use axum::extract::State;
use axum::response::IntoResponse;
use nekoton_abi::GenTimings;
use serde::Serialize;
use ton_block::{Deserializable, Serializable};

use everscale_rpc_models::jrpc;

use crate::server::Server;
use crate::storage::ShardAccountFromCache;
use crate::utils::{self, QueryError, QueryResult};
use crate::{Counters, RpcState};

pub struct JrpcServer {
    state: Arc<RpcState>,
    capabilities_response: serde_json::Value,
    key_block_response: Arc<ArcSwapOption<serde_json::Value>>,
    config_response: Arc<ArcSwapOption<serde_json::Value>>,
}

impl JrpcServer {
    pub fn new(state: Arc<RpcState>) -> Result<Arc<Self>> {
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

            serde_json::to_value(capabilities).unwrap()
        };

        // Prepare key block response listener
        fn serialize_block(
            seqno: u32,
            block: &ton_block::Block,
        ) -> Result<(Arc<serde_json::Value>, Arc<serde_json::Value>)> {
            let extra = block.read_extra()?;
            let custom = extra
                .read_custom()?
                .context("No custom found for key block")?;
            let config = custom.config().context("No config found for key block")?;

            let key_block_response = serde_json::to_value(jrpc::GetLatestKeyBlockResponse {
                block: block.clone(),
            })?;
            let config_response = serde_json::to_value(jrpc::GetBlockchainConfigResponse {
                global_id: block.global_id,
                config: config.clone(),
                seqno,
            })?;

            Ok((Arc::new(key_block_response), Arc::new(config_response)))
        }

        let mut key_block_rx = state.runtime_storage.subscribe_to_key_blocks();
        let (key_block_response, config_response) = match &*key_block_rx.borrow_and_update() {
            Some((seqno, block)) => {
                let (key_block, config) = serialize_block(*seqno, block)?;
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
                    let (Some(key_block_response), Some(config_response)) =
                        (key_block_response.upgrade(), config_response.upgrade())
                    else {
                        return;
                    };

                    let data = key_block_rx
                        .borrow_and_update()
                        .as_ref()
                        .map(|(seqno, block)| serialize_block(*seqno, block));

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

pub async fn jrpc_router(
    State(ctx): State<Arc<Server>>,
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

    let req = Request::new(req, ctx.state().jrpc_counters());
    match req.method() {
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
    }
}

// === impl JrpcServer ===

impl JrpcServer {
    fn get_capabilities(&self) -> QueryResult<&serde_json::Value> {
        Ok(&self.capabilities_response)
    }

    fn get_latest_key_block(&self) -> QueryResult<ArcJson> {
        // TODO: generate stub key block from zerostate
        match self.key_block_response.load_full() {
            Some(key_block) => Ok(ArcJson(key_block)),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_blockchain_config(&self) -> QueryResult<ArcJson> {
        match self.config_response.load_full() {
            Some(config) => Ok(ArcJson(config)),
            None => Err(QueryError::NotReady),
        }
    }

    fn get_status(&self) -> QueryResult<jrpc::GetStatusResponse> {
        Ok(jrpc::GetStatusResponse {
            ready: self.state.is_ready(),
        })
    }

    fn get_timings(&self) -> QueryResult<jrpc::GetTimingsResponse> {
        let Some(engine) = self.state.engine.load().upgrade() else {
            return Err(QueryError::NotReady);
        };
        let metrics = engine.metrics().as_ref();

        Ok(jrpc::GetTimingsResponse {
            last_mc_block_seqno: metrics.last_mc_block_seqno.load(Ordering::Acquire),
            last_shard_client_mc_block_seqno: metrics
                .last_shard_client_mc_block_seqno
                .load(Ordering::Acquire),
            last_mc_utime: metrics.last_mc_utime.load(Ordering::Acquire),
            mc_time_diff: metrics.mc_time_diff.load(Ordering::Acquire),
            shard_client_time_diff: metrics.shard_client_time_diff.load(Ordering::Acquire),
            smallest_known_lt: self
                .state
                .persistent_storage
                .as_ref()
                .map(|storage| storage.min_tx_lt.load(Ordering::Acquire)),
        })
    }

    fn get_contract_state(
        &self,
        req: &jrpc::GetContractStateRequest,
    ) -> QueryResult<serde_json::Value> {
        let state = match self.state.runtime_storage.get_contract_state(&req.address) {
            Ok(ShardAccountFromCache::Found(state))
                if Some(state.last_transaction_id.lt()) <= req.last_transaction_lt =>
            {
                return Ok(
                    serde_json::to_value(jrpc::GetContractStateResponse::Unchanged {
                        timings: GenTimings::Known {
                            gen_lt: state.last_transaction_id.lt(),
                            gen_utime: state.gen_utime,
                        },
                    })
                    .unwrap(),
                );
            }
            Ok(ShardAccountFromCache::Found(state)) => state,
            Ok(ShardAccountFromCache::NotFound(timings)) => {
                return Ok(
                    serde_json::to_value(jrpc::GetContractStateResponse::NotExists { timings })
                        .unwrap(),
                );
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

        let timings = nekoton_abi::GenTimings::Known {
            gen_lt: state.last_transaction_id.lt(),
            gen_utime: state.gen_utime,
        };

        let account = match ton_block::Account::construct_from_cell(state.data) {
            Ok(ton_block::Account::Account(account)) => account,
            Ok(ton_block::Account::AccountNone) => {
                return Ok(
                    serde_json::to_value(jrpc::GetContractStateResponse::NotExists { timings })
                        .unwrap(),
                )
            }
            Err(e) => {
                tracing::error!("failed to deserialize account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };

        let result = serde_json::to_value(jrpc::GetContractStateResponse::Exists {
            account,
            timings,
            last_transaction_id: state.last_transaction_id,
        });

        // NOTE: state guard must be dropped after the serialization
        drop(guard);

        result.map_err(|e| {
            tracing::error!("failed to serialize account: {e:?}");
            QueryError::FailedToSerialize
        })
    }

    fn get_accounts_by_code_hash(
        &self,
        req: &jrpc::GetAccountsByCodeHashRequest,
    ) -> QueryResult<Vec<String>> {
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

        let mut key = [0u8; { tables::CodeHashes::KEY_LEN }];
        key[0..32].copy_from_slice(&req.code_hash);
        if let Some(continuation) = &req.continuation {
            utils::extract_address(continuation, &mut key[32..])?;
        }

        let mut upper_bound = Vec::with_capacity(tables::CodeHashes::KEY_LEN);
        upper_bound.extend_from_slice(&key[..32]);
        upper_bound.extend_from_slice(&[0xff; 33]);

        let mut readopts = storage.code_hashes.new_read_config();
        readopts.set_snapshot(&snapshot);
        readopts.set_iterate_upper_bound(upper_bound); // NOTE: somehow make the range inclusive

        let code_hashes_cf = storage.code_hashes.cf();
        let mut iter = storage
            .inner
            .raw()
            .raw_iterator_cf_opt(&code_hashes_cf, readopts);

        iter.seek(key);
        if req.continuation.is_some() {
            iter.next();
        }

        let mut result = Vec::with_capacity(std::cmp::min(8, limit) as usize);

        for _ in 0..limit {
            let Some(value) = iter.key() else {
                match iter.status() {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::error!("code hashes iterator failed: {e:?}");
                        return Err(QueryError::StorageError);
                    }
                }
            };

            if value.len() != tables::CodeHashes::KEY_LEN {
                tracing::error!("parsing address from key failed: invalid value");
                return Err(QueryError::StorageError);
            }

            result.push(format!("{}:{}", value[32] as i8, hex::encode(&value[33..])));
            iter.next();
        }

        Ok(result)
    }

    fn send_message(&self, req: &jrpc::SendMessageRequest) -> QueryResult<()> {
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

    fn get_transactions_list(
        &self,
        req: &jrpc::GetTransactionsListRequest,
    ) -> QueryResult<Vec<String>> {
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
        utils::extract_address(&req.account, &mut key)?;
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

    fn get_transaction(
        &self,
        req: &jrpc::GetTransactionRequest,
    ) -> QueryResult<Option<RawStorageValue>> {
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
        req: &jrpc::GetDstTransactionRequest,
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
        base64::encode(self.0.as_ref()).serialize(serializer)
    }
}

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

pub struct JrpcError<'a> {
    id: i64,
    code: i32,
    message: Cow<'a, str>,
}

impl<'a> JrpcError<'a> {
    pub fn new(id: i64, code: i32, message: Cow<'a, str>) -> Self {
        Self { id, code, message }
    }
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
