use std::borrow::Cow;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::server::Server;
use crate::storage::ShardAccountFromCache;
use crate::utils::{hash_from_bytes, QueryError, QueryResult};
use crate::{Counters, RpcState};
use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use bytes::Bytes;
use everscale_rpc_models::proto::Protobuf;
use nekoton_abi::LastTransactionId;
use nekoton_proto::protos::rpc;
use nekoton_proto::protos::rpc::response::GetLibraryCell;
use serde::Serialize;
use ton_block::{Deserializable, Serializable};
use ton_types::UInt256;

pub struct ProtoServer {
    state: Arc<RpcState>,
    capabilities_response: rpc::response::GetCapabilities,
    key_block_response: Arc<ArcSwapOption<rpc::response::GetLatestKeyBlock>>,
    config_response: Arc<ArcSwapOption<rpc::response::GetBlockchainConfig>>,
    library_cache: moka::sync::Cache<UInt256, Bytes, ahash::RandomState>,
}

impl ProtoServer {
    pub fn new(state: Arc<RpcState>) -> Result<Arc<Self>> {
        // Prepare capabilities response as it doesn't change anymore
        let capabilities_response = {
            let mut capabilities = vec![
                "getCapabilities",
                "getLatestKeyBlock",
                "getBlockchainConfig",
                "getStatus",
                "getTimings",
                "getLibraryCell",
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
            seqno: u32,
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
                seqno,
            };

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
            library_cache: moka::sync::Cache::builder()
                .max_capacity(100)
                .build_with_hasher(Default::default()),
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
                    (StatusCode::UNPROCESSABLE_ENTITY, (*e).without_id()).into_response()
                }
            }
        }

        fn not_found(self) -> axum::response::Response {
            self.counters.increase_not_found();
            QueryError::MethodNotFound.without_id().into_response()
        }
    }

    let mut req = Request::new(req, ctx.state().proto_counters());
    match req.method() {
        Some(call) => match call {
            rpc::request::Call::GetCapabilities(()) => req.fill(ctx.proto().get_capabilities()),
            rpc::request::Call::GetLatestKeyBlock(()) => {
                req.fill(ctx.proto().get_latest_key_block())
            }
            rpc::request::Call::GetBlockchainConfig(()) => {
                req.fill(ctx.proto().get_blockchain_config())
            }
            rpc::request::Call::GetStatus(()) => req.fill(ctx.proto().get_status()),
            rpc::request::Call::GetTimings(()) => req.fill(ctx.proto().get_timings()),
            rpc::request::Call::GetLibraryCell(param) => {
                req.fill(ctx.proto().get_library_cell(param))
            }
            rpc::request::Call::GetContractState(param) => {
                req.fill(ctx.proto().get_contract_state(param))
            }
            rpc::request::Call::GetAccountsByCodeHash(p) => {
                req.fill(ctx.proto().get_accounts_by_code_hash(p))
            }
            rpc::request::Call::SendMessage(p) => req.fill(ctx.proto().send_message(p)),
            rpc::request::Call::GetTransactionsList(p) => {
                req.fill(ctx.proto().get_transactions_list(p))
            }
            rpc::request::Call::GetTransaction(p) => req.fill(ctx.proto().get_transaction(p)),
            rpc::request::Call::GetDstTransaction(p) => {
                req.fill(ctx.proto().get_dst_transaction(p))
            }
            _ => todo!()
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
                smallest_known_lt: self
                    .state
                    .persistent_storage
                    .as_ref()
                    .map(|storage| storage.min_tx_lt.load(Ordering::Acquire))
                    .unwrap_or(u64::MAX),
            },
        ))
    }

    fn get_library_cell(
        &self,
        req: rpc::request::GetLibraryCell,
    ) -> QueryResult<rpc::response::Result> {
        let Some(hash) = hash_from_bytes(req.hash) else {
            tracing::error!("Invalid hash");
            return Err(QueryError::InvalidParams);
        };

        let cell_opt = match self.library_cache.get(&hash) {
            Some(cell_boc) => Some(cell_boc),
            None => {
                match self
                    .state
                    .runtime_storage
                    .get_library_cell(&hash)
                    .map_err(|_| QueryError::StorageError)?
                {
                    Some(cell) => {
                        let bytes = ton_types::serialize_toc(&cell)
                            .map_err(|_| QueryError::StorageError)?;
                        let bytes: Bytes = bytes.into();
                        self.library_cache.insert(hash, bytes.clone());
                        Some(bytes)
                    }
                    None => None,
                }
            }
        };

        Ok(rpc::response::Result::GetLibraryCell(GetLibraryCell {
            cell: cell_opt,
        }))
    }

    fn get_contract_state(
        &self,
        req: rpc::request::GetContractState,
    ) -> QueryResult<rpc::response::Result> {
        let account = nekoton_proto::utils::bytes_to_addr(&req.address).map_err(|e| {
            tracing::error!("failed to parse address: {e:?}");
            QueryError::InvalidParams
        })?;

        let state = match self.state.runtime_storage.get_contract_state(&account) {
            Ok(ShardAccountFromCache::Found(state))
                if Some(state.last_transaction_id.lt()) <= req.last_transaction_lt =>
            {
                return Ok(rpc::response::Result::GetContractState(
                    rpc::response::GetContractState {
                        state: Some(rpc::response::get_contract_state::State::Unchanged(
                            rpc::response::get_contract_state::Timings {
                                gen_lt: state.last_transaction_id.lt(),
                                gen_utime: state.gen_utime,
                            },
                        )),
                    },
                ))
            }
            Ok(ShardAccountFromCache::Found(state)) => state,
            Ok(ShardAccountFromCache::NotFound(timings)) => {
                return Ok(rpc::response::Result::GetContractState(
                    rpc::response::GetContractState {
                        state: Some(rpc::response::get_contract_state::State::NotExists(
                            timings.into(),
                        )),
                    },
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

        let timings = rpc::response::get_contract_state::Timings {
            gen_lt: state.last_transaction_id.lt(),
            gen_utime: state.gen_utime,
        };

        let account = match ton_block::Account::construct_from_cell(state.data) {
            Ok(ton_block::Account::Account(account)) => account,
            Ok(ton_block::Account::AccountNone) => {
                return Ok(rpc::response::Result::GetContractState(
                    rpc::response::GetContractState {
                        state: Some(rpc::response::get_contract_state::State::NotExists(
                            timings.into(),
                        )),
                    },
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

        let last_transaction_id = match state.last_transaction_id {
            LastTransactionId::Exact(transaction_id) => {
                rpc::response::get_contract_state::exists::LastTransactionId::Exact(
                    rpc::response::get_contract_state::exists::Exact {
                        lt: transaction_id.lt,
                        hash: Bytes::copy_from_slice(transaction_id.hash.as_slice()),
                    },
                )
            }
            LastTransactionId::Inexact { latest_lt } => {
                rpc::response::get_contract_state::exists::LastTransactionId::Inexact(
                    rpc::response::get_contract_state::exists::Inexact { latest_lt },
                )
            }
        };

        let result = rpc::response::Result::GetContractState(rpc::response::GetContractState {
            state: Some(rpc::response::get_contract_state::State::Exists(
                rpc::response::get_contract_state::Exists {
                    account,
                    gen_timings: Some(timings),
                    last_transaction_id: Some(last_transaction_id),
                },
            )),
        });

        Ok(result)
    }

    fn get_accounts_by_code_hash(
        &self,
        req: rpc::request::GetAccountsByCodeHash,
    ) -> QueryResult<rpc::response::Result> {
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

        let mut key = [0u8; { tables::CodeHashes::KEY_LEN }];
        if req.code_hash.len() != 32 {
            return Err(QueryError::InvalidParams);
        }
        key[0..32].copy_from_slice(&req.code_hash);

        if let Some(continuation) = &req.continuation {
            if continuation.len() != 33 {
                return Err(QueryError::InvalidParams);
            }
            key[32..65].copy_from_slice(continuation);
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

            result.push(Bytes::copy_from_slice(&value[32..]));
            iter.next();
        }

        Ok(rpc::response::Result::GetAccounts(
            rpc::response::GetAccountsByCodeHash { account: result },
        ))
    }

    fn send_message(&self, req: rpc::request::SendMessage) -> QueryResult<rpc::response::Result> {
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

        Ok(rpc::response::Result::SendMessage(()))
    }

    fn get_transactions_list(
        &self,
        req: rpc::request::GetTransactionsList,
    ) -> QueryResult<rpc::response::Result> {
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
        if req.account.len() != 33 {
            return Err(QueryError::InvalidParams);
        }
        key[0..33].copy_from_slice(&req.account);
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
                    result.push(Bytes::copy_from_slice(value));
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

    fn get_transaction(
        &self,
        req: rpc::request::GetTransaction,
    ) -> QueryResult<rpc::response::Result> {
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
                    transaction: res.map(|slice| Bytes::from(slice.to_vec())),
                },
            )),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
    }

    fn get_dst_transaction(
        &self,
        req: rpc::request::GetDstTransaction,
    ) -> QueryResult<rpc::response::Result> {
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
                    transaction: res.map(|slice| Bytes::from(slice.to_vec())),
                },
            )),
            Err(e) => {
                tracing::error!("failed to get transaction: {e:?}");
                Err(QueryError::StorageError)
            }
        }
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
        Protobuf(rpc::Error {
            code: self.code,
            message: self.message.to_string(),
        })
        .into_response()
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
