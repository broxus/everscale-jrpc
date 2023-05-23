use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use arc_swap::{ArcSwapOption, ArcSwapWeak};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use ton_block::{Deserializable, HashmapAugType, Serializable};
use ton_indexer::utils::{BlockStuff, RefMcStateHandle, ShardStateStuff};
use ton_types::HashmapType;
use weedb::{rocksdb, Caches, Migrations, Semver, Table, WeeDb};

use everscale_jrpc_models::*;

use super::{QueryError, QueryResult};

mod tables;

#[derive(Default)]
pub struct JrpcState {
    engine: ArcSwapWeak<ton_indexer::Engine>,
    key_block_response: ArcSwapOption<serde_json::Value>,
    masterchain_accounts_cache: RwLock<Option<ShardAccounts>>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
    counters: Counters,
    storage: Option<Storage>,
}

impl JrpcState {
    pub fn new(storage: Option<JrpcStorageOptions>) -> Result<Self> {
        let storage = storage.map(Storage::new).transpose()?;
        Ok(Self {
            engine: Default::default(),
            key_block_response: Default::default(),
            masterchain_accounts_cache: Default::default(),
            shard_accounts_cache: Default::default(),
            counters: Default::default(),
            storage,
        })
    }

    pub fn metrics(&self) -> JrpcMetrics {
        self.counters.metrics()
    }

    pub fn handle_block_edge(&self) {
        if let Some(storage) = &self.storage {
            storage.update_snapshot();
        }
    }

    pub async fn handle_full_state(&self, _shard_state: &ShardStateStuff) -> Result<()> {
        // let Some(storage) = &self.storage else {
        //     return Ok(())
        // };

        // TODO: fill code hashes

        Ok(())
    }

    pub fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let block_info = &block_stuff.block().read_info()?;
        self.update(
            block_stuff.id(),
            block_stuff.block(),
            block_info,
            shard_state.state().read_accounts()?,
            shard_state.ref_mc_state_handle().clone(),
        )
    }

    pub fn update(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        block_info: &ton_block::BlockInfo,
        accounts: ton_block::ShardAccounts,
        state_handle: Arc<RefMcStateHandle>,
    ) -> Result<()> {
        let shard_accounts = ShardAccounts {
            accounts,
            state_handle,
            gen_utime: block_info.gen_utime().as_u32(),
        };

        if block_id.shard_id.is_masterchain() {
            *self.masterchain_accounts_cache.write() = Some(shard_accounts);
        } else {
            let mut cache = self.shard_accounts_cache.write();

            cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                tracing::warn!("Clearing shard states cache after shards merge/split");

                let block_ids = block_info.read_prev_ids()?;

                match block_ids.len() {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    1 => {
                        // Find all split shards for the block A
                        let (left, right) = block_ids[0].shard_id.split()?;

                        // Remove parent shard of the block A
                        if cache.contains_key(&left) && cache.contains_key(&right) {
                            cache.remove(&block_ids[0].shard_id);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    2 => {
                        // Find and remove all parent shards
                        for block_id in block_info.read_prev_ids()? {
                            cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        };

        if block_info.key_block() {
            self.update_key_block(block);
        }

        if let Some(storage) = &self.storage {
            let workchain = block_id.shard().workchain_id();
            let Ok(workchain) = i8::try_from(workchain) else {
                return Ok(());
            };

            let extra = block.read_extra()?;
            let account_blocks = extra.read_account_blocks()?;

            // Prepare column families
            let mut write_batch = rocksdb::WriteBatch::default();
            let tx_cf = &storage.transactions.cf();
            let tx_by_hash_cf = &storage.transactions_by_hash.cf();
            let tx_by_in_msg_cf = &storage.transactions_by_in_msg.cf();

            // Prepare buffer for full tx id
            let mut tx_full_id = [0u8; { tables::Transactions::KEY_LEN }];
            tx_full_id[0] = workchain as u8;

            // Iterate all changed accounts in block
            let mut non_empty_batch = false;
            account_blocks.iterate_with_keys(|account, value| {
                non_empty_batch |= true;

                // Fill account address in full transaction buffer
                tx_full_id[1..33].copy_from_slice(account.as_slice());

                // Flag to update code hash
                let mut has_special_actions = false;

                // Process account transactions
                value.transactions().iterate_slices(|_, mut value| {
                    let tx_cell = value.checked_drain_reference()?;
                    let tx_hash = tx_cell.repr_hash();
                    let tx_data = ton_types::serialize_toc(&tx_cell)?;
                    let tx = ton_block::Transaction::construct_from_cell(tx_cell)?;

                    tx_full_id[33..].copy_from_slice(&tx.lt.to_be_bytes());

                    // Update code hash
                    let descr = tx.read_description()?;
                    if let Some(action_phase) = descr.action_phase_ref() {
                        has_special_actions |= action_phase.spec_actions != 0;
                    }

                    // Write tx data and indices
                    write_batch.put_cf(tx_cf, tx_full_id.as_slice(), tx_data);
                    write_batch.put_cf(tx_by_hash_cf, tx_hash.as_slice(), tx_full_id.as_slice());
                    if let Some(in_msg_cell) = tx.in_msg_cell() {
                        write_batch.put_cf(
                            tx_by_in_msg_cf,
                            in_msg_cell.repr_hash().as_slice(),
                            tx_full_id.as_slice(),
                        );
                    }

                    Ok(true)
                })?;

                // TODO: Update account code hash if `has_special_actions`

                Ok(true)
            })?;

            if non_empty_batch {
                storage
                    .inner
                    .raw()
                    .write_opt(write_batch, storage.transactions.write_config())
                    .context("Failed to update JRPC storage")?;
            }
        }

        Ok(())
    }

    pub(crate) async fn initialize(&self, engine: &Arc<ton_indexer::Engine>) -> Result<()> {
        self.engine.store(Arc::downgrade(engine));

        if let Ok(last_key_block) = engine.load_last_key_block().await {
            let key_block_response = serde_json::to_value(BlockResponse {
                block: last_key_block.block().clone(),
            })?;

            self.key_block_response
                .compare_and_swap(&None::<Arc<_>>, Some(Arc::new(key_block_response)));
        }

        Ok(())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.engine.load().strong_count() > 0 && self.key_block_response.load().is_some()
    }

    pub(crate) fn counters(&self) -> &Counters {
        &self.counters
    }

    pub(crate) fn timings(&self) -> QueryResult<EngineMetrics> {
        let engine = self.engine.load().upgrade().ok_or(QueryError::NotReady)?;

        let metrics = engine.metrics();
        Ok(EngineMetrics {
            last_mc_block_seqno: metrics.last_mc_block_seqno.load(Ordering::Acquire),
            last_shard_client_mc_block_seqno: metrics
                .last_shard_client_mc_block_seqno
                .load(Ordering::Acquire),
            last_mc_utime: metrics.last_mc_utime.load(Ordering::Acquire),
            mc_time_diff: metrics.mc_time_diff.load(Ordering::Acquire),
            shard_client_time_diff: metrics.shard_client_time_diff.load(Ordering::Acquire),
        })
    }

    pub(crate) fn get_last_key_block(&self) -> QueryResult<Arc<serde_json::Value>> {
        self.key_block_response
            .load_full()
            .ok_or(QueryError::NotReady)
    }

    pub fn get_contract_state_raw(
        &self,
        account: &ton_block::MsgAddressInt,
    ) -> Result<Option<ShardAccount>> {
        let is_masterchain = account.is_masterchain();
        let account = account.address().get_bytestring_on_stack(0);
        let account = ton_types::UInt256::from_slice(account.as_slice());

        if is_masterchain {
            let state = self.masterchain_accounts_cache.read();
            state.as_ref().ok_or(QueryError::NotReady)?.get(&account)
        } else {
            let cache = self.shard_accounts_cache.read();
            let mut state = Ok(None);

            let mut has_account_shard = false;
            for (shard_ident, shard_accounts) in cache.iter() {
                if !contains_account(shard_ident, &account) {
                    continue;
                }

                has_account_shard = true;
                state = shard_accounts.get(&account)
            }

            if !has_account_shard {
                return Err(QueryError::NotReady.into());
            }

            state
        }
    }

    pub fn get_contract_state(
        &self,
        account: &ton_block::MsgAddressInt,
    ) -> QueryResult<serde_json::Value> {
        let state = self.get_contract_state_raw(account);

        let state = match state {
            Ok(Some(state)) => state,
            Ok(None) => return Ok(serde_json::to_value(ContractStateResponse::NotExists).unwrap()),
            Err(e) => {
                let account = account.to_string();
                tracing::error!(account, "Failed to read shard account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };
        let guard = state.state_handle;

        let account = match ton_block::Account::construct_from_cell(state.data)
            .map_err(|_| QueryError::InvalidAccountState)?
        {
            ton_block::Account::AccountNone => {
                return Ok(serde_json::to_value(ContractStateResponse::NotExists).unwrap())
            }
            ton_block::Account::Account(account) => account,
        };

        let response = serde_json::to_value(ContractStateResponse::Exists {
            account,
            timings: nekoton_abi::GenTimings::Known {
                gen_lt: state.last_transaction_id.lt(),
                gen_utime: state.gen_utime,
            },
            last_transaction_id: state.last_transaction_id,
        })
        .map_err(|e| {
            tracing::error!("Failed to serialize shard account: {e:?}");
            QueryError::FailedToSerialize
        })?;

        // NOTE: state guard must be dropped after the serialization
        drop(guard);

        Ok(response)
    }

    pub(crate) async fn send_message(&self, message: ton_block::Message) -> QueryResult<()> {
        let engine = self.engine.load().upgrade().ok_or(QueryError::NotReady)?;

        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => header.dst.workchain_id(),
            _ => return Err(QueryError::ExternalMessageExpected),
        };

        let cells = message
            .serialize()
            .map_err(|_| QueryError::FailedToSerialize)?;

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| QueryError::FailedToSerialize)?;

        engine
            .broadcast_external_message(to, &serialized)
            .map_err(|_| QueryError::ConnectionError)
    }

    pub(crate) fn get_transactions(
        &self,
        account: &ton_block::MsgAddressInt,
        last_transaction_lt: Option<u64>,
        limit: u8,
    ) -> Result<Vec<String>, StorageError> {
        const MAX_LIMIT: u8 = 100;

        let storage = self.get_storage()?;

        if limit == 0 {
            return Ok(Vec::new());
        } else if limit > MAX_LIMIT {
            return Err(StorageError::TooBigLimit);
        }

        let snapshot = storage.load_snapshot()?;

        let mut key = [0u8; { tables::Transactions::KEY_LEN }];
        extract_address(account, &mut key)?;
        key[33..].copy_from_slice(&last_transaction_lt.unwrap_or(u64::MAX).to_be_bytes());

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
                None => {
                    iter.status()?;
                    break;
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn get_transaction(
        &self,
        tx_hash: &[u8; 32],
    ) -> Result<Option<impl AsRef<[u8]> + '_>, StorageError> {
        let storage = self.get_storage()?;

        let Some(key) = storage.transactions_by_hash.get(tx_hash)? else {
            return Ok(None);
        };

        match storage.transactions.get(key)? {
            Some(value) => Ok(Some(value)),
            None => Err(StorageError::IndexMiss),
        }
    }

    pub(crate) fn get_dst_transaction(
        &self,
        in_msg_hash: &[u8; 32],
    ) -> Result<Option<impl AsRef<[u8]> + '_>, StorageError> {
        let storage = self.get_storage()?;

        let Some(key) = storage.transactions_by_in_msg.get(in_msg_hash)? else {
            return Ok(None);
        };

        match storage.transactions.get(key)? {
            Some(value) => Ok(Some(value)),
            None => Err(StorageError::IndexMiss),
        }
    }

    fn update_key_block(&self, block: &ton_block::Block) {
        match serde_json::to_value(BlockResponse {
            block: block.clone(),
        }) {
            Ok(response) => self.key_block_response.store(Some(Arc::new(response))),
            Err(e) => {
                tracing::error!("Failed to update key block: {e:?}");
            }
        }
    }

    fn get_storage(&self) -> Result<&Storage, StorageError> {
        self.storage.as_ref().ok_or(StorageError::Disabled)
    }
}

#[derive(Default)]
pub struct Counters {
    total: AtomicU64,
    not_found: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    pub fn increase_total(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increase_not_found(&self) {
        self.not_found.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increase_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    fn metrics(&self) -> JrpcMetrics {
        JrpcMetrics {
            total: self.total.load(Ordering::Relaxed),
            not_found: self.not_found.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct JrpcMetrics {
    /// Total amount JRPC requests
    pub total: u64,
    /// Number of requests resolved with an error
    pub not_found: u64,
    /// Number of requests with unknown method
    pub errors: u64,
}

pub struct ShardAccount {
    pub data: ton_types::Cell,
    pub last_transaction_id: nekoton_abi::LastTransactionId,
    pub state_handle: Arc<RefMcStateHandle>,
    pub gen_utime: u32,
}

struct ShardAccounts {
    accounts: ton_block::ShardAccounts,
    state_handle: Arc<RefMcStateHandle>,
    gen_utime: u32,
}

impl ShardAccounts {
    fn get(&self, account: &ton_types::UInt256) -> Result<Option<ShardAccount>> {
        match self.accounts.get(account)? {
            Some(account) => Ok(Some(ShardAccount {
                data: account.account_cell(),
                last_transaction_id: nekoton_abi::LastTransactionId::Exact(
                    nekoton_abi::TransactionId {
                        lt: account.last_trans_lt(),
                        hash: *account.last_trans_hash(),
                    },
                ),
                state_handle: self.state_handle.clone(),
                gen_utime: self.gen_utime,
            })),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JrpcStorageOptions {
    pub db_path: PathBuf,
    #[serde(default)]
    pub db_options: DbOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbOptions {
    pub max_memory_usage: usize,
    pub min_caches_capacity: usize,
    pub min_compaction_memory_budget: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            max_memory_usage: 2 << 30,             // 2 GB
            min_caches_capacity: 64 << 20,         // 64 MB
            min_compaction_memory_budget: 1 << 30, // 1 GB
        }
    }
}

struct Storage {
    transactions: Table<tables::Transactions>,
    transactions_by_hash: Table<tables::TransactionsByHash>,
    transactions_by_in_msg: Table<tables::TransactionsByInMsg>,
    code_hashes: Table<tables::CodeHashes>,
    code_hashes_by_address: Table<tables::CodeHashesByAddress>,

    snapshot: ArcSwapOption<OwnedSnapshot>,
    inner: WeeDb,
}

impl Storage {
    const DB_VERSION: Semver = [0, 1, 0];

    fn new(options: JrpcStorageOptions) -> Result<Self> {
        let JrpcStorageOptions {
            db_path,
            db_options,
        } = options;

        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Some(limit) => limit,
            // Current soft limit
            None => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };

        let caches_capacity = std::cmp::max(
            db_options.max_memory_usage / 3,
            db_options.min_caches_capacity,
        );
        let compaction_memory_budget = std::cmp::max(
            db_options.max_memory_usage - db_options.max_memory_usage / 3,
            db_options.min_compaction_memory_budget,
        );

        let caches = Caches::with_capacity(caches_capacity);

        let inner = WeeDb::builder(db_path, caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // io
                opts.set_max_open_files(limit as i32);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                opts.set_max_background_jobs(std::cmp::max((num_cpus::get() as i32) / 2, 2));
                opts.increase_parallelism(num_cpus::get() as i32);

                opts.optimize_level_style_compaction(compaction_memory_budget);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .with_table::<tables::Transactions>()
            .with_table::<tables::TransactionsByHash>()
            .with_table::<tables::TransactionsByInMsg>()
            .with_table::<tables::CodeHashes>()
            .with_table::<tables::CodeHashesByAddress>()
            .build()
            .context("Failed building db")?;

        let migrations = Migrations::with_target_version(Self::DB_VERSION);
        inner
            .apply(migrations)
            .context("Failed to apply migrations")?;

        Ok(Self {
            transactions: inner.instantiate_table(),
            transactions_by_hash: inner.instantiate_table(),
            transactions_by_in_msg: inner.instantiate_table(),
            code_hashes: inner.instantiate_table(),
            code_hashes_by_address: inner.instantiate_table(),
            snapshot: Default::default(),
            inner,
        })
    }

    fn load_snapshot(&self) -> Result<Arc<OwnedSnapshot>, StorageError> {
        self.snapshot
            .load_full()
            .ok_or(StorageError::SnapshotNotFound)
    }

    fn update_snapshot(&self) {
        let snapshot = Arc::new(OwnedSnapshot::new(self.inner.raw().clone()));
        self.snapshot.store(Some(snapshot));
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.snapshot.store(None);
        self.inner.raw().cancel_all_background_work(true);
    }
}

struct OwnedSnapshot {
    inner: rocksdb::Snapshot<'static>,
    _db: Arc<rocksdb::DB>,
}

impl OwnedSnapshot {
    fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::Snapshot;

        unsafe fn extend_lifetime<'a>(r: Snapshot<'a>) -> Snapshot<'static> {
            std::mem::transmute::<Snapshot<'a>, Snapshot<'static>>(r)
        }

        // SAFETY: `Snapshot` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the snapshot
        let inner = unsafe { extend_lifetime(db.as_ref().snapshot()) };
        Self { inner, _db: db }
    }
}

impl std::ops::Deref for OwnedSnapshot {
    type Target = rocksdb::Snapshot<'static>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn extract_address(
    address: &ton_block::MsgAddressInt,
    target: &mut [u8],
) -> Result<(), StorageError> {
    if let ton_block::MsgAddressInt::AddrStd(address) = address {
        let account = address.address.get_bytestring_on_stack(0);
        let account = account.as_ref();

        if target.len() >= 33 && account.len() == 32 {
            target[0] = address.workchain_id as u8;
            target[1..33].copy_from_slice(account);
            return Ok(());
        }
    }

    Err(StorageError::InvalidAddress)
}

fn contains_account(shard: &ton_block::ShardIdent, account: &ton_types::UInt256) -> bool {
    let shard_prefix = shard.shard_prefix_with_tag();
    if shard_prefix == ton_block::SHARD_FULL {
        true
    } else {
        let len = shard.prefix_len();
        let account_prefix = account_prefix(account, len as usize) >> (64 - len);
        let shard_prefix = shard_prefix >> (64 - len);
        account_prefix == shard_prefix
    }
}

fn account_prefix(account: &ton_types::UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for (i, byte) in account.iter().enumerate().take(bytes) {
        value |= (*byte as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("extended API disabled")]
    Disabled,
    #[error("invalid address")]
    InvalidAddress,
    #[error("too big limit")]
    TooBigLimit,
    #[error("DB error")]
    DbError(#[from] rocksdb::Error),
    #[error("snapshot not found")]
    SnapshotNotFound,
    #[error("index miss")]
    IndexMiss,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for byte in account_id.iter_mut().take(8) {
            *byte = 0xff;
        }

        let account_id = ton_types::UInt256::from(account_id);
        for i in 0..64 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }

    #[test]
    fn test_contains_account() {
        let account = ton_types::UInt256::from_be_bytes(
            &hex::decode("459b6795bf4d4c3b930c83fe7625cfee99a762e1e114c749b62bfa751b781fa5")
                .unwrap(),
        );

        let mut shards =
            vec![ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap()];
        for _ in 0..4 {
            let mut new_shards = vec![];
            for shard in &shards {
                let (left, right) = shard.split().unwrap();
                new_shards.push(left);
                new_shards.push(right);
            }

            shards = new_shards;
        }

        let mut target_shard = None;
        for shard in shards {
            if !contains_account(&shard, &account) {
                continue;
            }

            if target_shard.is_some() {
                panic!("Account can't be in two shards");
            }
            target_shard = Some(shard);
        }

        assert!(
            matches!(target_shard, Some(shard) if shard.shard_prefix_with_tag() == 0x4800000000000000)
        );
    }
}
