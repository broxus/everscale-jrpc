use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use fdlimit::Outcome;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use ton_block::{Deserializable, HashmapAugType, Libraries};
use ton_indexer::utils::{RefMcStateHandle, ShardStateStuff};
use ton_types::{Cell, HashmapType, UInt256};
use weedb::{rocksdb, Caches, Migrations, Semver, Table, WeeDb};

use crate::{PersistentStorageConfig, TransactionsGcOptions};

pub mod tables;

pub struct RuntimeStorage {
    key_block: watch::Sender<Option<(u32, ton_block::Block)>>,
    masterchain_accounts_cache: RwLock<Option<ShardAccounts>>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
}

impl Default for RuntimeStorage {
    fn default() -> Self {
        let (key_block, _) = watch::channel(None);
        Self {
            key_block,
            masterchain_accounts_cache: Default::default(),
            shard_accounts_cache: Default::default(),
        }
    }
}

impl RuntimeStorage {
    pub fn subscribe_to_key_blocks(&self) -> watch::Receiver<Option<(u32, ton_block::Block)>> {
        self.key_block.subscribe()
    }

    pub fn update_key_block(&self, seqno: u32, block: &ton_block::Block) {
        self.key_block.send_replace(Some((seqno, block.clone())));
    }

    pub fn update_contract_states(
        &self,
        block_id: &ton_block::BlockIdExt,
        block_info: &ton_block::BlockInfo,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let accounts = shard_state.state().read_accounts()?;
        let libraries = shard_state.state().libraries().clone();
        let state_handle = shard_state.ref_mc_state_handle().clone();

        let shard_accounts = ShardAccounts {
            libraries,
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
                tracing::debug!("Clearing shard states cache after shards merge/split");

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
        }

        Ok(())
    }

    pub fn get_library_cell(&self, hash: &UInt256) -> Result<Option<Cell>> {
        let state = self.masterchain_accounts_cache.read();
        let lib = match &*state {
            None => {
                tracing::warn!("masterchain_accounts_cache is not ready");
                None
            }
            Some(accounts) => accounts.libraries.get(hash)?,
        };

        Ok(lib.map(|x| x.lib().clone()))
    }

    pub fn get_contract_state(
        &self,
        account: &ton_block::MsgAddressInt,
    ) -> Result<ShardAccountFromCache> {
        let is_masterchain = account.is_masterchain();
        let account = account.address().get_bytestring_on_stack(0);
        let account = ton_types::UInt256::from_slice(account.as_slice());

        Ok(if is_masterchain {
            let state = self.masterchain_accounts_cache.read();
            match &*state {
                None => ShardAccountFromCache::NotReady,
                Some(accounts) => match accounts.get(&account)? {
                    Some(state) => ShardAccountFromCache::Found(state),
                    None => ShardAccountFromCache::NotFound(nekoton_abi::GenTimings::Known {
                        gen_lt: 0,
                        gen_utime: accounts.gen_utime,
                    }),
                },
            }
        } else {
            let cache = self.shard_accounts_cache.read();
            let mut state = Ok(None);

            let mut gen_utime = 0;
            for (shard_ident, shard_accounts) in cache.iter() {
                if !contains_account(shard_ident, &account) {
                    continue;
                }

                gen_utime = shard_accounts.gen_utime;
                state = shard_accounts.get(&account)
            }

            match state? {
                Some(state) => ShardAccountFromCache::Found(state),
                None if gen_utime == 0 => ShardAccountFromCache::NotReady,
                None => ShardAccountFromCache::NotFound(nekoton_abi::GenTimings::Known {
                    gen_lt: 0,
                    gen_utime,
                }),
            }
        })
    }
}

pub enum ShardAccountFromCache {
    NotReady,
    NotFound(nekoton_abi::GenTimings),
    Found(ShardAccount),
}

pub struct ShardAccount {
    pub data: ton_types::Cell,
    pub last_transaction_id: nekoton_abi::LastTransactionId,
    pub state_handle: Arc<RefMcStateHandle>,
    pub gen_utime: u32,
}

struct ShardAccounts {
    libraries: Libraries,
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

pub struct PersistentStorage {
    pub transactions: Table<tables::Transactions>,
    pub transactions_by_hash: Table<tables::TransactionsByHash>,
    pub transactions_by_in_msg: Table<tables::TransactionsByInMsg>,
    pub code_hashes: Table<tables::CodeHashes>,
    pub code_hashes_by_address: Table<tables::CodeHashesByAddress>,

    pub snapshot: ArcSwapOption<OwnedSnapshot>,
    pub inner: WeeDb,

    pub shard_split_depth: u8,
    pub transactions_gc_options: Option<TransactionsGcOptions>,

    pub min_tx_lt: AtomicU64,
}

impl PersistentStorage {
    const DB_VERSION: Semver = [0, 1, 0];

    pub fn new(config: &PersistentStorageConfig) -> Result<Self> {
        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Ok(Outcome::LimitRaised { from, to }) => {
                tracing::info!("Raised fd limit from {} to {}", from, to);
                to
            }
            Ok(Outcome::Unsupported) => {
                tracing::warn!("Failed to raise fd limit: unsupported platform");
                256
            }
            // Current soft limit
            Err(e) => {
                tracing::warn!("Failed to raise fd limit: {e}");
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };
        let options = &config.persistent_db_options;

        let caches_capacity =
            std::cmp::max(options.max_memory_usage / 3, options.min_caches_capacity);
        let compaction_memory_budget = std::cmp::max(
            options.max_memory_usage - options.max_memory_usage / 3,
            options.min_compaction_memory_budget,
        );

        let caches = Caches::with_capacity(caches_capacity);

        let inner = WeeDb::builder(&config.persistent_db_path, caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
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

        // Find smallest lt
        let transactions: Table<tables::Transactions> = inner.instantiate_table();

        Ok(Self {
            transactions,
            transactions_by_hash: inner.instantiate_table(),
            transactions_by_in_msg: inner.instantiate_table(),
            code_hashes: inner.instantiate_table(),
            code_hashes_by_address: inner.instantiate_table(),
            snapshot: Default::default(),
            inner,
            shard_split_depth: config.shard_split_depth,
            transactions_gc_options: config.transactions_gc_options.clone(),
            min_tx_lt: AtomicU64::new(u64::MAX),
        })
    }

    pub fn transactions_gc_options(&self) -> &Option<TransactionsGcOptions> {
        &self.transactions_gc_options
    }

    pub fn load_snapshot(&self) -> Option<Arc<OwnedSnapshot>> {
        self.snapshot.load_full()
    }

    pub fn update_snapshot(&self) {
        let snapshot = Arc::new(OwnedSnapshot::new(self.inner.raw().clone()));
        self.snapshot.store(Some(snapshot));
    }

    pub fn sync_min_tx_lt(&self) -> Result<()> {
        let db = self.inner.raw();

        let min_lt = match db.get(TX_MIN_LT)? {
            Some(value) if value.len() == 8 => {
                Some(u64::from_le_bytes(value[..8].try_into().unwrap()))
            }
            Some(value) if value.is_empty() => None,
            Some(_) => anyhow::bail!("Invalid stored min transaction lt"),
            None => {
                let now = Instant::now();
                tracing::info!("computing the smallest known transaction lt");

                let mut min_lt = None::<u64>;
                for tx in self.transactions.iterator(rocksdb::IteratorMode::Start) {
                    let (k, _) = tx?;
                    if k.len() == tables::Transactions::KEY_LEN {
                        let lt = u64::from_be_bytes(k[33..].try_into().unwrap());
                        match &mut min_lt {
                            Some(min_lt) => *min_lt = (*min_lt).min(lt),
                            None => min_lt = Some(lt),
                        }
                    }
                }

                tracing::info!(
                    elapsed = %humantime::format_duration(now.elapsed()),
                    "computed the smallest known transaction lt",
                );

                min_lt
            }
        };

        tracing::info!(?min_lt);

        self.min_tx_lt
            .store(min_lt.unwrap_or(u64::MAX), Ordering::Release);
        Ok(())
    }

    pub async fn reset_accounts(&self, shard_state: Arc<ShardStateStuff>) -> Result<()> {
        let shard = *shard_state.shard();
        let workchain = shard.workchain_id();
        let Ok(workchain) = i8::try_from(workchain) else {
            return Ok(());
        };

        let now = Instant::now();
        tracing::info!(%shard, "clearing old code hash indices");
        self.remove_code_hashes(&shard).await?;
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "cleared old code hash indices",
        );

        // Split on virtual shards
        let (_state_guard, virtual_shards) = {
            let guard = shard_state.ref_mc_state_handle().clone();

            let mut virtual_shards = FxHashMap::default();
            split_shard(
                shard,
                shard_state.state().read_accounts()?,
                self.shard_split_depth,
                &mut virtual_shards,
            )
            .context("Failed to split shard state into virtual shards")?;

            // NOTE: ensure that root cell is dropped
            drop(shard_state);
            (guard, virtual_shards)
        };

        // Prepare column families
        let mut write_batch = rocksdb::WriteBatch::default();
        let db = self.inner.raw().as_ref();
        let code_hashes_cf = &self.code_hashes.cf();
        let code_hashes_by_address_cf = &self.code_hashes_by_address.cf();

        // Prepare buffer for code hashes ids
        let mut code_hashes_id = [0u8; { tables::CodeHashes::KEY_LEN }];
        code_hashes_id[32] = workchain as u8;

        let mut code_hashes_by_address_id = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
        code_hashes_by_address_id[0] = workchain as u8;

        // Iterate all changed accounts in block
        let mut non_empty_batch = false;
        let now = Instant::now();
        tracing::info!(%shard, "building new code hash indices");

        for (virtual_shard, accounts) in virtual_shards {
            let now = Instant::now();
            tracing::info!(%shard, %virtual_shard, "collecting code hashes");

            for entry in accounts.iter() {
                let (id, mut account) = entry?;
                let id: &[u8; 32] = match id.data().try_into() {
                    Ok(data) => data,
                    Err(_) => continue,
                };

                let code_hash = {
                    ton_block::DepthBalanceInfo::construct_from(&mut account)?; // skip an augmentation
                    match extract_code_hash(ton_block::ShardAccount::construct_from(&mut account)?)?
                    {
                        Some(code_hash) => code_hash,
                        None => continue,
                    }
                };

                non_empty_batch |= true;

                // Fill account address in full code hashes buffer
                code_hashes_id[..32].copy_from_slice(code_hash.as_slice());
                code_hashes_id[33..65].copy_from_slice(id);

                code_hashes_by_address_id[1..33].copy_from_slice(id);

                // Write tx data and indices
                write_batch.put_cf(code_hashes_cf, code_hashes_id.as_slice(), []);
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    code_hash.as_slice(),
                );
            }

            tracing::info!(
                %shard,
                %virtual_shard,
                elapsed = %humantime::format_duration(now.elapsed()),
                "collected code hashes",
            );
        }

        if non_empty_batch {
            db.write_opt(write_batch, self.code_hashes.write_config())
                .context("Failed to update server storage")?;
        }
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "built new code hash indices",
        );

        // Flush indices after delete/insert
        let now = Instant::now();
        tracing::info!(%shard, "flushing code hash indices");
        let bound = Option::<[u8; 0]>::None;
        db.compact_range_cf(code_hashes_cf, bound, bound);
        db.compact_range_cf(code_hashes_by_address_cf, bound, bound);
        tracing::info!(
            %shard,
            elapsed = %humantime::format_duration(now.elapsed()),
            "flushed code hash indices",
        );

        // Done
        Ok(())
    }

    pub async fn remove_old_transactions(&self, min_lt: u64) -> Result<()> {
        const ITEMS_PER_BATCH: usize = 100000;

        type TxKey = [u8; tables::Transactions::KEY_LEN];

        enum PendingDelete {
            Single,
            Range,
        }

        struct GcState<'a> {
            db: &'a rocksdb::DB,
            writeopt: &'a rocksdb::WriteOptions,
            tx_cf: weedb::BoundedCfHandle<'a>,
            tx_by_hash: weedb::BoundedCfHandle<'a>,
            tx_by_in_msg: weedb::BoundedCfHandle<'a>,
            key_range_begin: TxKey,
            key_range_end: TxKey,
            pending_delete: Option<PendingDelete>,
            batch: rocksdb::WriteBatch,
            total_tx: usize,
            total_tx_by_hash: usize,
            total_tx_by_in_msg: usize,
        }

        impl<'a> GcState<'a> {
            fn new(db: &'a PersistentStorage) -> Self {
                Self {
                    db: db.inner.raw(),
                    writeopt: db.transactions.write_config(),
                    tx_cf: db.transactions.cf(),
                    tx_by_hash: db.transactions_by_hash.cf(),
                    tx_by_in_msg: db.transactions_by_in_msg.cf(),
                    key_range_begin: [0u8; tables::Transactions::KEY_LEN],
                    key_range_end: [0u8; tables::Transactions::KEY_LEN],
                    pending_delete: None,
                    batch: Default::default(),
                    total_tx: 0,
                    total_tx_by_hash: 0,
                    total_tx_by_in_msg: 0,
                }
            }

            fn delete_tx(&mut self, key: &TxKey, mut value: &[u8]) {
                // Batch multiple deletes for the primary table
                self.pending_delete = Some(if self.pending_delete.is_none() {
                    self.key_range_begin.copy_from_slice(key);
                    PendingDelete::Single
                } else {
                    self.key_range_end.copy_from_slice(key);
                    PendingDelete::Range
                });
                self.total_tx += 1;

                // Delete secondary index entries
                if let Ok(tx_cell) = ton_types::deserialize_tree_of_cells(&mut value) {
                    // Delete transaction by hash index entry
                    self.batch
                        .delete_cf(&self.tx_by_hash, tx_cell.repr_hash().as_slice());
                    self.total_tx_by_hash += 1;

                    // Delete transaction by incoming message hash index entry
                    if let Ok(tx) = ton_block::Transaction::construct_from_cell(tx_cell) {
                        if let Some(in_msg) = tx.in_msg_cell() {
                            self.batch
                                .delete_cf(&self.tx_by_in_msg, in_msg.repr_hash().as_slice());
                            self.total_tx_by_in_msg += 1;
                        }
                    }
                }
            }

            fn end_account(&mut self) {
                // Flush pending batch
                if let Some(pending) = self.pending_delete.take() {
                    match pending {
                        PendingDelete::Single => self
                            .batch
                            .delete_cf(&self.tx_cf, self.key_range_begin.as_slice()),
                        PendingDelete::Range => self.batch.delete_range_cf(
                            &self.tx_cf,
                            self.key_range_begin.as_slice(),
                            self.key_range_end.as_slice(),
                        ),
                    }
                }
            }

            fn flush(&mut self) -> Result<()> {
                self.db
                    .write_opt(std::mem::take(&mut self.batch), self.writeopt)?;
                Ok(())
            }
        }

        let db = self.inner.raw().as_ref();

        // Check min lt from the last run
        if let Some(known_min_lt) = db.get(TX_MIN_LT)? {
            if known_min_lt.len() == 8 {
                let known_min_lt = u64::from_le_bytes(known_min_lt[..8].try_into().unwrap());
                let was_running =
                    matches!(db.get(TX_GC_RUNNING)?, Some(status) if !status.is_empty());

                if !was_running && min_lt <= known_min_lt {
                    tracing::info!(known_min_lt, min_lt, "skipping removing old transactions");
                    return Ok(());
                }
            }
        }

        // Force update min lt and gc flag
        self.min_tx_lt.store(min_lt, Ordering::Release);
        db.put(TX_MIN_LT, min_lt.to_le_bytes())?;
        db.put(TX_GC_RUNNING, [1])?;

        let now = Instant::now();
        tracing::info!(min_lt, "removing old transactions");

        // Prepare snapshot and iterator
        let snapshot = db.snapshot();
        let mut readopts = self.transactions.new_read_config();
        readopts.set_snapshot(&snapshot);
        let mut iter = db.raw_iterator_cf_opt(&self.transactions.cf(), readopts);
        iter.seek_to_first();

        // Prepare gc state
        let mut gc = GcState::new(self);

        // `last_account` buffer is used to track the last processed account.
        //
        // The buffer is also used to seek for the next account. Its last
        // 8 bytes are `u64::MAX`. It forces the `seek` method to jump right
        // to the first tx of the next account (assuming that there are no
        // transactions with LT == u64::MAX).
        let mut last_account: TxKey = [0u8; tables::Transactions::KEY_LEN];
        last_account[33..41].copy_from_slice(&u64::MAX.to_be_bytes());

        let mut items = 0usize;
        let mut total_invalid = 0usize;
        let mut iteration = 0usize;
        loop {
            let Some((key, value)) = iter.item() else {
                break iter.status()?;
            };
            iteration += 1;

            let Ok::<&TxKey, _>(key) = key.try_into() else {
                // Remove invalid entires from the primary index only
                items += 1;
                total_invalid += 1;
                gc.batch.delete_cf(&gc.tx_cf, key);
                continue;
            };

            // Check whether the next account is processed
            let item_account = &key[..33];
            let is_next_account = item_account != &last_account[..33];
            if is_next_account {
                // Update last account address
                last_account[..33].copy_from_slice(item_account);

                // Add pending delete into batch
                gc.end_account();
            }

            // Get lt from the key
            let lt = u64::from_be_bytes(key[33..41].try_into().unwrap());

            if lt < min_lt {
                // Add tx and its secondary indices into the batch
                items += 1;
                gc.delete_tx(key, value);
                iter.next();
            } else {
                // Seek to the next account
                if lt < u64::MAX {
                    iter.seek(last_account.as_slice());
                } else {
                    iter.next();
                }
            }

            // Write batch
            if items >= ITEMS_PER_BATCH {
                tracing::info!(iteration, "flushing batch");
                gc.flush()?;
                items = 0;
            }

            if iteration % 1000 == 0 {
                // Give some rest to the thread
                tokio::task::yield_now().await;
            }
        }

        // Add final pending delete into batch
        gc.end_account();

        // Write remaining batch
        if items != 0 {
            gc.flush()?;
        }

        // Reset gc flag
        db.put(TX_GC_RUNNING, [])?;

        tracing::info!(
            elapsed = %humantime::format_duration(now.elapsed()),
            min_lt,
            total_invalid,
            total_tx = gc.total_tx,
            total_tx_by_hash = gc.total_tx_by_hash,
            total_tx_by_in_msg = gc.total_tx_by_in_msg,
            "removed old transactions",
        );

        // Done
        Ok(())
    }

    pub fn update(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let workchain = block_id.shard().workchain_id();
        let Ok(workchain) = i8::try_from(workchain) else {
            return Ok(());
        };

        let extra = block.read_extra()?;
        let account_blocks = extra.read_account_blocks()?;
        let accounts = shard_state
            .map(|shard_state| shard_state.state().read_accounts())
            .transpose()?;

        //

        // Prepare column families
        let mut write_batch = rocksdb::WriteBatch::default();
        let tx_cf = &self.transactions.cf();
        let tx_by_hash_cf = &self.transactions_by_hash.cf();
        let tx_by_in_msg_cf = &self.transactions_by_in_msg.cf();

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
            let mut has_special_actions = accounts.is_none(); // skip updates for this flag if no state
            let mut was_active = false;
            let mut is_active = false;

            // Process account transactions
            let mut first_tx = true;
            value.transactions().iterate_slices(|_, mut value| {
                let tx_cell = value.checked_drain_reference()?;
                let tx_hash = tx_cell.repr_hash();
                let tx_data = ton_types::serialize_toc(&tx_cell)?;
                let tx = ton_block::Transaction::construct_from_cell(tx_cell)?;

                tx_full_id[33..].copy_from_slice(&tx.lt.to_be_bytes());

                // Update marker flags
                if first_tx {
                    // Remember the original status from the first transaction
                    was_active = tx.orig_status == ton_block::AccountStatus::AccStateActive;
                    first_tx = false;
                }
                if was_active && tx.orig_status != ton_block::AccountStatus::AccStateActive {
                    // Handle the case when an account (with some updated code) was deleted,
                    // and then deployed with the initial code (end status).
                    // Treat this situation as a special action.
                    has_special_actions = true;
                }
                is_active = tx.end_status == ton_block::AccountStatus::AccStateActive;

                if !has_special_actions {
                    // Search for special actions (might be code hash update)
                    let descr = tx.read_description()?;
                    if let Some(action_phase) = descr.action_phase_ref() {
                        has_special_actions |= action_phase.spec_actions != 0;
                    }
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

            // Update code hash
            if let Some(accounts) = &accounts {
                let update = if is_active && (!was_active || has_special_actions) {
                    // Account is active after this block and this is either a new account,
                    // or it was an existing account which possibly changed its code.
                    // Update: just store the code hash.
                    Some(false)
                } else if was_active && !is_active {
                    // Account was active before this block and is not active after the block.
                    // Update: remove the code hash.
                    Some(true)
                } else {
                    // No update for other cases
                    None
                };

                // Apply the update if any
                if let Some(remove) = update {
                    self.update_code_hash(workchain, &account, accounts, remove, &mut write_batch)?;
                }
            }

            Ok(true)
        })?;

        if non_empty_batch {
            self.inner
                .raw()
                .write_opt(write_batch, self.transactions.write_config())
                .context("Failed to update server storage")?;
        }

        Ok(())
    }

    fn update_code_hash(
        &self,
        workchain: i8,
        account: &UInt256,
        accounts: &ton_block::ShardAccounts,
        remove: bool,
        write_batch: &mut rocksdb::WriteBatch,
    ) -> Result<()> {
        // Prepare column families
        let code_hashes_cf = &self.code_hashes.cf();
        let code_hashes_by_address_cf = &self.code_hashes_by_address.cf();

        // Check the secondary index first
        let mut code_hashes_by_address_id = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
        code_hashes_by_address_id[0] = workchain as u8;
        code_hashes_by_address_id[1..33].copy_from_slice(account.as_slice());

        // Find the old code hash
        let old_code_hash = self
            .code_hashes_by_address
            .get(code_hashes_by_address_id.as_slice())?;

        // Find the new code hash
        let new_code_hash = 'code_hash: {
            if !remove {
                if let Some(account) = accounts.get(account)? {
                    break 'code_hash extract_code_hash(account)?;
                }
            }
            None
        };

        if remove && old_code_hash.is_none()
            || matches!(
                (&old_code_hash, &new_code_hash),
                (Some(old), Some(new)) if old.as_ref() == new.as_slice()
            )
        {
            // Code hash should not be changed.
            return Ok(());
        }

        let mut code_hashes_id = [0u8; { tables::CodeHashes::KEY_LEN }];
        code_hashes_id[32] = workchain as u8;
        code_hashes_id[33..65].copy_from_slice(account.as_slice());

        // Remove entry from the primary index
        if let Some(old_code_hash) = old_code_hash {
            code_hashes_id[..32].copy_from_slice(&old_code_hash);
            write_batch.delete_cf(code_hashes_cf, code_hashes_id.as_slice());
        }

        match new_code_hash {
            Some(new_code_hash) => {
                // Update primary index
                code_hashes_id[..32].copy_from_slice(new_code_hash.as_slice());
                write_batch.put_cf(
                    code_hashes_cf,
                    code_hashes_id.as_slice(),
                    new_code_hash.as_slice(),
                );

                // Update secondary index
                write_batch.put_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                    new_code_hash.as_slice(),
                );
            }
            None => {
                // Remove entry from the secondary index
                write_batch.delete_cf(
                    code_hashes_by_address_cf,
                    code_hashes_by_address_id.as_slice(),
                );
            }
        }

        Ok(())
    }

    async fn remove_code_hashes(&self, shard: &ton_block::ShardIdent) -> Result<()> {
        let workchain = shard.workchain_id() as u8;

        // Remove from the secondary index first
        {
            let mut from = [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            from[0] = workchain;

            {
                let [_, from @ ..] = &mut from;
                extend_account_prefix(shard, false, from);
            }

            let mut to = from;
            {
                let [_, to @ ..] = &mut to;
                extend_account_prefix(shard, true, to);
            }

            let db = self.inner.raw().as_ref();
            let cf = &self.code_hashes_by_address.cf();
            let writeopts = self.code_hashes_by_address.write_config();

            // Remove `[from; to)`
            db.delete_range_cf_opt(cf, &from, &to, writeopts)?;
            // Remove `to`, (-1:ffff..ffff might be a valid existing address)
            db.delete_cf_opt(cf, to, writeopts)?;
        }

        // Full scan the main code hashes index and remove all entires for the shard
        let db = self.inner.raw().clone();
        let cf = self.code_hashes.get_unbounded_cf();
        let writeopts = self.code_hashes.new_write_config();
        let mut readopts = self.code_hashes.new_read_config();

        let shard = *shard;
        tokio::task::spawn_blocking(move || {
            let cf = cf.bound();
            let snapshot = db.snapshot();
            readopts.set_snapshot(&snapshot);

            let mut iter = db.raw_iterator_cf_opt(&cf, readopts);
            iter.seek_to_first();

            let mut prefix = shard.shard_prefix_with_tag();
            let mut lower_bits = ton_block::ShardIdent::lower_bits(prefix);
            prefix -= lower_bits;
            lower_bits |= lower_bits - 1;

            loop {
                let key = match iter.key() {
                    Some(key) => key,
                    None => return iter.status(),
                };

                if key.len() != tables::CodeHashes::KEY_LEN
                    || key[32] == workchain
                        && (shard.is_full() || {
                            let key = u64::from_be_bytes(key[33..41].try_into().unwrap());
                            (key ^ prefix) & !lower_bits == 0
                        })
                {
                    db.delete_cf_opt(&cf, key, &writeopts)?;
                }

                iter.next();
            }
        })
        .await??;

        // Done
        Ok(())
    }
}

impl Drop for PersistentStorage {
    fn drop(&mut self) {
        self.snapshot.store(None);
        self.inner.raw().cancel_all_background_work(true);
    }
}

pub struct OwnedSnapshot {
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

fn contains_account(shard: &ton_block::ShardIdent, account: &ton_types::UInt256) -> bool {
    if shard.is_full() {
        return true;
    }

    let mut shard_prefix = shard.shard_prefix_with_tag();
    let mut lower_bits = ton_block::ShardIdent::lower_bits(shard_prefix);
    shard_prefix -= lower_bits;
    lower_bits |= lower_bits - 1;

    let account_prefix = u64::from_be_bytes(account.as_slice()[..8].try_into().unwrap());
    (account_prefix ^ shard_prefix) & !lower_bits == 0
}

fn extend_account_prefix(shard: &ton_block::ShardIdent, max: bool, target: &mut [u8; 32]) {
    let mut prefix = shard.shard_prefix_with_tag();
    if max {
        prefix |= prefix - 1;
    } else {
        prefix -= ton_block::ShardIdent::lower_bits(prefix);
    };
    target[..8].copy_from_slice(&prefix.to_be_bytes());
    target[8..].fill(0xff * max as u8);
}

fn extract_code_hash(account: ton_block::ShardAccount) -> Result<Option<ton_types::UInt256>> {
    if let ton_block::Account::Account(account) = account.read_account()? {
        if let ton_block::AccountState::AccountActive { state_init } = account.storage.state {
            if let Some(code) = state_init.code {
                return Ok(Some(code.repr_hash()));
            }
        }
    }
    Ok(None)
}

fn split_shard(
    ident: ton_block::ShardIdent,
    accounts: ton_block::ShardAccounts,
    depth: u8,
    shards: &mut FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>,
) -> Result<()> {
    if depth == 0 {
        shards.insert(ident, accounts);
        return Ok(());
    }

    let (left_shard_ident, right_shard_ident) = ident.split()?;
    let (left_accounts, right_accounts) = accounts.split(&ident.shard_key(false))?;

    split_shard(left_shard_ident, left_accounts, depth - 1, shards)?;
    split_shard(right_shard_ident, right_accounts, depth - 1, shards)?;

    Ok(())
}

const TX_MIN_LT: &[u8] = b"tx_min_lt";
const TX_GC_RUNNING: &[u8] = b"tx_gc_running";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_prefix() {
        fn make_addr(byte: u8) -> ton_types::UInt256 {
            ton_types::UInt256::from([byte; 32])
        }

        let (s_4, s_c) = ton_block::ShardIdent::full(0).split().unwrap();

        // 0100
        assert!(contains_account(&s_4, &make_addr(0x00)));
        assert!(!contains_account(&s_c, &make_addr(0x00)));
        assert!(contains_account(&s_4, &make_addr(0b01111111)));
        assert!(!contains_account(&s_c, &make_addr(0b01111111)));

        // 1100
        assert!(!contains_account(&s_4, &make_addr(0xd1)));
        assert!(contains_account(&s_c, &make_addr(0xd0)));
        assert!(!contains_account(&s_4, &make_addr(0b11111111)));
        assert!(contains_account(&s_c, &make_addr(0b11111111)));
    }
}
