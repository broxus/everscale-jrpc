//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use everscale_jrpc_server::*;
//! use async_trait::async_trait;
//! struct ExampleSubscriber {
//!     jrpc_state: Arc<JrpcState>,
//! }
//!
//! #[async_trait]
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
//!     let jrpc_state = Arc::new(JrpcState::new(None));
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
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use arc_swap::ArcSwapWeak;
use serde::{Deserialize, Serialize};
use ton_block::Account::Account;
use ton_block::AccountState::AccountActive;
use ton_block::HashmapAugType;
use ton_block::{AccountStuff, StateInit};
use ton_indexer::utils::{BlockStuff, ShardStateStuff};
use weedb::rocksdb;

use crate::storage::tables;
pub use everscale_jrpc_models as models;

use self::server::JrpcServer;
use self::storage::{DbOptions, PersistentStorage, RuntimeStorage};

mod server;
mod storage;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Listen address of the API.
    pub listen_address: SocketAddr,

    /// Provided API settings.
    /// Default: `ApiConfig::Simple`.
    #[serde(default)]
    pub api_config: ApiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ApiConfig {
    Simple(CommonApiConfig),
    Full {
        #[serde(flatten)]
        common: CommonApiConfig,
        persistent_db_path: PathBuf,
        #[serde(default)]
        persistent_db_options: DbOptions,
    },
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self::Simple(Default::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CommonApiConfig {
    /// Whether to generate a stub keyblock from zerostate. Default: `false`.
    pub generate_stub_keyblock: bool,
}

impl ApiConfig {
    pub fn common(&self) -> &CommonApiConfig {
        match self {
            Self::Simple(common) => common,
            Self::Full { common, .. } => common,
        }
    }

    pub fn is_full(&self) -> bool {
        matches!(self, Self::Full { .. })
    }
}

pub struct JrpcState {
    config: Config,
    engine: ArcSwapWeak<ton_indexer::Engine>,
    runtime_storage: RuntimeStorage,
    persistent_storage: Option<PersistentStorage>,
    counters: Counters,
}

impl JrpcState {
    pub fn new(config: Config) -> Result<Self> {
        let persistent_storage = match &config.api_config {
            ApiConfig::Simple(..) => None,
            ApiConfig::Full {
                persistent_db_path,
                persistent_db_options,
                ..
            } => Some(PersistentStorage::new(
                persistent_db_path,
                persistent_db_options,
            )?),
        };

        Ok(Self {
            config,
            engine: Default::default(),
            runtime_storage: Default::default(),
            persistent_storage,
            counters: Default::default(),
        })
    }

    pub async fn initialize(&self, engine: &Arc<ton_indexer::Engine>) -> Result<()> {
        match engine.load_last_key_block().await {
            Ok(last_key_block) => {
                self.runtime_storage
                    .update_key_block(last_key_block.block());
            }
            Err(e) => {
                if self.config.api_config.common().generate_stub_keyblock {
                    let zerostate = engine.load_mc_zero_state().await?;
                    self.runtime_storage
                        .update_key_block(&make_key_block_stub(&zerostate)?);
                } else {
                    return Err(e);
                }
            }
        }

        self.engine.store(Arc::downgrade(engine));
        Ok(())
    }

    pub fn serve(self: Arc<Self>) -> Result<impl Future<Output = ()> + Send + 'static> {
        JrpcServer::new(self)?.serve()
    }

    pub fn metrics(&self) -> JrpcMetrics {
        self.counters.metrics()
    }

    pub fn process_blocks_edge(&self) {
        if let Some(storage) = &self.persistent_storage {
            storage.update_snapshot();
        }
    }

    pub async fn process_full_state(&self, shard_state: &ShardStateStuff) -> Result<()> {
        if let Some(storage) = &self.persistent_storage {
            let accounts = shard_state.state().read_accounts()?;
            // Prepare column families
            let mut write_batch = rocksdb::WriteBatch::default();
            let code_hashes_cf = &storage.code_hashes.cf();
            let code_hashes_by_address_cf = &storage.code_hashes_by_address.cf();

            // Prepare buffer for code hashes ids
            let mut code_hashes_full_id = [0u8; { tables::CodeHashes::KEY_LEN }];
            let mut code_hashes_by_address_full_id =
                [0u8; { tables::CodeHashesByAddress::KEY_LEN }];
            let workchain = shard_state.shard().workchain_id();
            code_hashes_full_id[32] = workchain as u8;
            code_hashes_by_address_full_id[0] = workchain as u8;

            // Iterate all changed accounts in block
            let mut non_empty_batch = false;
            accounts.iterate_with_keys(|id, account| {
                non_empty_batch |= true;

                // Fill account address in full code hashes buffer
                code_hashes_full_id[33..65].copy_from_slice(id.as_slice());
                code_hashes_by_address_full_id[1..33].copy_from_slice(id.as_slice());

                if let Account(AccountStuff { storage, .. }) = account.read_account()? {
                    if let AccountActive {
                        state_init: StateInit { code, .. },
                    } = storage.state
                    {
                        if let Some(code_hash) = code {
                            code_hashes_full_id[..32]
                                .copy_from_slice(code_hash.repr_hash().as_slice());
                            // Write tx data and indices
                            write_batch.put_cf(
                                code_hashes_cf,
                                code_hashes_full_id.as_slice(),
                                &[0; 1],
                            );
                            write_batch.put_cf(
                                code_hashes_by_address_cf,
                                code_hashes_by_address_full_id.as_slice(),
                                code_hash.repr_hash().as_slice(),
                            );
                        }
                    }
                }

                Ok(true)
            })?;

            if non_empty_batch {
                storage
                    .inner
                    .raw()
                    .write_opt(write_batch, storage.code_hashes.write_config())
                    .context("Failed to update JRPC storage")?;
            }
        }

        Ok(())
    }

    pub fn process_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let block_info = &block_stuff.block().read_info()?;
        self.process_block_parts(
            block_stuff.id(),
            block_stuff.block(),
            block_info,
            shard_state,
        )
    }

    pub fn process_block_parts(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        block_info: &ton_block::BlockInfo,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        if let Some(shard_state) = shard_state {
            self.runtime_storage
                .update_contract_states(block_id, block_info, shard_state)?;
        }

        if block_info.key_block() {
            self.runtime_storage.update_key_block(block);
        }

        if let Some(storage) = &self.persistent_storage {
            storage.update(block_id, block)?;
        }

        Ok(())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.engine.load().strong_count() > 0
    }

    pub(crate) fn counters(&self) -> &Counters {
        &self.counters
    }
}

#[derive(Default)]
struct Counters {
    total: AtomicU64,
    not_found: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    fn increase_total(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn increase_not_found(&self) {
        self.not_found.fetch_add(1, Ordering::Relaxed);
    }

    fn increase_errors(&self) {
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

fn make_key_block_stub(zerostate: &ShardStateStuff) -> Result<ton_block::Block> {
    let state = zerostate.state();

    let mut block_info = ton_block::BlockInfo::new();
    block_info.set_key_block(true);
    block_info.set_gen_utime(state.gen_time().into());

    let mut extra = ton_block::BlockExtra::new();

    let mut mc_block_extra = ton_block::McBlockExtra::default();
    mc_block_extra.set_config(zerostate.config_params()?.clone());
    extra.write_custom(Some(&mc_block_extra))?;

    ton_block::Block::with_params(
        state.global_id(),
        block_info,
        Default::default(),
        Default::default(),
        extra,
    )
}
