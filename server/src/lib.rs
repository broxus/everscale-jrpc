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

use anyhow::Result;
use arc_swap::ArcSwapWeak;
use serde::{Deserialize, Serialize};
use ton_indexer::utils::{BlockStuff, ShardStateStuff};

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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ApiConfig {
    #[default]
    Simple,
    Full {
        persistent_db_path: PathBuf,
        #[serde(default)]
        persistent_db_options: DbOptions,
    },
}

impl ApiConfig {
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
            ApiConfig::Simple => None,
            ApiConfig::Full {
                persistent_db_path,
                persistent_db_options,
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
        if let Ok(last_key_block) = engine.load_last_key_block().await {
            self.runtime_storage
                .update_key_block(last_key_block.block());
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

    pub async fn process_full_state(&self, _shard_state: &ShardStateStuff) -> Result<()> {
        // let Some(storage) = &self.storage else {
        //     return Ok(())
        // };

        // TODO: fill code hashes

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
