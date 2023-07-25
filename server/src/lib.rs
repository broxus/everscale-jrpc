use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapWeak;
use serde::{Deserialize, Serialize};
use ton_indexer::utils::{BlockStuff, ShardStateStuff};

pub use everscale_rpc_models as models;

use self::server::Server;
use self::storage::{DbOptions, PersistentStorage, RuntimeStorage};

mod jrpc;
mod proto;
mod server;
mod storage;
mod utils;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Listen address of the API.
    pub listen_address: SocketAddr,

    /// Provided API settings.
    /// Default: `ApiConfig::Simple`.
    #[serde(flatten, default)]
    pub api_config: ApiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "camelCase")]
pub enum ApiConfig {
    Simple(CommonApiConfig),
    Full {
        #[serde(flatten)]
        common: CommonApiConfig,
        persistent_db_path: PathBuf,
        #[serde(default)]
        persistent_db_options: DbOptions,
        #[serde(default)]
        shard_split_depth: u8,
    },
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self::Simple(Default::default())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
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

pub struct RpcState {
    config: Config,
    engine: ArcSwapWeak<ton_indexer::Engine>,
    runtime_storage: RuntimeStorage,
    persistent_storage: Option<PersistentStorage>,
    jrpc_counters: Counters,
    proto_counters: Counters,
}

impl RpcState {
    pub fn new(config: Config) -> Result<Self> {
        let persistent_storage = match &config.api_config {
            ApiConfig::Simple(..) => None,
            ApiConfig::Full {
                persistent_db_path,
                persistent_db_options,
                shard_split_depth,
                ..
            } => Some(PersistentStorage::new(
                persistent_db_path,
                persistent_db_options,
                *shard_split_depth,
            )?),
        };

        Ok(Self {
            config,
            engine: Default::default(),
            runtime_storage: Default::default(),
            persistent_storage,
            jrpc_counters: Default::default(),
            proto_counters: Default::default(),
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
        Server::new(self)?.serve()
    }

    pub fn jrpc_metrics(&self) -> Metrics {
        self.jrpc_counters.metrics()
    }

    pub fn proto_metrics(&self) -> Metrics {
        self.proto_counters.metrics()
    }

    pub fn process_blocks_edge(&self) {
        if let Some(storage) = &self.persistent_storage {
            storage.update_snapshot();
        }
    }

    pub async fn process_full_state(&self, shard_state: Arc<ShardStateStuff>) -> Result<()> {
        if let Some(storage) = &self.persistent_storage {
            storage.reset_accounts(shard_state).await?;
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
        if let Some(shard_state) = &shard_state {
            self.runtime_storage
                .update_contract_states(block_id, block_info, shard_state)?;
        }

        if block_info.key_block() {
            self.runtime_storage.update_key_block(block);
        }

        if let Some(storage) = &self.persistent_storage {
            storage.update(block_id, block, shard_state)?;
        }

        Ok(())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.engine.load().strong_count() > 0
    }

    pub(crate) fn jrpc_counters(&self) -> &Counters {
        &self.jrpc_counters
    }

    pub(crate) fn proto_counters(&self) -> &Counters {
        &self.proto_counters
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

    fn metrics(&self) -> Metrics {
        Metrics {
            total: self.total.load(Ordering::Relaxed),
            not_found: self.not_found.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct Metrics {
    /// Total amount requests
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
