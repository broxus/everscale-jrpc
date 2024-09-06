use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
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
mod ws;

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
        #[serde(flatten)]
        storage: PersistentStorageConfig,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistentStorageConfig {
    pub persistent_db_path: PathBuf,
    #[serde(default)]
    pub persistent_db_options: DbOptions,
    #[serde(default)]
    pub shard_split_depth: u8,
    #[serde(default)]
    pub transactions_gc_options: Option<TransactionsGcOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionsGcOptions {
    pub ttl_sec: u32,
    pub interval_sec: u32,
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
    ws_producer: ws::WsProducer,
    runtime_storage: RuntimeStorage,
    persistent_storage: Option<PersistentStorage>,
    jrpc_counters: Counters,
    proto_counters: Counters,
}

impl RpcState {
    pub fn new(config: Config) -> Result<Self> {
        let persistent_storage = match &config.api_config {
            ApiConfig::Simple(..) => None,
            ApiConfig::Full { storage, .. } => Some(PersistentStorage::new(storage)?),
        };

        Ok(Self {
            config,
            engine: Default::default(),
            runtime_storage: Default::default(),
            ws_producer: Default::default(),
            persistent_storage,
            jrpc_counters: Counters::named("jrpc"),
            proto_counters: Counters::named("proto"),
        })
    }

    pub async fn initialize(self: &Arc<Self>, engine: &Arc<ton_indexer::Engine>) -> Result<()> {
        match engine.load_last_key_block().await {
            Ok(last_key_block) => {
                self.runtime_storage
                    .update_key_block(last_key_block.id().seq_no, last_key_block.block());
            }
            Err(e) => {
                if self.config.api_config.common().generate_stub_keyblock {
                    let zerostate = engine.load_mc_zero_state().await?;
                    self.runtime_storage
                        .update_key_block(0, &make_key_block_stub(&zerostate)?);
                } else {
                    return Err(e);
                }
            }
        };

        self.engine.store(Arc::downgrade(engine));

        if let Some(persistent_storage) = &self.persistent_storage {
            persistent_storage
                .sync_min_tx_lt()
                .context("Failed to sync min transaction lt")?;
        }

        let transactions_gc = self
            .persistent_storage
            .as_ref()
            .and_then(|s| s.transactions_gc_options().clone());

        if let Some(gc) = transactions_gc {
            let this = Arc::downgrade(self);
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(Duration::from_secs(gc.interval_sec as u64));
                loop {
                    interval.tick().await;

                    let Some(item) = this.upgrade() else {
                        return;
                    };
                    let Some(engine) = item.engine.load_full().upgrade() else {
                        return;
                    };
                    let Some(persistent_storage) = item.persistent_storage.as_ref() else {
                        return;
                    };

                    let target_utime = broxus_util::now().saturating_sub(gc.ttl_sec);

                    let min_lt = match engine.find_closest_key_block_lt(target_utime).await {
                        Ok(lt) => lt,
                        Err(e) => {
                            tracing::error!(
                                target_utime,
                                "failed to find the closest key block lt: {e:?}"
                            );
                            continue;
                        }
                    };

                    if let Err(e) = persistent_storage.remove_old_transactions(min_lt).await {
                        tracing::error!(
                            target_utime,
                            min_lt,
                            "failed to remove old transactions: {e:?}"
                        );
                    }
                }
            });
        }

        Ok(())
    }

    pub fn serve(self: Arc<Self>) -> Result<impl Future<Output = ()> + Send + 'static> {
        Server::new(self)?.serve()
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

    pub async fn process_block(
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
        .await
    }

    pub async fn process_block_parts(
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
            self.runtime_storage
                .update_key_block(block_id.seq_no, block);
        }

        if let Some(storage) = &self.persistent_storage {
            storage.update(block_id, block, shard_state)?;
        }

        self.ws_producer
            .handle_block(block_id.shard_id.workchain_id(), block)
            .await?;

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

pub struct InitialState {
    pub smallest_known_lt: u64,
}

struct Counters {
    total: metrics::Counter,
    not_found: metrics::Counter,
    errors: metrics::Counter,
}

impl Counters {
    fn named(name: &'static str) -> Self {
        Self {
            total: metrics::counter!("jrpc_total", "kind" => name),
            not_found: metrics::counter!("jrpc_not_found", "kind" => name),
            errors: metrics::counter!("jrpc_errors", "kind" => name),
        }
    }
    fn increase_total(&self) {
        self.total.increment(1)
    }

    fn increase_not_found(&self) {
        self.not_found.increment(1)
    }

    fn increase_errors(&self) {
        self.errors.increment(1)
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
