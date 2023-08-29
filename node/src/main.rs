use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use everscale_rpc_server::RpcState;
use is_terminal::IsTerminal;
use pomfrit::formatter::*;
use ton_indexer::Engine;
use tracing_subscriber::EnvFilter;

use self::config::*;
use self::subscriber::*;

mod config;
mod subscriber;

#[global_allocator]
static GLOBAL: broxus_util::alloc::Allocator = broxus_util::alloc::allocator();

#[derive(Debug, PartialEq, Eq, FromArgs)]
#[argh(description = "")]
pub struct App {
    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    pub config: String,

    /// path to global config file
    #[argh(option)]
    pub global_config: String,

    /// compact database and exit
    #[argh(switch)]
    run_compaction: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let logger = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env_lossy(),
    );
    if std::io::stdout().is_terminal() {
        logger.init();
    } else {
        logger.without_time().init();
    }

    let any_signal = broxus_util::any_signal(broxus_util::TERMINATION_SIGNALS);

    let app = broxus_util::read_args_with_version!(_);
    let run = run(app);

    tokio::select! {
        result = run => result,
        signal = any_signal => {
            if let Ok(signal) = signal {
                tracing::warn!(?signal, "received termination signal, flushing state...");
            }
            // NOTE: engine future is safely dropped here so rocksdb method
            // `rocksdb_close` is called in DB object destructor
            Ok(())
        }
    }
}

async fn run(app: App) -> Result<()> {
    tracing::info!(version = env!("CARGO_PKG_VERSION"));

    let panicked = Arc::new(AtomicBool::default());
    let orig_hook = panic::take_hook();
    panic::set_hook({
        let panicked = panicked.clone();
        Box::new(move |panic_info| {
            panicked.store(true, Ordering::Release);
            orig_hook(panic_info);
        })
    });

    let config: AppConfig = broxus_util::read_config(app.config)?;

    let global_config = ton_indexer::GlobalConfig::from_file(&app.global_config)
        .context("Failed to open global config")?;

    tracing::info!("initializing producer...");

    // Prepare Server
    let rpc_state = RpcState::new(config.rpc_config)
        .map(Arc::new)
        .context("Failed to create Server state")?;

    // Create engine
    let engine = Engine::new(
        config
            .node_settings
            .build_indexer_config()
            .await
            .context("Failed building")?,
        global_config,
        Arc::new(EngineSubscriber::new(rpc_state.clone())),
    )
    .await?;

    // Return early for special cases
    if app.run_compaction {
        tracing::warn!("compacting database");
        engine.trigger_compaction().await;
        return Ok(());
    }

    // Create metrics exporter
    let (_exporter, metrics_writer) = pomfrit::create_exporter(config.metrics_settings).await?;
    metrics_writer.spawn({
        let rpc_state = rpc_state.clone();
        let engine = engine.clone();
        move |buf| {
            buf.write(ExplorerMetrics {
                engine: &engine,
                panicked: &panicked,
                rpc_state: &rpc_state,
            });
        }
    });
    tracing::info!("initialized exporter");

    // Start the engine
    engine.start().await.context("Failed to start engine")?;
    tracing::info!("initialized engine");

    // Start RPC after the engine is running
    rpc_state.initialize(&engine).await?;
    tokio::spawn(rpc_state.serve()?);
    tracing::info!("initialized RPC");

    // Done
    tracing::info!("initialized producer");
    futures_util::future::pending().await
}

struct ExplorerMetrics<'a> {
    engine: &'a Engine,
    panicked: &'a AtomicBool,
    rpc_state: &'a RpcState,
}

impl std::fmt::Display for ExplorerMetrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let panicked = self.panicked.load(Ordering::Acquire) as u8;
        f.begin_metric("panicked").value(panicked)?;

        // TON indexer
        let indexer_metrics = self.engine.metrics();

        let last_mc_utime = indexer_metrics.last_mc_utime.load(Ordering::Acquire);
        if last_mc_utime > 0 {
            f.begin_metric("ton_indexer_mc_time_diff")
                .value(indexer_metrics.mc_time_diff.load(Ordering::Acquire))?;
            f.begin_metric("ton_indexer_sc_time_diff").value(
                indexer_metrics
                    .shard_client_time_diff
                    .load(Ordering::Acquire),
            )?;

            f.begin_metric("ton_indexer_last_mc_utime")
                .value(last_mc_utime)?;
        }

        let last_mc_block_seqno = indexer_metrics.last_mc_block_seqno.load(Ordering::Acquire);
        if last_mc_block_seqno > 0 {
            f.begin_metric("ton_indexer_last_mc_block_seqno")
                .value(last_mc_block_seqno)?;
        }

        let last_shard_client_mc_block_seqno = indexer_metrics
            .last_shard_client_mc_block_seqno
            .load(Ordering::Acquire);
        if last_shard_client_mc_block_seqno > 0 {
            f.begin_metric("ton_indexer_last_sc_block_seqno")
                .value(last_shard_client_mc_block_seqno)?;
        }

        // jemalloc

        let broxus_util::alloc::profiling::JemallocStats {
            allocated,
            active,
            metadata,
            resident,
            mapped,
            retained,
            dirty,
            fragmentation,
        } = broxus_util::alloc::profiling::fetch_stats().map_err(|e| {
            tracing::error!("failed to fetch allocator stats: {e:?}");
            std::fmt::Error
        })?;

        f.begin_metric("jemalloc_allocated_bytes")
            .value(allocated)?;
        f.begin_metric("jemalloc_active_bytes").value(active)?;
        f.begin_metric("jemalloc_metadata_bytes").value(metadata)?;
        f.begin_metric("jemalloc_resident_bytes").value(resident)?;
        f.begin_metric("jemalloc_mapped_bytes").value(mapped)?;
        f.begin_metric("jemalloc_retained_bytes").value(retained)?;
        f.begin_metric("jemalloc_dirty_bytes").value(dirty)?;
        f.begin_metric("jemalloc_fragmentation_bytes")
            .value(fragmentation)?;

        // RocksDB

        let ton_indexer::RocksdbStats {
            whole_db_stats,
            block_cache_usage,
            block_cache_pined_usage,
        } = self.engine.get_memory_usage_stats().map_err(|e| {
            tracing::error!("failed to fetch rocksdb stats: {e:?}");
            std::fmt::Error
        })?;

        f.begin_metric("rocksdb_block_cache_usage_bytes")
            .value(block_cache_usage)?;
        f.begin_metric("rocksdb_block_cache_pined_usage_bytes")
            .value(block_cache_pined_usage)?;
        f.begin_metric("rocksdb_memtable_total_size_bytes")
            .value(whole_db_stats.mem_table_total)?;
        f.begin_metric("rocksdb_memtable_unflushed_size_bytes")
            .value(whole_db_stats.mem_table_unflushed)?;
        f.begin_metric("rocksdb_memtable_cache_bytes")
            .value(whole_db_stats.cache_total)?;

        let internal_metrics = self.engine.internal_metrics();

        f.begin_metric("shard_states_cache_len")
            .value(internal_metrics.shard_states_cache_len)?;
        f.begin_metric("shard_states_operations_len")
            .value(internal_metrics.shard_states_operations_len)?;
        f.begin_metric("block_applying_operations_len")
            .value(internal_metrics.block_applying_operations_len)?;
        f.begin_metric("next_block_applying_operations_len")
            .value(internal_metrics.next_block_applying_operations_len)?;

        let cells_cache_stats = internal_metrics.cells_cache_stats;
        f.begin_metric("cells_cache_hits")
            .value(cells_cache_stats.hits)?;
        f.begin_metric("cells_cache_requests")
            .value(cells_cache_stats.requests)?;
        f.begin_metric("cells_cache_occupied")
            .value(cells_cache_stats.occupied)?;
        f.begin_metric("cells_cache_hits_ratio")
            .value(cells_cache_stats.hits_ratio)?;
        f.begin_metric("cells_cache_size_bytes")
            .value(cells_cache_stats.size_bytes)?;

        // RPC

        f.begin_metric("jrpc_enabled").value(1)?;

        let jrpc = self.rpc_state.jrpc_metrics();
        f.begin_metric("jrpc_total").value(jrpc.total)?;
        f.begin_metric("jrpc_errors").value(jrpc.errors)?;
        f.begin_metric("jrpc_not_found").value(jrpc.not_found)?;

        let proto = self.rpc_state.proto_metrics();
        f.begin_metric("proto_total").value(proto.total)?;
        f.begin_metric("proto_errors").value(proto.errors)?;
        f.begin_metric("proto_not_found").value(proto.not_found)?;

        Ok(())
    }
}
