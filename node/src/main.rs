use std::io::IsTerminal;
use std::net::SocketAddr;
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use argh::FromArgs;
use everscale_rpc_server::RpcState;
use std::sync::Arc;
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
    if let Some(c) = config.metrics_settings {
        install_monitoring(c.listen_address)?;
    }
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

fn install_monitoring(metrics_addr: SocketAddr) -> Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        60.0, 120.0, 300.0, 600.0, 3600.0,
    ];
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Prefix("time".to_string()), EXPONENTIAL_SECONDS)?
        .with_http_listener(metrics_addr)
        .install()
        .context("Failed installing metrics exporter")
}
