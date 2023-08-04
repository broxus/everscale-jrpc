use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use everscale_network::{adnl, dht, overlay, rldp};
use rand::Rng;
use serde::{Deserialize, Serialize};
use ton_indexer::OldBlocksPolicy;

/// Full application config
#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// JRPC config
    pub rpc_config: everscale_rpc_server::Config,

    /// Prometheus metrics exporter settings.
    /// Completely disable when not specified
    #[serde(default)]
    pub metrics_settings: Option<pomfrit::Config>,

    /// Light node settings
    #[serde(default)]
    pub node_settings: NodeConfig,
}

/// Light node settings
#[derive(Serialize, Deserialize, Clone)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Node public ip. Automatically determines if None
    pub adnl_public_ip: Option<Ipv4Addr>,

    /// Node port. Default: 30303
    pub adnl_port: u16,

    /// Path to the DB directory. Default: `./db`
    pub db_path: PathBuf,

    /// Path to the ADNL keys. Default: `./adnl-keys.json`.
    /// NOTE: generates new keys if specified path doesn't exist
    pub temp_keys_path: PathBuf,

    pub db_options: ton_indexer::DbOptions,

    /// Archives map queue. Default: 16
    pub parallel_archive_downloads: usize,

    /// Archives GC and uploader options
    pub archive_options: Option<ton_indexer::ArchiveOptions>,

    /// Shard state GC options
    pub state_gc_options: Option<ton_indexer::StateGcOptions>,

    pub start_from: Option<u32>,

    #[serde(default)]
    pub adnl_options: adnl::NodeOptions,
    #[serde(default)]
    pub rldp_options: rldp::NodeOptions,
    #[serde(default)]
    pub dht_options: dht::NodeOptions,
    #[serde(default)]
    pub overlay_shard_options: overlay::OverlayOptions,
    #[serde(default)]
    pub neighbours_options: ton_indexer::NeighboursOptions,

    #[serde(default)]
    pub persistent_state_options: ton_indexer::PersistentStateOptions,
}

impl NodeConfig {
    pub async fn build_indexer_config(self) -> Result<ton_indexer::NodeConfig> {
        // Determine public ip
        let ip_address = broxus_util::resolve_public_ip(self.adnl_public_ip).await?;
        tracing::info!(?ip_address, "using public ip");

        // Generate temp keys
        let adnl_keys = ton_indexer::NodeKeys::load(self.temp_keys_path, false)
            .context("Failed to load temp keys")?;

        // Prepare DB folder
        std::fs::create_dir_all(&self.db_path)?;

        let old_blocks_policy = match self.start_from {
            None => OldBlocksPolicy::Ignore,
            Some(from_seqno) => OldBlocksPolicy::Sync { from_seqno },
        };

        // Done
        Ok(ton_indexer::NodeConfig {
            ip_address: SocketAddrV4::new(ip_address, self.adnl_port),
            adnl_keys,
            rocks_db_path: self.db_path.join("rocksdb"),
            file_db_path: self.db_path.join("files"),
            state_gc_options: self.state_gc_options,
            blocks_gc_options: Some(ton_indexer::BlocksGcOptions {
                kind: ton_indexer::BlocksGcKind::BeforePreviousKeyBlock,
                enable_for_sync: true,
                ..Default::default()
            }),
            shard_state_cache_options: None,
            db_options: self.db_options,
            archive_options: self.archive_options,
            sync_options: ton_indexer::SyncOptions {
                old_blocks_policy,
                parallel_archive_downloads: self.parallel_archive_downloads,
                ..Default::default()
            },
            persistent_state_options: self.persistent_state_options,
            adnl_options: self.adnl_options,
            rldp_options: self.rldp_options,
            dht_options: self.dht_options,
            overlay_shard_options: self.overlay_shard_options,
            neighbours_options: self.neighbours_options,
        })
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            adnl_public_ip: None,
            adnl_port: 30303,
            db_path: "db".into(),
            temp_keys_path: "adnl-keys.json".into(),
            db_options: Default::default(),
            parallel_archive_downloads: 16,
            archive_options: Some(Default::default()),
            state_gc_options: Some(ton_indexer::StateGcOptions {
                offset_sec: rand::thread_rng().gen_range(0..3600),
                interval_sec: 3600,
            }),
            start_from: None,
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
            persistent_state_options: Default::default(),
        }
    }
}

impl ConfigExt for ton_indexer::GlobalConfig {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

pub trait ConfigExt: Sized {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>;
}
