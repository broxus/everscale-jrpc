use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::sync::{Barrier, Notify};
use ton_block::ShardIdent;
use ton_indexer::utils::{BlockStuff, ShardStateStuff};

pub(crate) struct GrpcBlockHandler {
    ref_blocks: Mutex<HashMap<ShardIdent, (u32, Arc<Barrier>)>>,
    ready: NotifyReady,
}

impl GrpcBlockHandler {
    pub fn new() -> Self {
        GrpcBlockHandler {
            ref_blocks: Default::default(),
            ready: NotifyReady::new(),
        }
    }

    pub async fn handle_mc_block(&self, block_stuff: &BlockStuff) -> Result<()> {
        if !block_stuff.id().shard().is_masterchain() {
            return Ok(());
        }
        let sc_hashes = block_stuff.block().read_extra()?;
        let sc_hashes = sc_hashes
            .read_custom()?
            .context("Given block is not a master block.")?;
        let sc_hashes = sc_hashes.hashes();

        let barrier = Arc::new(Barrier::new(sc_hashes.len()? + 1));
        {
            let mut shards = self.ref_blocks.lock().unwrap();
            sc_hashes.iterate_shards(|id, info| {
                shards.insert(id, (info.seq_no, barrier.clone()));

                Ok(true)
            })?;
        }
        self.ready.set_ready();

        barrier.wait().await;
        Ok(())
    }

    pub async fn handle_shard_block(&self, block_stuff: &BlockStuff) -> Result<()> {
        if block_stuff.id().shard().is_masterchain() {
            return Ok(());
        }

        let (max_seq_no, barrier) = self
            .get_high_seq_no_for_shard(block_stuff.id().shard())
            .await?;
        if block_stuff.id().seq_no() <= max_seq_no {
            barrier.wait().await;
        }

        Ok(())
    }

    async fn get_high_seq_no_for_shard(&self, shard: &ShardIdent) -> Result<(u32, Arc<Barrier>)> {
        self.ready.wait_ready().await;

        let ref_blocks = self.ref_blocks.lock().unwrap();
        let seq_no = ref_blocks.get(shard).context("Shard not found")?.clone();

        Ok(seq_no)
    }
}

struct NotifyReady {
    pub(crate) ready: Arc<AtomicBool>,
    pub(crate) ready_wait: Arc<Notify>,
}

impl NotifyReady {
    pub fn new() -> Self {
        NotifyReady {
            ready: Arc::new(AtomicBool::new(false)),
            ready_wait: Arc::new(Notify::new()),
        }
    }

    pub fn set_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
        self.ready_wait.notify_one();
    }

    pub async fn wait_ready(&self) {
        if !self.ready.load(Ordering::Relaxed) {
            self.ready_wait.notified().await;
        }
    }
}
