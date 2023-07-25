use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_rpc_server::RpcState;
use ton_indexer::utils::ShardStateStuff;
use ton_indexer::ProcessBlockContext;

pub struct EngineSubscriber {
    rpc_state: Arc<RpcState>,
}

impl EngineSubscriber {
    pub fn new(rpc_state: Arc<RpcState>) -> EngineSubscriber {
        Self { rpc_state }
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for EngineSubscriber {
    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        self.rpc_state
            .process_block(ctx.block_stuff(), ctx.shard_state_stuff())
            .context("Failed to update server state")
    }

    async fn process_full_state(&self, state: Arc<ShardStateStuff>) -> Result<()> {
        self.rpc_state
            .process_full_state(state)
            .await
            .context("Failed to update server state")
    }

    async fn process_blocks_edge(
        &self,
        _: ton_indexer::ProcessBlocksEdgeContext<'_>,
    ) -> Result<()> {
        self.rpc_state.process_blocks_edge();
        Ok(())
    }
}
