use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_jrpc_server::JrpcState;
use ton_indexer::utils::ShardStateStuff;
use ton_indexer::ProcessBlockContext;

pub struct EngineSubscriber {
    jrpc_state: Arc<JrpcState>,
}

impl EngineSubscriber {
    pub fn new(jrpc_state: Arc<JrpcState>) -> EngineSubscriber {
        Self { jrpc_state }
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for EngineSubscriber {
    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        self.jrpc_state
            .process_block(ctx.block_stuff(), ctx.shard_state_stuff())
            .context("Failed to update JRPC state")
    }

    async fn process_full_state(&self, state: &ShardStateStuff) -> Result<()> {
        self.jrpc_state
            .process_full_state(state)
            .await
            .context("Failed to update JRPC state")
    }

    async fn process_blocks_edge(
        &self,
        _: ton_indexer::ProcessBlocksEdgeContext<'_>,
    ) -> Result<()> {
        self.jrpc_state.process_blocks_edge();
        Ok(())
    }
}
