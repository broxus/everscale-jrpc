use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use arc_swap::{ArcSwapOption, ArcSwapWeak};
use everscale_jrpc_models::*;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use ton_block::{Deserializable, HashmapAugType, Serializable};
use ton_indexer::utils::{BlockStuff, RefMcStateHandle, ShardStateStuff};

use super::{QueryError, QueryResult};

#[derive(Default)]
pub struct JrpcState {
    engine: ArcSwapWeak<ton_indexer::Engine>,
    key_block_response: ArcSwapOption<serde_json::Value>,
    masterchain_accounts_cache: RwLock<Option<ShardAccounts>>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
    counters: Counters,
}

impl JrpcState {
    pub fn metrics(&self) -> JrpcMetrics {
        self.counters.metrics()
    }

    pub fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let block_info = &block_stuff.block().read_info()?;
        self.update(
            block_stuff.id(),
            block_stuff.block(),
            block_info,
            shard_state.state().read_accounts()?,
            shard_state.ref_mc_state_handle().clone(),
        )
    }

    pub fn update(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        block_info: &ton_block::BlockInfo,
        accounts: ton_block::ShardAccounts,
        state_handle: Arc<RefMcStateHandle>,
    ) -> Result<()> {
        let shard_accounts = ShardAccounts {
            accounts,
            state_handle,
            gen_utime: block_info.gen_utime().0,
        };

        if block_id.shard_id.is_masterchain() {
            *self.masterchain_accounts_cache.write() = Some(shard_accounts);
        } else {
            let mut cache = self.shard_accounts_cache.write();

            cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                log::warn!("Clearing shard states cache after shards merge/split");

                let block_ids = block_info.read_prev_ids()?;

                match block_ids.len() {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    1 => {
                        // Find all split shards for the block A
                        let (left, right) = block_ids[0].shard_id.split()?;

                        // Remove parent shard of the block A
                        if cache.contains_key(&left) && cache.contains_key(&right) {
                            cache.remove(&block_ids[0].shard_id);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    2 => {
                        // Find and remove all parent shards
                        for block_id in block_info.read_prev_ids()? {
                            cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        };

        if block_info.key_block() {
            self.update_key_block(block);
        }

        Ok(())
    }

    pub(crate) async fn initialize(&self, engine: &Arc<ton_indexer::Engine>) -> Result<()> {
        self.engine.store(Arc::downgrade(engine));
        let last_key_block = engine.load_last_key_block().await?;
        let key_block_response = serde_json::to_value(BlockResponse {
            block: last_key_block.block().clone(),
        })?;

        self.key_block_response
            .compare_and_swap(&None::<Arc<_>>, Some(Arc::new(key_block_response)));

        Ok(())
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.engine.load().strong_count() > 0 && self.key_block_response.load().is_some()
    }

    pub(crate) fn counters(&self) -> &Counters {
        &self.counters
    }

    pub(crate) fn get_last_key_block(&self) -> QueryResult<Arc<serde_json::Value>> {
        self.key_block_response
            .load_full()
            .ok_or(QueryError::NotReady)
    }

    pub(crate) fn get_contract_state(
        &self,
        account: &ton_block::MsgAddressInt,
    ) -> QueryResult<serde_json::Value> {
        let state = {
            let is_masterchain = account.is_masterchain();
            let account = account.address().get_bytestring_on_stack(0);
            let account = ton_types::UInt256::from_slice(account.as_slice());

            if is_masterchain {
                let state = self.masterchain_accounts_cache.read();
                state.as_ref().ok_or(QueryError::NotReady)?.get(&account)
            } else {
                let cache = self.shard_accounts_cache.read();
                let mut state = Ok(None);

                let mut has_account_shard = false;
                for (shard_ident, shard_accounts) in cache.iter() {
                    if !contains_account(shard_ident, &account) {
                        continue;
                    }

                    has_account_shard = true;
                    state = shard_accounts.get(&account)
                }

                if !has_account_shard {
                    return Err(QueryError::NotReady);
                }

                state
            }
        };

        let state = match state {
            Ok(Some(state)) => state,
            Ok(None) => return Ok(serde_json::to_value(ContractStateResponse::NotExists).unwrap()),
            Err(e) => {
                log::error!("Failed to read shard account: {e:?}");
                return Err(QueryError::InvalidAccountState);
            }
        };
        let guard = state.state_handle;

        let account = match ton_block::Account::construct_from_cell(state.data)
            .map_err(|_| QueryError::InvalidAccountState)?
        {
            ton_block::Account::AccountNone => {
                return Ok(serde_json::to_value(ContractStateResponse::NotExists).unwrap())
            }
            ton_block::Account::Account(account) => account,
        };

        let response = serde_json::to_value(ContractStateResponse::Exists {
            account,
            timings: nekoton_abi::GenTimings::Known {
                gen_lt: state.last_transaction_id.lt(),
                gen_utime: state.gen_utime,
            },
            last_transaction_id: state.last_transaction_id,
        })
        .map_err(|e| {
            log::error!("Failed to serialize shard account: {e:?}");
            QueryError::FailedToSerialize
        })?;

        // NOTE: state guard must be dropped after the serialization
        drop(guard);

        Ok(response)
    }

    pub(crate) async fn send_message(&self, message: ton_block::Message) -> QueryResult<()> {
        let engine = self.engine.load().upgrade().ok_or(QueryError::NotReady)?;

        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => header.dst.workchain_id(),
            _ => return Err(QueryError::ExternalMessageExpected),
        };

        let cells = message
            .write_to_new_cell()
            .map_err(|_| QueryError::FailedToSerialize)?
            .into();

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| QueryError::FailedToSerialize)?;

        engine
            .broadcast_external_message(to, &serialized)
            .map_err(|_| QueryError::ConnectionError)
    }

    fn update_key_block(&self, block: &ton_block::Block) {
        match serde_json::to_value(BlockResponse {
            block: block.clone(),
        }) {
            Ok(response) => self.key_block_response.store(Some(Arc::new(response))),
            Err(e) => {
                log::error!("Failed to update key block: {e:?}");
            }
        }
    }
}

#[derive(Default)]
pub struct Counters {
    total: AtomicU64,
    not_found: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    pub fn increase_total(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increase_not_found(&self) {
        self.not_found.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increase_errors(&self) {
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

pub(crate) struct ShardAccount {
    pub data: ton_types::Cell,
    pub last_transaction_id: nekoton_abi::LastTransactionId,
    pub state_handle: Arc<RefMcStateHandle>,
    pub gen_utime: u32,
}

struct ShardAccounts {
    accounts: ton_block::ShardAccounts,
    state_handle: Arc<RefMcStateHandle>,
    gen_utime: u32,
}

impl ShardAccounts {
    fn get(&self, account: &ton_types::UInt256) -> Result<Option<ShardAccount>> {
        match self.accounts.get(account)? {
            Some(account) => Ok(Some(ShardAccount {
                data: account.account_cell(),
                last_transaction_id: nekoton_abi::LastTransactionId::Exact(
                    nekoton_abi::TransactionId {
                        lt: account.last_trans_lt(),
                        hash: *account.last_trans_hash(),
                    },
                ),
                state_handle: self.state_handle.clone(),
                gen_utime: self.gen_utime,
            })),
            None => Ok(None),
        }
    }
}

fn contains_account(shard: &ton_block::ShardIdent, account: &ton_types::UInt256) -> bool {
    let shard_prefix = shard.shard_prefix_with_tag();
    if shard_prefix == ton_block::SHARD_FULL {
        true
    } else {
        let len = shard.prefix_len();
        let account_prefix = account_prefix(account, len as usize) >> (64 - len);
        let shard_prefix = shard_prefix >> (64 - len);
        account_prefix == shard_prefix
    }
}

fn account_prefix(account: &ton_types::UInt256, len: usize) -> u64 {
    debug_assert!(len <= 64);

    let account = account.as_slice();

    let mut value: u64 = 0;

    let bytes = len / 8;
    for (i, byte) in account.iter().enumerate().take(bytes) {
        value |= (*byte as u64) << (8 * (7 - i));
    }

    let remainder = len % 8;
    if remainder > 0 {
        let r = account[bytes] >> (8 - remainder);
        value |= (r as u64) << (8 * (7 - bytes) + 8 - remainder);
    }

    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_prefix() {
        let mut account_id = [0u8; 32];
        for byte in account_id.iter_mut().take(8) {
            *byte = 0xff;
        }

        let account_id = ton_types::UInt256::from(account_id);
        for i in 0..64 {
            let prefix = account_prefix(&account_id, i);
            assert_eq!(64 - prefix.trailing_zeros(), i as u32);
        }
    }

    #[test]
    fn test_contains_account() {
        let account = ton_types::UInt256::from_be_bytes(
            &hex::decode("459b6795bf4d4c3b930c83fe7625cfee99a762e1e114c749b62bfa751b781fa5")
                .unwrap(),
        );

        let mut shards =
            vec![ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap()];
        for _ in 0..4 {
            let mut new_shards = vec![];
            for shard in &shards {
                let (left, right) = shard.split().unwrap();
                new_shards.push(left);
                new_shards.push(right);
            }

            shards = new_shards;
        }

        let mut target_shard = None;
        for shard in shards {
            if !contains_account(&shard, &account) {
                continue;
            }

            if target_shard.is_some() {
                panic!("Account can't be in two shards");
            }
            target_shard = Some(shard);
        }

        assert!(
            matches!(target_shard, Some(shard) if shard.shard_prefix_with_tag() == 0x4800000000000000)
        );
    }
}
