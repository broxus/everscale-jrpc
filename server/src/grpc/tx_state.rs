use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use futures::stream::BoxStream;
use futures::{SinkExt, Stream, StreamExt};
use rocksdb_builder::{DbCaches, Tree};
use tonic::async_trait;
use tonic::codegen::Bytes;

use everscale_proto::pb;
use everscale_proto::pb::{GetTransactionRequest, GetTransactionResp};

use super::JrpcState;

#[derive(Clone)]
pub struct StreamService {
    state: Arc<JrpcState>,
    pub tx_state: Arc<Db>,
}

impl StreamService {
    pub fn new<P: AsRef<Path>>(state: Arc<JrpcState>, path: P) -> Result<Self> {
        Ok(Self {
            state,
            tx_state: Arc::new(Db::new(path).context("Failed to create db client")?),
        })
    }
    pub fn state(&self) -> Arc<JrpcState> {
        self.state.clone()
    }
}

#[async_trait]
impl pb::stream_server::Stream for StreamService {
    type GetTransactionStream = BoxStream<'static, Result<pb::GetTransactionResp, tonic::Status>>;

    async fn get_transaction(
        &self,
        request: tonic::Request<pb::GetTransactionRequest>,
    ) -> Result<tonic::Response<Self::GetTransactionStream>, tonic::Status> {
        log::info!("get_transaction request: {:?}", request);
        let request = request.into_inner();
        let stream = match self.tx_state.clone().stream_transactions(request).await {
            Ok(stream) => stream.map(Ok),
            Err(e) => {
                log::error!("Failed to get stream: {}", e);
                return Err(tonic::Status::new(tonic::Code::Internal, e.to_string()));
            }
        };

        Ok(tonic::Response::new(stream.boxed()))
    }
}

pub struct Db {
    tree: Tree<Transactions>,
    notify: Arc<tokio::sync::Notify>,
}

impl Db {
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        let caches = DbCaches::with_capacity(256 * 1024 * 1024)?;
        let db = rocksdb_builder::DbBuilder::new(file_path, &caches)
            .options(|opts, _caches| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                let parallelism = std::thread::available_parallelism()
                    .map(|x| x.get())
                    .unwrap_or(1);
                opts.set_max_background_jobs(std::cmp::max((parallelism as i32) / 2, 2));
                opts.increase_parallelism(parallelism as i32);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .column::<Transactions>()
            .build()
            .context("Failed building db")?;

        Ok(Self {
            tree: Tree::new(&db)?,
            notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    pub fn add_tx(&self, tx: GetTransactionResp) -> Result<()> {
        let key = TxKey {
            time: tx.time,
            lt: tx.lt,
            hash: tx.hash.as_ref().try_into()?,
        };

        let key = key.as_array();
        self.tree.insert(key, tx.transaction)?;

        Ok(())
    }

    pub fn notify(&self) {
        self.notify.notify_one();
    }

    async fn get_tx(
        &self,
        request: GetTx,
        mut tx: futures::channel::mpsc::Sender<GetTransactionResp>,
    ) -> Result<TxKey> {
        let (key_prefix, to_skip) = match request {
            GetTx::Key(k) => (k, true),
            GetTx::Resp(r) => (
                TxKey {
                    time: r.time,
                    lt: 0,
                    hash: [0; 32],
                },
                false,
            ),
        };

        let key_prefix = key_prefix.as_array();

        let mut iter = self.tree.prefix_iterator(key_prefix);
        if to_skip {
            iter.next();
        }
        let mut last = None;

        loop {
            {
                let (key, value) = match iter.item() {
                    Some(item) => item,
                    None => break iter.status()?,
                };
                let key = key.to_vec();
                let k = TxKey::try_from_slice(&key)?;
                let value = value.to_vec();
                let transaction = Bytes::from(value);

                let resp = GetTransactionResp {
                    time: k.time,
                    lt: k.lt,
                    hash: k.hash.to_vec().into(),
                    transaction,
                };
                last = Some(resp.clone());
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                println!("Sending tx with time {}", resp.time);
                tx.send(resp).await?;
            }

            iter.next();
        }

        #[allow(clippy::field_reassign_with_default)]
        let last = match last {
            Some(a) => a,
            None => {
                let mut resp = GetTransactionResp::default();
                resp.hash = vec![0; 32].into();
                resp
            }
        };

        let key = TxKey {
            time: last.time,
            lt: last.lt,
            hash: last.hash.as_ref().try_into()?,
        };

        Ok(key)
    }

    pub async fn stream_transactions(
        self: Arc<Self>,
        request: GetTransactionRequest,
    ) -> Result<impl Stream<Item = GetTransactionResp>> {
        let (tx, rx) = futures::channel::mpsc::channel(1);
        let this = self.clone();

        tokio::spawn(async move {
            let res = async move {
                let mut past_max_hash = this
                    .get_tx(GetTx::Resp(request), tx.clone())
                    .await
                    .context("Failed to get tx")?;

                let this = self.clone();
                loop {
                    self.notify.notified().await;
                    past_max_hash = this
                        .get_tx(GetTx::Key(past_max_hash), tx.clone())
                        .await
                        .context("Failed to get tx")?;
                }
            };
            let res: Result<()> = res.await;
            if let Err(e) = res {
                let e: anyhow::Error = e;
                log::error!("Failed to stream transactions: {}", e);
            }
        });

        Ok(rx)
    }
}

enum GetTx {
    Key(TxKey),
    Resp(GetTransactionRequest),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
struct TxKey {
    time: u32,
    lt: u64,
    hash: [u8; 32],
}

impl TxKey {
    pub fn as_array(&self) -> [u8; 44] {
        let mut res = [0; 44];
        res[..4].copy_from_slice(&self.time.to_be_bytes());
        res[4..12].copy_from_slice(&self.lt.to_be_bytes());
        res[12..].copy_from_slice(&self.hash);
        res
    }

    #[allow(clippy::field_reassign_with_default)]
    pub fn try_from_slice(slice: &[u8]) -> Result<Self> {
        if slice.len() != 44 {
            bail!("Invalid slice length");
        }
        let mut res = Self::default();
        res.time = u32::from_be_bytes(slice[..4].try_into()?);
        res.lt = u64::from_be_bytes(slice[4..12].try_into()?);
        res.hash.copy_from_slice(&slice[12..]);
        Ok(res)
    }
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `ton_block::BlockIdExt`
struct Transactions;
impl rocksdb_builder::Column for Transactions {
    const NAME: &'static str = "key_blocks";
}

#[cfg(test)]
mod test {
    use crate::grpc::tx_state::TxKey;

    #[test]
    fn test_encode_decode() {
        let key = TxKey {
            time: 1,
            lt: 2,
            hash: [255; 32],
        };

        let encoded = key.as_array();
        println!("{}", hex::encode(&encoded));
        let decoded = TxKey::try_from_slice(&encoded).unwrap();

        assert_eq!(key, decoded);

        let key2 = TxKey {
            time: 2,
            lt: 3,
            hash: [3; 32],
        };

        let encoded2 = key2.as_array();

        assert!(encoded < encoded2);
    }
}
