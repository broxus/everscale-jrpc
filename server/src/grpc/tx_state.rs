use super::JrpcState;
use anyhow::{Context, Result};
use everscale_proto::pb;
use everscale_proto::pb::get_transaction_request::Ty;
use everscale_proto::pb::{GetTransactionRequest, GetTransactionResp};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use rocksdb_builder::Tree;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use ton_block::{GetRepresentationHash, Serializable};
use tonic::async_trait;

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
            Ok(stream) => stream.map(|tx| Ok(tx)),
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
        let db = rocksdb_builder::DbBuilder::new(file_path)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // io
                opts.set_max_open_files(limit as i32);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                opts.set_max_background_jobs(std::cmp::max(
                    (std::thread::available_parallelism()? as i32) / 2,
                    2,
                ));
                opts.increase_parallelism(std::thread::available_parallelism()? as i32);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .column::<Transactions>()
            .build()
            .context("Failed building db")?;
    }

    pub fn add_tx(&self, tx: GetTransactionResp) -> Result<()> {
        let key = TxKey {
            time: tx.time,
            lt: tx.lt,
            hash: tx.hash.try_into()?,
        };

        let key = bincode::encode_to_vec(key, bincode::config::standard())?;
        self.tree.insert(key, tx.transaction)?;

        Ok(())
    }

    pub fn notify(&self) {
        self.notify.notify_one();
    }

    pub fn get_tx(&self, request: GetTransactionRequest) -> Result<Vec<GetTransactionResp>> {
        let db = self.sqllite.lock();

        let res = match request.ty {
            Some(Ty::Hash(hash)) => {
                let (lt, time): (u64, u32) = db.query_row(
                    "SELECT lt,time FROM ever_transactions WHERE hash = ?",
                    rusqlite::params![hash.as_ref()],
                    |row| Ok((row.get_unwrap(0), row.get_unwrap(1))),
                )?;
                // language=SQLite
                let mut stmt = db. prepare_cached("SELECT hash, data, lt, time FROM ever_transactions WHERE lt > ? AND time > ? ORDER BY time, lt")?;
                let response = stmt.query(rusqlite::params![lt, time])?;
                self.fetch_transactions(response)
            }
            Some(Ty::Time(time)) => {
                // language=SQLite
                let mut stmt = db. prepare_cached(
                    "SELECT hash, data, lt, time FROM ever_transactions WHERE time > ? ORDER BY time, lt",
                )?;
                let response = stmt.query([time])?;
                self.fetch_transactions(response)
            }
            None => Ok(Vec::new()),
        };

        log::debug!(
            "Fetched {} transactions",
            res.as_ref().map(|v| v.len()).unwrap_or(0)
        );
        res
    }

    pub async fn stream_transactions(
        self: Arc<Self>,
        request: GetTransactionRequest,
    ) -> Result<impl Stream<Item = GetTransactionResp>> {
        let (mut tx, rx) = futures::channel::mpsc::channel(1);
        let this = self.clone();
        let past_transactions = tokio::task::spawn_blocking(move || this.get_tx(request))
            .await?
            .context("Failed to get past transactions")?;

        tokio::spawn(async move {
            let res = async move {
                let mut past_transactions_max_hash = past_transactions
                    .iter()
                    .last()
                    .context("No past transactions")?
                    .hash
                    .clone();

                futures::stream::iter(past_transactions)
                    .map(Ok)
                    .forward(&mut tx)
                    .await?;

                let this = self.clone();
                loop {
                    self.notify.notified().await;
                    let this = this.clone();
                    let past_transactions = tokio::task::spawn_blocking(move || {
                        this.get_tx(GetTransactionRequest {
                            ty: Some(Ty::Hash(past_transactions_max_hash.clone())),
                        })
                    })
                    .await??;
                    past_transactions_max_hash = past_transactions
                        .iter()
                        .last()
                        .context("No past transactions")?
                        .hash
                        .clone();

                    futures::stream::iter(past_transactions)
                        .map(Ok)
                        .forward(&mut tx)
                        .await?;
                }
                Ok(())
            };
            if let Err(e) = res.await {
                let e: anyhow::Error = e;
                log::error!("Failed to stream transactions: {}", e);
            }
        });

        Ok(rx)
    }

    fn fetch_transactions(&self, mut rows: Rows) -> Result<Vec<GetTransactionResp>> {
        let mut res = Vec::new();
        while let Some(row) = rows.next()? {
            let hash: Vec<u8> = row.get_unwrap(0);
            let data: Vec<u8> = row.get_unwrap(1);
            let lt: u64 = row.get_unwrap(2);
            let time: u32 = row.get_unwrap(3);

            let resp = GetTransactionResp {
                hash: hash.into(),
                lt,
                time,
                transaction: data.into(),
            };
            res.push(resp);
        }

        Ok(res)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TxKey {
    time: u32,
    lt: u64,
    hash: [u8; 32],
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `ton_block::BlockIdExt`
struct Transactions;
impl rocksdb_builder::Column for Transactions {
    const NAME: &'static str = "key_blocks";
}
