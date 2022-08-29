use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::{atomic, Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Bytes;
use everscale_proto::pb;
use futures::{SinkExt, Stream, StreamExt};
use tokio::sync::Notify;
use ton_block::Block;
use ton_types::FxDashMap;

type ClientID = u32;

#[derive(Default)]
pub(crate) struct ClientProgress {
    next_client_id: AtomicU32,
    user_commits: Mutex<Vec<Committed>>,
    shard_notifies: FxDashMap<u64, Arc<ShardNotify>>,
    live_clients: FxDashMap<ClientID, Arc<Mutex<ClientRecord>>>,
    client_queues: FxDashMap<ClientID, Arc<Mutex<ClientQueue>>>,
}

impl ClientProgress {
    pub fn new() -> Arc<Self> {
        let this = Self {
            ..Default::default()
        };
        let this = Arc::new(this);
        this.run_gc_task();

        this
    }

    pub fn next_id(&self, client_life_span: Duration) -> ClientID {
        let id = self.next_client_id.fetch_add(1, atomic::Ordering::Acquire);
        self.live_clients
            .insert(id, ClientRecord::new(client_life_span));
        id
    }

    pub fn ping_pong(
        self: &Arc<Self>,
        mut client_stream: impl Stream<Item = Result<pb::LeasePingRequest, tonic::Status>>
            + Unpin
            + Send
            + 'static,
    ) -> impl Stream<Item = Result<pb::LeasePingResponse, tonic::Status>> {
        let this = self.clone();

        let (mut tx, rx) = futures::channel::mpsc::channel(1);
        tokio::spawn(async move {
            while let Some(message) = client_stream.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        log::error!("Error in client stream: {:?}", err);
                        tx.send(Err(err)).await.ok();
                        return;
                    }
                };
                let client_id = message.lease_id;

                if this.ping_client(&client_id).is_none() {
                    log::error!("Client {} is gone", client_id);
                    tx.send(Err(tonic::Status::not_found("Client is gone")))
                        .await
                        .ok();
                    return;
                } else {
                    if let Err(e) = tx
                        .send(Ok(pb::LeasePingResponse { is_success: true }))
                        .await
                    {
                        log::error!("Failed to send ping answer: {}", e);
                        return;
                    }
                }
            }
        });

        rx
    }

    pub fn commit_block(&self, client_id: ClientID, seq_no: u32, shard: u64) -> Result<()> {
        let client = self
            .live_clients
            .get(&client_id)
            .map(|x| x.clone())
            .context("Client not found")?;

        {
            let mut client = client.lock().unwrap();
            client.commit(shard, seq_no);
        }

        self.shard_notifies
            .entry(shard)
            .or_insert_with(|| ShardNotify::new())
            .commit(seq_no);

        Ok(())
    }

    fn ping_client(&self, client_id: &ClientID) -> Option<()> {
        let client = self.live_clients.get(client_id)?.value().clone();
        {
            let mut client = client.lock().unwrap();
            client.ping();
        }

        Some(())
    }

    fn run_gc_task(self: &Arc<Self>) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                this.live_clients.retain(|_, client| {
                    let client = client.lock().unwrap();
                    client.is_alive()
                });
            }
        });
    }

    pub fn fanout_clients(&self, block: pb::GetBlockResponse) {}
}

struct ClientRecord {
    deadline_at: Instant,
    committed: Committed,
    lease_duration: Duration,
}

#[derive(Default)]
struct Committed {
    map: HashMap<u64, u32>,
}

impl ClientRecord {
    pub fn new(lease_duration: Duration) -> Arc<Mutex<Self>> {
        let deadline_at = Instant::now() + lease_duration;
        Arc::new(Mutex::new(Self {
            deadline_at,
            committed: Default::default(),
            lease_duration,
        }))
    }

    fn ping(&mut self) {
        self.deadline_at = Instant::now() + Duration::from_secs(10);
    }

    fn is_alive(&self) -> bool {
        self.deadline_at > Instant::now()
    }

    fn commit(&mut self, shard: u64, seq_no: u32) {
        self.committed.map.insert(shard, seq_no);
    }
}

#[derive(Default)]
struct ShardNotify {
    notify: Arc<Notify>,
    last_block_id: AtomicU32,
}

impl ShardNotify {
    fn new() -> Arc<Self> {
        let notify = Arc::new(Notify::new());
        notify.notify_one();

        Arc::new(Self {
            notify,
            last_block_id: AtomicU32::new(0),
        })
    }

    async fn wait_for_commit(&self, want_to_commit_block: u32) {
        loop {
            self.notify.notified().await;
            if self.last_block_id.load(atomic::Ordering::Acquire) >= want_to_commit_block {
                return;
            }
        }
    }

    fn commit(&self, block_id: u32) {
        let update_result = self.last_block_id.fetch_update(
            atomic::Ordering::AcqRel,
            atomic::Ordering::Acquire,
            |x| {
                if x < block_id {
                    Some(block_id)
                } else {
                    None
                }
            },
        );

        if update_result.is_ok() {
            self.notify.notify_one()
        };
    }
}

struct ClientQueue {
    queue: Mutex<VecDeque<pb::GetBlockResponse>>,
}
