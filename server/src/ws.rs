use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex};
use ton_block::{Deserializable, HashmapAugType, MsgAddressInt};
use ton_types::{AccountId, HashmapType};

use crate::server::Server;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebSocketUpgradeQuery {
    client_id: uuid::Uuid,
}

pub async fn ws_router(
    ws: WebSocketUpgrade,
    State(ctx): State<Arc<Server>>,
    Query(query): Query<WebSocketUpgradeQuery>,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| handle_socket(query.client_id, ctx, socket))
}

async fn handle_socket(client_id: uuid::Uuid, state: Arc<Server>, socket: WebSocket) {
    const BUFFER_SIZE: usize = 100;
    let (tx, rx) = mpsc::channel(BUFFER_SIZE);

    let clients = &state.state().ws_producer.clients;
    clients
        .lock()
        .await
        .insert(client_id, (tx, AtomicUsize::default()));

    let (sender, mut receiver) = socket.split();

    start_listening_ws_events(client_id, sender, rx);

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Close(_)) | Err(_) => {
                tracing::warn!(%client_id, "websocket connection closed");
                clients.lock().await.remove(&client_id);
                break;
            }
            Ok(_) => {}
        }
    }
}

#[derive(Default)]
pub struct WsProducer {
    clients: Mutex<FxHashMap<uuid::Uuid, (mpsc::Sender<Message>, AtomicUsize)>>,
}

impl WsProducer {
    pub async fn handle_block(&self, workchain_id: i32, block: &ton_block::Block) -> Result<()> {
        let extra = block.read_extra()?;
        let account_blocks = extra.read_account_blocks()?;

        let mut accounts = Vec::with_capacity(account_blocks.len()?);
        account_blocks.iterate_with_keys(|account, value| {
            let mut lt = 0;
            value.transactions().iterate_slices(|_, mut value| {
                let tx_cell = value.checked_drain_reference()?;
                let tx = ton_block::Transaction::construct_from_cell(tx_cell)?;
                if lt < tx.logical_time() {
                    lt = tx.logical_time();
                }

                Ok(true)
            })?;

            let address =
                MsgAddressInt::with_standart(None, workchain_id as i8, AccountId::from(account))?;

            accounts.push(AccountInfo {
                account: nekoton_proto::utils::addr_to_bytes(&address).to_vec(),
                account_lt: lt,
            });

            Ok(true)
        })?;

        let mut clients = self.clients.lock().await;
        clients.retain(|client_id, (client_tx, msg_seqno)| {
            let seqno = msg_seqno.load(Ordering::Acquire);
            let message = match bincode::serialize(&WsMessage {
                seqno,
                payload: accounts.clone(),
            }) {
                Ok(message) => message,
                Err(e) => {
                    tracing::error!(%client_id, "failed to serialize ws message: {e}");
                    return false;
                }
            };

            msg_seqno.store(seqno + 1, Ordering::Release);

            let message = Message::Binary(message.clone());
            match client_tx.try_send(message) {
                Ok(_) => true,
                Err(e) => {
                    tracing::error!(%client_id, "failed to send ws message to channel: {e}");
                    false
                }
            }
        });

        Ok(())
    }
}

fn start_listening_ws_events(
    client_id: uuid::Uuid,
    mut ws_sender: SplitSink<WebSocket, Message>,
    mut events_rx: mpsc::Receiver<Message>,
) {
    tokio::spawn(async move {
        while let Some(message) = events_rx.recv().await {
            if let Err(e) = ws_sender.send(message).await {
                tracing::error!(%client_id, "failed to send message to ws client: {e}");
            }
        }

        if let Err(e) = ws_sender.close().await {
            tracing::error!(%client_id, "failed to close ws connection: {e}")
        }

        tracing::warn!(%client_id, "stopped listening for ws events");

        events_rx.close();
        while events_rx.recv().await.is_some() {}
    });
}

#[derive(Clone, Serialize, Deserialize)]
struct AccountInfo {
    pub account: Vec<u8>,
    pub account_lt: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct WsMessage {
    seqno: usize,
    payload: Vec<AccountInfo>,
}
