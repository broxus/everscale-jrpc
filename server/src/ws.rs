use std::sync::Arc;

use anyhow::Result;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use ton_block::{Deserializable, HashmapAugType};
use ton_types::HashmapType;

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
    let (sender, mut receiver) = socket.split();

    let clients = &state.state().ws_producer.clients;
    clients.lock().await.insert(client_id, sender);

    while let Some(msg) = receiver.next().await {
        match msg {
            Ok(Message::Close(_)) | Err(_) => {
                tracing::warn!(%client_id, "websocket connection closed");
                clients.lock().await.remove(&client_id);
                return;
            }
            Ok(_) => {}
        }
    }
}

#[derive(Default)]
pub struct WsProducer {
    clients: Mutex<FxHashMap<uuid::Uuid, SplitSink<WebSocket, axum::extract::ws::Message>>>,
}

impl WsProducer {
    pub async fn handle_block(&self, block: &ton_block::Block) -> Result<()> {
        let extra = block.read_extra()?;
        let account_blocks = extra.read_account_blocks()?;

        let mut accounts = FxHashMap::default();
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
            accounts.insert(account.into_vec(), lt);

            Ok(true)
        })?;

        let message = bincode::serialize(&accounts)?;
        let mut clients = self.clients.lock().await;
        for (_, client) in clients.iter_mut() {
            let message = axum::extract::ws::Message::Binary(message.clone());
            client.send(message).await?;
        }

        Ok(())
    }
}
