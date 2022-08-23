use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::async_trait;
use everscale_proto::pb;
pub use everscale_proto::pb::rpc_server::RpcServer;
use everscale_proto::pb::{
    Address, GetlastKeyBlockRequest, GetlastKeyBlockResponse, SendMessageRequest, StateRequest,
    StateResponse,
};
use tokio::sync::Notify;
use ton_block::{Deserializable, MsgAddressInt};
use ton_types::FxDashMap;
use tonic::{Request, Response, Status, Streaming};

use crate::{JrpcState, QueryResult};

#[derive(Clone)]
pub struct GrpcServer {
    state: Arc<JrpcState>,
    clients_counter: Arc<AtomicU32>,
    shards_commit_map: Arc<ShardsCommitMap>,
}

impl GrpcServer {
    pub fn new(state: Arc<JrpcState>) -> Self {
        Self {
            state,
            clients_counter: Default::default(),
            shards_commit_map: Arc::new(ShardsCommitMap::new()),
        }
    }
}

#[async_trait]
impl everscale_proto::pb::rpc_server::Rpc for GrpcServer {
    async fn state(
        &self,
        request: tonic::Request<StateRequest>,
    ) -> Result<tonic::Response<StateResponse>, tonic::Status> {
        let request = request.into_inner();
        let address = match request.address {
            None => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    "Address is required",
                ))
            }
            Some(a) => a,
        };

        let address_bytes: [u8; 32] = match address.address.try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    "Address is invalid",
                ))
            }
        };
        let workchain_id = match address.workchain.try_into() {
            Ok(a) => a,
            Err(_) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    "Workchain is invalid",
                ))
            }
        };
        let address = match MsgAddressInt::with_standart(None, workchain_id, address_bytes.into()) {
            Ok(a) => a,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    e.to_string(),
                ))
            }
        };

        let state = match self.state.get_contract_state_grpc(&address) {
            Ok(s) => s,
            Err(e) => return Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        };

        Ok(tonic::Response::new(state))
    }

    async fn getlast_key_block(
        &self,
        _request: tonic::Request<GetlastKeyBlockRequest>,
    ) -> Result<tonic::Response<GetlastKeyBlockResponse>, tonic::Status> {
        let key_block = match self.state.get_last_key_block_grpc() {
            Ok(s) => s,
            Err(e) => return Err(tonic::Status::new(tonic::Code::Internal, e.to_string())),
        };

        Ok(tonic::Response::new(key_block))
    }

    type SendMessageStream = futures::stream::BoxStream<
        'static,
        Result<everscale_proto::pb::SendMessageResponse, tonic::Status>,
    >;

    async fn send_message(
        &self,
        request: tonic::Request<SendMessageRequest>,
    ) -> Result<tonic::Response<Self::SendMessageStream>, tonic::Status> {
        let mut request = request.into_inner();
        let message = match ton_types::deserialize_tree_of_cells(&mut &*request.message) {
            Ok(m) => m,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    e.to_string(),
                ))
            }
        };
        let message = match ton_block::Message::construct_from_cell(message) {
            Ok(m) => m,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    e.to_string(),
                ))
            }
        };

        todo!()
    }

    type GetBlockStream = ();

    async fn get_block(
        &self,
        request: Request<Streaming<pb::GetBlockRequest>>,
    ) -> Result<Response<Self::GetBlockStream>, Status> {
        todo!()
    }

    async fn commit_block(
        &self,
        request: Request<pb::CommitBlockRequest>,
    ) -> Result<Response<pb::CommitBlockResponse>, Status> {
        todo!()
    }
}

type ClientID = u32;

struct Committed {
    client_id: ClientID,
    block_id: u32,
    client_lease_deadline: Instant,
    notify: Arc<Notify>,
}

#[derive(Default)]
struct ShardsCommitMap {
    shards_commit_map: Mutex<HashMap<u64, Committed>>,
}

impl ShardsCommitMap {
    fn new() -> Self {
        Self {
            shards_commit_map: Default::default(),
        }
    }

    async fn commit(&self, client_id: ClientID, block_id: u32, shard_id: u64) {
        let mut map = self.shards_commit_map.lock().unwrap();

        let mut commited = map.entry(shard_id).or_insert_with(|| Committed {
            client_id,
            block_id,
            client_lease_deadline: Instant::now() + Duration::from_secs(20), //todo: configurable?
            notify: Arc::new(Notify::new()),
        });
        if block_id + 1 + 1 >= commited.block_id {
            commited.notify.notify_one();
            return;
        }
    }

    async fn wait_for_commit(&self, shard: u64) -> Result<()> {
        let committed = {
            let mut map = self.shards_commit_map.lock().unwrap();

            match map.get(&shard).map(|x| x.notify.clone()) {
                None => return Ok(()),
                Some(w) => w,
            }
        };
        committed.notified().await;

        Ok(())
    }
}
