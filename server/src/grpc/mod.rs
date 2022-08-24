use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
mod clients_state;

use anyhow::Result;
use axum::async_trait;
use everscale_proto::pb;
pub use everscale_proto::pb::rpc_server::RpcServer;
use everscale_proto::pb::{
    Address, GetlastKeyBlockRequest, GetlastKeyBlockResponse, SendMessageRequest, StateRequest,
    StateResponse,
};
use futures::StreamExt;
use tokio::sync::Notify;
use ton_block::{Deserializable, MsgAddressInt};
use ton_types::FxDashMap;
use tonic::{Request, Response, Status, Streaming};

use crate::{JrpcState, QueryResult};

#[derive(Clone)]
pub struct GrpcServer {
    state: Arc<JrpcState>,
    clients_state: Arc<clients_state::ClientProgress>,
}

impl GrpcServer {
    pub fn new(state: Arc<JrpcState>) -> Self {
        Self {
            state,
            clients_state: clients_state::ClientProgress::new(),
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

    type GetBlockStream = futures::stream::BoxStream<
        'static,
        Result<everscale_proto::pb::GetBlockResponse, tonic::Status>,
    >;

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

    async fn register_client(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> std::result::Result<Response<pb::RegisterResponse>, Status> {
        let ttl = Duration::from_secs(request.into_inner().ttl as u64);
        let client_id = self.clients_state.next_id(ttl);

        Ok(Response::new(pb::RegisterResponse { client_id }))
    }

    type LeasePingStream =
        futures::stream::BoxStream<'static, Result<pb::LeasePingResponse, tonic::Status>>;

    async fn lease_ping(
        &self,
        request: Request<Streaming<pb::LeasePingRequest>>,
    ) -> std::result::Result<Response<Self::LeasePingStream>, Status> {
        let stream = request.into_inner();

        Ok(Response::new(self.clients_state.ping_pong(stream).boxed()))
    }
}
