use std::sync::Arc;

use anyhow::Result;
use axum::async_trait;
use futures::stream::BoxStream;
use ton_block::{Deserializable, MsgAddressInt};
use ton_indexer::utils::{BlockStuff, ShardStateStuff};

use tonic::{Request, Response, Status};

use everscale_proto::pb;
use pb::rpc_server::Rpc;
pub use pb::rpc_server::RpcServer;
pub use pb::stream_server::StreamServer;
use pb::{
    GetTransactionRequest, GetlastKeyBlockRequest, GetlastKeyBlockResponse, SendMessageRequest,
    StateRequest, StateResponse,
};

use crate::JrpcState;

mod models;

#[cfg(feature = "streaming")]
mod tx_state;

pub struct Builder {
    state: Arc<JrpcState>,
}

impl Builder {
    pub fn new(state: Arc<JrpcState>) -> Result<Self> {
        Ok(Self { state })
    }

    #[cfg(feature = "streaming")]
    pub fn build_with_streaming<P: AsRef<std::path::Path>>(self, path: P) -> Result<GrpcService> {
        use ton_block::HashmapAugType;
        use ton_types::HashmapType;
        use tonic::codegen::Bytes;

        let stream = tx_state::StreamService::new(self.state.clone(), path)?;

        Ok(GrpcService {
            rpc: RpcService { state: self.state },
            stream,
        })
    }

    #[cfg(not(feature = "streaming"))]
    pub fn build(self) -> GrpcService {
        GrpcService {
            rpc: RpcService { state: self.state },
        }
    }
}

#[derive(Clone)]
pub struct GrpcService {
    rpc: RpcService,
    #[cfg(feature = "streaming")]
    stream: tx_state::StreamService,
}

#[async_trait]
impl Rpc for GrpcService {
    async fn state(
        &self,
        request: Request<StateRequest>,
    ) -> std::result::Result<Response<StateResponse>, Status> {
        self.rpc.state(request).await
    }

    async fn getlast_key_block(
        &self,
        request: Request<GetlastKeyBlockRequest>,
    ) -> std::result::Result<Response<GetlastKeyBlockResponse>, Status> {
        self.rpc.getlast_key_block(request).await
    }

    type SendMessageStream = <RpcService as Rpc>::SendMessageStream;

    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> std::result::Result<Response<Self::SendMessageStream>, Status> {
        self.rpc.send_message(request).await
    }
}

impl GrpcService {
    pub async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        #[cfg(feature = "streaming")]
        {
            use axum::body::Bytes;
            use ton_block::GetRepresentationHash;
            use ton_block::HashmapAugType;
            use ton_types::HashmapType;

            let mut records = Vec::new();
            let block_extra = block_stuff.block().read_extra()?;

            block_extra
                .read_account_blocks()?
                .iterate_objects(|account_block| {
                    account_block
                        .transactions()
                        .iterate_slices(|_, raw_transaction| {
                            let cell = raw_transaction.reference(0)?;
                            let boc: Bytes = ton_types::serialize_toc(&cell)?.into();
                            let transaction =
                                ton_block::Transaction::construct_from(&mut cell.into())?;
                            match transaction.description.read_struct()? {
                                ton_block::TransactionDescr::Ordinary(_) => {}
                                _ => return Ok(true),
                            };

                            records.push(pb::GetTransactionResp {
                                hash: transaction.hash()?.into_vec().into(),
                                time: transaction.now,
                                lt: transaction.lt,
                                transaction: boc,
                            });
                            Ok(true)
                        })?;
                    Ok(true)
                })?;

            let is_empty = records.is_empty();
            for tx in records {
                self.stream.tx_state.add_tx(tx)?;
            }

            if !is_empty {
                self.stream.tx_state.notify();
            }
        }

        if let Some(shard_state) = shard_state {
            self.rpc.state.handle_block(block_stuff, shard_state)?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct RpcService {
    state: Arc<JrpcState>,
}

#[async_trait]
impl Rpc for RpcService {
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

        let address_bytes: [u8; 32] = match address.address.as_ref().try_into() {
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

    type SendMessageStream = BoxStream<'static, Result<pb::SendMessageResponse, tonic::Status>>;

    async fn send_message(
        &self,
        request: tonic::Request<SendMessageRequest>,
    ) -> Result<tonic::Response<Self::SendMessageStream>, tonic::Status> {
        let request = request.into_inner();
        let message = match ton_types::deserialize_tree_of_cells(&mut &*request.message) {
            Ok(m) => m,
            Err(e) => {
                return Err(tonic::Status::new(
                    tonic::Code::InvalidArgument,
                    e.to_string(),
                ))
            }
        };
        let _message = match ton_block::Message::construct_from_cell(message) {
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
}

#[cfg(feature = "streaming")]
#[async_trait]
impl pb::stream_server::Stream for GrpcService {
    type GetTransactionStream =
        <tx_state::StreamService as pb::stream_server::Stream>::GetTransactionStream;

    async fn get_transaction(
        &self,
        request: Request<GetTransactionRequest>,
    ) -> std::result::Result<Response<Self::GetTransactionStream>, Status> {
        self.stream.get_transaction(request).await
    }
}
