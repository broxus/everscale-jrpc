use std::time::Duration;

use anyhow::{Context, Result};
use everscale_jrpc_models::BlockResponse;
use everscale_proto::pb;
use everscale_proto::pb::rpc_client::RpcClient;
use everscale_proto::pb::state_response::GenTimings;
use everscale_proto::pb::{LeasePingResponse, StateRequest};
use futures::{SinkExt, TryStream, TryStreamExt};
use nekoton::transport::models::ExistingContract;
use reqwest::Url;
use tokio_util::sync::CancellationToken;
use ton_block::{AccountStuff, Deserializable, MaybeDeserialize, MsgAddressInt};
use ton_types::UInt256;
use tonic::transport::{Channel, Uri};
use tonic::{Response, Status, Streaming};

use crate::JrpcClientOptions;

struct GrpcClient {
    client: RpcClient<Channel>,
}

impl GrpcClient {
    /// [endpoints] full URLs of the RPC endpoints.
    pub async fn new(endpoint: Uri) -> Result<GrpcClient> {
        let load_balanced_client = Channel::builder(endpoint)
            .connect()
            .await
            .context("Failed to connect to the RPC server")?;
        let client = RpcClient::new(load_balanced_client);

        Ok(Self { client })
    }

    pub async fn get_contract_state(
        &self,
        address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        let mut client = self.client.clone();
        let address = pb::Address {
            workchain: address.workchain_id(),
            address: address.address().get_bytestring(0).into(),
        };

        let response = client
            .state(StateRequest {
                address: Some(address.into()),
            })
            .await?;

        let response = response.into_inner();
        let result = match response.contract_state {
            Some(result) => {
                let (timings, gen_lt) = match result.gen_timings {
                    None => (nekoton_abi::GenTimings::Unknown, 0),
                    Some(t) => (
                        nekoton_abi::GenTimings::Known {
                            gen_lt: t.gen_lt,
                            gen_utime: t.gen_utime,
                        },
                        t.gen_lt,
                    ),
                };

                let last_transaction_id = match result.last_transaction_id {
                    None => nekoton_abi::LastTransactionId::Inexact { latest_lt: gen_lt },
                    Some(t) => nekoton_abi::LastTransactionId::Exact(nekoton_abi::TransactionId {
                        // out grpc implementation always returns exact value
                        lt: t.lt,
                        hash: ton_types::UInt256::from_slice(&t.hash),
                    }),
                };

                let account_stuff =
                    ton_types::deserialize_tree_of_cells(&mut result.account.as_ref())
                        .and_then(|cell| {
                            let slice = &mut cell.into();
                            Ok(ton_block::AccountStuff {
                                addr: Deserializable::construct_from(slice)?,
                                storage_stat: Deserializable::construct_from(slice)?,
                                storage: ton_block::AccountStorage {
                                    last_trans_lt: Deserializable::construct_from(slice)?,
                                    balance: Deserializable::construct_from(slice)?,
                                    state: Deserializable::construct_from(slice)?,
                                    init_code_hash: if slice.remaining_bits() > 0 {
                                        UInt256::read_maybe_from(slice)?
                                    } else {
                                        None
                                    },
                                },
                            })
                        })
                        .context("failed to deserialize account stuff")?;

                ExistingContract {
                    account: account_stuff,
                    timings,
                    last_transaction_id,
                }
            }
            None => {
                return Ok(None);
            }
        };

        Ok(Some(result))
    }

    pub async fn run_local(
        &self,
        address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton::abi::ExecutionOutput>> {
        use nekoton::abi::FunctionExt;
        use nekoton::utils::SimpleClock;

        let state = match self.get_contract_state(address).await? {
            Some(a) => a,
            None => return Ok(None),
        };

        function
            .clone()
            .run_local(&SimpleClock, state.account, input)
            .map(Some)
    }

    pub async fn get_latest_key_block(&self) -> Result<BlockResponse> {
        let mut client = self.client.clone();

        let response = client
            .getlast_key_block(pb::GetlastKeyBlockRequest {})
            .await?;

        Ok(BlockResponse {
            block: ton_block::Block::construct_from_bytes(&response.into_inner().key_block)
                .context("Failed to deserialize block")?,
        })
    }

    pub async fn block_stream(
        &self,
        lease_duration: Duration,
    ) -> Result<impl TryStream<Ok = ton_block::Block, Error = tonic::Status>> {
        use futures::stream::{Stream, TryStreamExt};

        let mut client = self.client.clone();

        let cancel_token = CancellationToken::new();

        let ping_token = cancel_token.clone();
        let client_id = client
            .register_client(pb::RegisterRequest {
                ttl: lease_duration.as_secs() as u32,
            })
            .await?
            .into_inner()
            .client_id;

        self.spawn_lease_job(lease_duration, ping_token, client_id);

        let mut stream = client
            .block_stream(pb::GetBlockRequest {
                lease_id: client_id,
            })
            .await?
            .into_inner();
        let stream =
            stream.map_ok(|block| ton_block::Block::construct_from_bytes(&block.block).unwrap());

        Ok(stream)
    }

    fn spawn_lease_job(
        &self,
        lease_duration: Duration,
        ping_token: CancellationToken,
        client_id: u32,
    ) {
        let mut client: RpcClient<Channel> = self.client.clone();

        tokio::spawn(async move {
            let (mut tx, rx) = futures::channel::mpsc::channel(1);

            // ping stream producer
            // will die if rx is dropped
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(lease_duration / 2);
                loop {
                    let res = tx
                        .send(pb::LeasePingRequest {
                            lease_id: client_id,
                        })
                        .await;
                    if let Err(e) = res {
                        log::error!("Failed to produce ping: {}", e);
                        break;
                    }
                    interval.tick().await;
                }
            });

            tokio::spawn(async move {
                let ping_token_clone = ping_token.clone();
                let fut = async move {
                    let stream = client.lease_ping(rx).await;
                    let mut stream = match stream {
                        Ok(s) => s.into_inner(),
                        Err(e) => {
                            log::error!("Failed to create ping stream: {}", e);
                            ping_token.cancel();
                            return;
                        }
                    };
                    while let Ok(Some(response)) = stream.message().await {
                        if !response.is_success {
                            log::error!("Ping stream failed.");
                            ping_token.cancel();
                            return;
                        }
                    }
                    ping_token.cancel();
                };

                tokio::select! {
                    _ = fut => {},
                    _ = ping_token_clone.cancelled() => {
                        log::warn!("Ping stream canceled.");
                    },
                }
            });
        });
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_key_block() {
        let client = super::GrpcClient::new("http://localhost:8081".parse().unwrap())
            .await
            .unwrap();

        client.get_latest_key_block().await.unwrap();
    }

    #[tokio::test]
    async fn test_contract_state() {
        let client = super::GrpcClient::new("http://localhost:8081".parse().unwrap())
            .await
            .unwrap();

        let address = ton_block::MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )
        .unwrap();
        client.get_contract_state(&address).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_lease() {
        use futures::TryStreamExt;
        env_logger::builder()
            .is_test(true)
            .filter(Some("everscale_jrpc_client"), log::LevelFilter::Debug)
            .try_init()
            .unwrap();
        let client = super::GrpcClient::new("http://localhost:8081".parse().unwrap())
            .await
            .unwrap();
        let mut stream = client.block_stream(Duration::from_secs(1)).await.unwrap();

        stream
            .try_for_each(|block| async move {
                println!("{:?}", block);
                Ok(())
            })
            .await
            .unwrap();
    }
}
