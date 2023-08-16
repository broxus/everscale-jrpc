use anyhow::Result;
use axum_core::extract::FromRequest;
use axum_core::response::{IntoResponse, Response};
use axum_core::{body, BoxError};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Request, StatusCode};
use http_body::{Body, Full};
use prost::bytes::Bytes;
use prost::Message;
use ton_types::UInt256;

use crate::rpc;
use crate::rpc::response::get_contract_state::contract_state;

pub struct Protobuf<T>(pub T);

#[async_trait::async_trait]
impl<S, B, T> FromRequest<S, B> for Protobuf<T>
where
    T: Message + Default,
    S: Send + Sync,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = StatusCode;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = match Bytes::from_request(req, state).await {
            Ok(b) => b,
            Err(err) => {
                tracing::warn!("Failed to read body: {}", err);
                return Err(StatusCode::BAD_REQUEST);
            }
        };
        let message = match T::decode(bytes) {
            Ok(m) => m,
            Err(err) => {
                tracing::warn!("Failed to decode protobuf request: {}", err);
                return Err(StatusCode::BAD_REQUEST);
            }
        };
        Ok(Protobuf(message))
    }
}

impl<T> IntoResponse for Protobuf<T>
where
    T: Message,
{
    fn into_response(self) -> Response {
        let buf = self.0.encode_to_vec();
        let mut res = Response::new(body::boxed(Full::from(buf)));
        res.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-protobuf"),
        );
        res
    }
}

pub enum ProtoAnswer {
    Result(rpc::Response),
    Error(rpc::Error),
}

impl ProtoAnswer {
    pub async fn parse_response(response: reqwest::Response) -> Result<Self> {
        let res = match response.status() {
            StatusCode::OK => Self::Result(rpc::Response::decode(response.bytes().await?)?),
            _ => Self::Error(rpc::Error::decode(response.bytes().await?)?),
        };

        Ok(res)
    }

    pub fn success(result: rpc::response::Result) -> Self {
        Self::Result(rpc::Response {
            result: Some(result),
        })
    }
}

impl IntoResponse for ProtoAnswer {
    fn into_response(self) -> Response {
        match self {
            Self::Result(res) => Protobuf(res).into_response(),
            Self::Error(e) => Protobuf(e).into_response(),
        }
    }
}

impl From<contract_state::GenTimings> for nekoton_abi::GenTimings {
    fn from(t: contract_state::GenTimings) -> Self {
        match t {
            contract_state::GenTimings::Known(known) => Self::Known {
                gen_lt: known.gen_lt,
                gen_utime: known.gen_utime,
            },
            contract_state::GenTimings::Unknown(()) => Self::Unknown,
        }
    }
}

impl From<nekoton_abi::GenTimings> for contract_state::GenTimings {
    fn from(t: nekoton_abi::GenTimings) -> Self {
        match t {
            nekoton_abi::GenTimings::Known { gen_lt, gen_utime } => {
                contract_state::GenTimings::Known(contract_state::Known { gen_lt, gen_utime })
            }
            nekoton_abi::GenTimings::Unknown => contract_state::GenTimings::Unknown(()),
        }
    }
}

impl From<contract_state::LastTransactionId> for nekoton_abi::LastTransactionId {
    fn from(t: contract_state::LastTransactionId) -> Self {
        match t {
            contract_state::LastTransactionId::Exact(contract_state::Exact { lt, hash }) => {
                Self::Exact(nekoton_abi::TransactionId {
                    lt,
                    hash: UInt256::from_slice(hash.as_ref()),
                })
            }
            contract_state::LastTransactionId::Inexact(contract_state::Inexact { latest_lt }) => {
                Self::Inexact { latest_lt }
            }
        }
    }
}

impl From<nekoton_abi::LastTransactionId> for contract_state::LastTransactionId {
    fn from(t: nekoton_abi::LastTransactionId) -> Self {
        match t {
            nekoton_abi::LastTransactionId::Exact(nekoton_abi::TransactionId { lt, hash }) => {
                Self::Exact(contract_state::Exact {
                    lt,
                    hash: Bytes::from(hash.into_vec()),
                })
            }
            nekoton_abi::LastTransactionId::Inexact { latest_lt } => {
                Self::Inexact(contract_state::Inexact { latest_lt })
            }
        }
    }
}
