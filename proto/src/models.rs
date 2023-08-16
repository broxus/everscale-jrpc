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
use crate::rpc::response::get_contract_state::exists::{Exact, Inexact, LastTransactionId};
use crate::rpc::response::get_contract_state::not_exist::GenTimings;
use crate::rpc::response::get_contract_state::{NotExist, Timings};

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

impl From<GenTimings> for nekoton_abi::GenTimings {
    fn from(t: GenTimings) -> Self {
        match t {
            GenTimings::Known(known) => Self::Known {
                gen_lt: known.gen_lt,
                gen_utime: known.gen_utime,
            },
            GenTimings::Unknown(()) => Self::Unknown,
        }
    }
}

impl From<nekoton_abi::GenTimings> for GenTimings {
    fn from(t: nekoton_abi::GenTimings) -> Self {
        match t {
            nekoton_abi::GenTimings::Known { gen_lt, gen_utime } => {
                GenTimings::Known(Timings { gen_lt, gen_utime })
            }
            nekoton_abi::GenTimings::Unknown => GenTimings::Unknown(()),
        }
    }
}

impl From<nekoton_abi::GenTimings> for NotExist {
    fn from(t: nekoton_abi::GenTimings) -> Self {
        let get_timings = match t {
            nekoton_abi::GenTimings::Known { gen_lt, gen_utime } => {
                GenTimings::Known(Timings { gen_lt, gen_utime })
            }
            nekoton_abi::GenTimings::Unknown => GenTimings::Unknown(()),
        };

        Self {
            gen_timings: Some(get_timings),
        }
    }
}

impl From<Timings> for NotExist {
    fn from(t: Timings) -> Self {
        Self {
            gen_timings: Some(GenTimings::Known(Timings {
                gen_lt: t.gen_lt,
                gen_utime: t.gen_utime,
            })),
        }
    }
}

impl From<Timings> for nekoton_abi::GenTimings {
    fn from(t: Timings) -> Self {
        Self::Known {
            gen_lt: t.gen_lt,
            gen_utime: t.gen_utime,
        }
    }
}

impl From<LastTransactionId> for nekoton_abi::LastTransactionId {
    fn from(t: LastTransactionId) -> Self {
        match t {
            LastTransactionId::Exact(Exact { lt, hash }) => {
                Self::Exact(nekoton_abi::TransactionId {
                    lt,
                    hash: UInt256::from_slice(hash.as_ref()),
                })
            }
            LastTransactionId::Inexact(Inexact { latest_lt }) => Self::Inexact { latest_lt },
        }
    }
}

impl From<nekoton_abi::LastTransactionId> for LastTransactionId {
    fn from(t: nekoton_abi::LastTransactionId) -> Self {
        match t {
            nekoton_abi::LastTransactionId::Exact(nekoton_abi::TransactionId { lt, hash }) => {
                Self::Exact(Exact {
                    lt,
                    hash: Bytes::from(hash.into_vec()),
                })
            }
            nekoton_abi::LastTransactionId::Inexact { latest_lt } => {
                Self::Inexact(Inexact { latest_lt })
            }
        }
    }
}
