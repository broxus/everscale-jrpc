pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

use bytes::Bytes;
use ton_types::UInt256;

pub use prost;

use crate::rpc::response::get_contract_state::contract_state;

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

pub mod models {
    use axum::body::{Full, HttpBody};
    use axum::extract::FromRequest;
    use axum::http::{HeaderValue, Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::{body, BoxError};
    use bytes::Bytes;
    use prost::Message;

    pub struct Protobuf<T>(pub T);

    #[axum::async_trait]
    impl<S, B, T> FromRequest<S, B> for Protobuf<T>
    where
        T: Message + Default,
        S: Send + Sync,
        B: HttpBody + Send + 'static,
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
        fn into_response(self) -> axum::response::Response {
            let buf = self.0.encode_to_vec();
            let mut res = axum::response::Response::new(body::boxed(Full::from(buf)));
            res.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-protobuf"),
            );
            res
        }
    }
}
