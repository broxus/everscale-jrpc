pub mod rpc {
    include!(concat!(env!("OUT_DIR"), "/rpc.rs"));
}

pub use prost;

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
