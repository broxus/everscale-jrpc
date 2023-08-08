use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::RequestExt;

use crate::jrpc;
use crate::proto;
use crate::RpcState;

pub struct Server {
    state: Arc<RpcState>,
    jrpc: Arc<jrpc::JrpcServer>,
    proto: Arc<proto::ProtoServer>,
}

impl Server {
    pub fn new(state: Arc<RpcState>) -> Result<Arc<Self>> {
        let jrpc = jrpc::JrpcServer::new(state.clone())?;
        let proto = proto::ProtoServer::new(state.clone())?;

        Ok(Arc::new(Self { state, jrpc, proto }))
    }

    pub fn state(&self) -> &Arc<RpcState> {
        &self.state
    }

    pub fn jrpc(&self) -> &Arc<jrpc::JrpcServer> {
        &self.jrpc
    }

    pub fn proto(&self) -> &Arc<proto::ProtoServer> {
        &self.proto
    }

    pub fn serve(self: Arc<Self>) -> Result<impl Future<Output = ()> + Send + 'static> {
        use tower::ServiceBuilder;
        use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
        use tower_http::timeout::TimeoutLayer;

        let listen_address = self.state.config.listen_address;

        // Prepare middleware
        let service = ServiceBuilder::new()
            .layer(DefaultBodyLimit::max(MAX_REQUEST_SIZE))
            .layer(
                CorsLayer::new()
                    .allow_headers(AllowHeaders::list([
                        axum::http::header::AUTHORIZATION,
                        axum::http::header::CONTENT_TYPE,
                    ]))
                    .allow_origin(AllowOrigin::any())
                    .allow_methods(AllowMethods::list([
                        axum::http::Method::GET,
                        axum::http::Method::POST,
                        axum::http::Method::OPTIONS,
                        axum::http::Method::PUT,
                    ])),
            )
            .layer(TimeoutLayer::new(Duration::from_secs(25)));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        // Prepare routes
        let router = axum::Router::new()
            .route("/", get(health_check))
            .route("/", post(common_route))
            .route("/rpc", post(jrpc::jrpc_router))
            .route("/proto", post(proto::proto_router))
            .layer(service)
            .with_state(self);

        // Start server
        let future = axum::Server::try_bind(&listen_address)?
            .http2_adaptive_window(true)
            .tcp_keepalive(Duration::from_secs(60).into())
            .serve(router.into_make_service());
        Ok(async move { future.await.unwrap() })
    }
}

const MAX_REQUEST_SIZE: usize = 2 << 17; //256kb

async fn common_route(
    State(ctx): State<Arc<Server>>,
    req: Request<Body>,
) -> axum::response::Response {
    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());

    if let Some(content_type) = content_type {
        if content_type.starts_with("application/json") {
            let res = match req.extract().await {
                Ok(req) => jrpc::jrpc_router(State(ctx), req).await,
                Err(_) => StatusCode::BAD_REQUEST.into_response(),
            };
            return res;
        }

        if content_type.starts_with("application/x-protobuf") {
            let res = match req.extract().await {
                Ok(req) => proto::proto_router(State(ctx), req).await,
                Err(_) => StatusCode::BAD_REQUEST.into_response(),
            };
            return res;
        }
    }

    StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response()
}

async fn health_check() -> impl axum::response::IntoResponse {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis()
        .to_string()
}
