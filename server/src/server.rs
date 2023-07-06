use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};

use crate::jrpc;
use crate::proto;
use crate::ServerState;

pub struct Server {
    state: Arc<ServerState>,
    jrpc: Arc<jrpc::JrpcServer>,
    proto: Arc<proto::ProtoServer>,
}

impl Server {
    pub fn new(state: Arc<ServerState>) -> Result<Arc<Self>> {
        let jrpc = jrpc::JrpcServer::new(state.clone())?;
        let proto = proto::ProtoServer::new(state.clone())?;

        Ok(Arc::new(Self { state, jrpc, proto }))
    }

    pub fn state(&self) -> &Arc<ServerState> {
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
            .layer(TimeoutLayer::new(Duration::from_secs(10)));

        #[cfg(feature = "compression")]
        let service = service.layer(tower_http::compression::CompressionLayer::new().gzip(true));

        // Prepare routes
        let router = axum::Router::new()
            .route("/", get(health_check))
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

async fn health_check() -> impl axum::response::IntoResponse {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis()
        .to_string()
}
