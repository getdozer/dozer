pub mod auth;
mod client_server;
pub mod common;
pub mod health;
pub mod internal;
// pub mod dynamic;
mod auth_middleware;
mod grpc_web_middleware;
mod metric_middleware;
mod shared_impl;
pub mod typed;
pub mod types_helper;

use bytes::Bytes;
pub use client_server::ApiServer;
use dozer_types::errors::internal::BoxedError;
use dozer_types::tonic::transport::server::{Router, Routes, TcpIncoming};
use futures_util::{
    stream::{AbortHandle, Abortable, Aborted},
    Future,
};
use http::{Request, Response};
use hyper::Body;
use tower::{Layer, Service};

async fn run_server<L, ResBody>(
    server: Router<L>,
    incoming: TcpIncoming,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), dozer_types::tonic::transport::Error>
where
    L: Layer<Routes>,
    L::Service: Service<Request<Body>, Response = Response<ResBody>> + Clone + Send + 'static,
    <<L as Layer<Routes>>::Service as Service<Request<Body>>>::Future: Send + 'static,
    <<L as Layer<Routes>>::Service as Service<Request<Body>>>::Error: Into<BoxedError> + Send,
    ResBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<BoxedError>,
{
    // Tonic graceful shutdown doesn't allow us to set a timeout, resulting in hanging if a client doesn't close the connection.
    // So we just abort the server when the shutdown signal is received.
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    tokio::spawn(async move {
        shutdown.await;
        abort_handle.abort();
    });

    match Abortable::new(server.serve_with_incoming(incoming), abort_registration).await {
        Ok(result) => result,
        Err(Aborted) => Ok(()),
    }
}
