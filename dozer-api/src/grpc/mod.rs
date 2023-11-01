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

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
pub use client_server::ApiServer;
use dozer_services::tonic::{
    self,
    transport::server::{Connected, Router, Routes, TcpConnectInfo, TcpIncoming},
};
use dozer_types::errors::internal::BoxedError;
use futures_util::Future;
use futures_util::StreamExt;
pub use grpc_web_middleware::enable_grpc_web;
use http::{Request, Response};
use hyper::server::conn::AddrStream;
use hyper::Body;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tower::{Layer, Service};

use crate::shutdown::ShutdownReceiver;

#[derive(Debug)]
struct ShutdownAddrStream<F> {
    inner: AddrStream,
    state: ShutdownState<F>,
}

#[derive(Debug)]
enum ShutdownState<F> {
    SignalPending(F),
    ShutdownPending,
    Done,
}

impl<F: Future<Output = ()> + Unpin> ShutdownAddrStream<F> {
    fn check_shutdown(&mut self, cx: &mut Context<'_>) -> Result<(), io::Error> {
        match &mut self.state {
            ShutdownState::SignalPending(signal) => {
                if let Poll::Ready(()) = Pin::new(signal).poll(cx) {
                    self.state = ShutdownState::ShutdownPending;
                    self.check_shutdown(cx)
                } else {
                    Ok(())
                }
            }
            ShutdownState::ShutdownPending => match Pin::new(&mut self.inner).poll_shutdown(cx) {
                Poll::Ready(Ok(())) => {
                    self.state = ShutdownState::Done;
                    Ok(())
                }
                Poll::Ready(Err(e)) => Err(e),
                Poll::Pending => Ok(()),
            },
            ShutdownState::Done => Ok(()),
        }
    }

    fn poll_impl<T>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        func: fn(Pin<&mut AddrStream>, &mut Context<'_>) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        let this = Pin::into_inner(self);
        if let Err(e) = this.check_shutdown(cx) {
            return Poll::Ready(Err(e));
        }

        func(Pin::new(&mut this.inner), cx)
    }
}

impl<F: Future<Output = ()> + Unpin> AsyncRead for ShutdownAddrStream<F> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = Pin::into_inner(self);
        if let Err(e) = this.check_shutdown(cx) {
            return Poll::Ready(Err(e));
        }

        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<F: Future<Output = ()> + Unpin> AsyncWrite for ShutdownAddrStream<F> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = Pin::into_inner(self);
        if let Err(e) = this.check_shutdown(cx) {
            return Poll::Ready(Err(e));
        }

        Pin::new(&mut this.inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_impl(cx, AsyncWrite::poll_flush)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.poll_impl(cx, AsyncWrite::poll_shutdown)
    }
}

impl<F> Connected for ShutdownAddrStream<F> {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.inner.connect_info()
    }
}

async fn run_server<L, ResBody>(
    server: Router<L>,
    incoming: TcpIncoming,
    shutdown: ShutdownReceiver,
) -> Result<(), tonic::transport::Error>
where
    L: Layer<Routes>,
    L::Service: Service<Request<Body>, Response = Response<ResBody>> + Clone + Send + 'static,
    <<L as Layer<Routes>>::Service as Service<Request<Body>>>::Future: Send + 'static,
    <<L as Layer<Routes>>::Service as Service<Request<Body>>>::Error: Into<BoxedError> + Send,
    ResBody: http_body::Body<Data = Bytes> + Send + 'static,
    ResBody::Error: Into<BoxedError>,
{
    let incoming = incoming.map(|stream| {
        stream.map(|stream| {
            let shutdown = shutdown.create_shutdown_future();
            ShutdownAddrStream {
                inner: stream,
                state: ShutdownState::SignalPending(Box::pin(shutdown)),
            }
        })
    });

    server
        .serve_with_incoming_shutdown(incoming, shutdown.create_shutdown_future())
        .await
}
