use dozer_services::tonic::{body::BoxBody, transport::NamedService};
use futures_util::Future;
use pin_project::pin_project;
use tonic_web::{CorsGrpcWeb, GrpcWebLayer, GrpcWebService};
use tower::{BoxError, Layer, Service};
use tower_http::cors::{Cors, CorsLayer};

#[derive(Debug, Clone)]
pub enum MaybeGrpcWebService<S> {
    GrpcWeb(Cors<GrpcWebService<S>>),
    NoGrpcWeb(S),
}

#[pin_project(project = MaybeGrpcWebServiceFutureProj)]
pub enum MaybeGrpcWebServiceFuture<S>
where
    S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>,
    S: Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
{
    GrpcWeb(#[pin] <CorsGrpcWeb<S> as Service<http::Request<hyper::Body>>>::Future),
    NoGrpcWeb(#[pin] <S as Service<http::Request<hyper::Body>>>::Future),
}

impl<S> Service<http::Request<hyper::Body>> for MaybeGrpcWebService<S>
where
    S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>,
    S: Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = MaybeGrpcWebServiceFuture<S>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self {
            MaybeGrpcWebService::GrpcWeb(service) => service.poll_ready(cx),
            MaybeGrpcWebService::NoGrpcWeb(service) => service.poll_ready(cx),
        }
    }

    fn call(&mut self, req: http::Request<hyper::Body>) -> Self::Future {
        match self {
            MaybeGrpcWebService::GrpcWeb(service) => {
                MaybeGrpcWebServiceFuture::GrpcWeb(service.call(req))
            }
            MaybeGrpcWebService::NoGrpcWeb(service) => {
                MaybeGrpcWebServiceFuture::NoGrpcWeb(service.call(req))
            }
        }
    }
}

impl<S> NamedService for MaybeGrpcWebService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S> Future for MaybeGrpcWebServiceFuture<S>
where
    S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>,
    S: Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
{
    type Output = <<S as Service<http::Request<hyper::Body>>>::Future as Future>::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        match this {
            MaybeGrpcWebServiceFutureProj::GrpcWeb(fut) => fut.poll(cx),
            MaybeGrpcWebServiceFutureProj::NoGrpcWeb(fut) => fut.poll(cx),
        }
    }
}

pub fn enable_grpc_web<S>(service: S, enabled: bool) -> MaybeGrpcWebService<S>
where
    S: Service<http::Request<hyper::Body>, Response = http::Response<BoxBody>>,
    S: Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
{
    if enabled {
        let service = GrpcWebLayer::new().layer(service);
        let service = CorsLayer::permissive().layer(service);
        MaybeGrpcWebService::GrpcWeb(service)
    } else {
        MaybeGrpcWebService::NoGrpcWeb(service)
    }
}
