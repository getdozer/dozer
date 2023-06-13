use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use futures_util::future::LocalBoxFuture;
use metrics::histogram;

use crate::api_helper::API_LATENCY_HISTOGRAM_NAME;

pub struct RestMetric;

impl<S, B> Transform<S, ServiceRequest> for RestMetric
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type InitError = ();
    type Transform = RestMetricMiddleware<S>;
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RestMetricMiddleware { service }))
    }
}

pub struct RestMetricMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RestMetricMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let fut = self.service.call(req);

        Box::pin(async move {
            let start_time = std::time::Instant::now();
            let res = fut.await;
            histogram!(API_LATENCY_HISTOGRAM_NAME, start_time.elapsed());
            res
        })
    }
}
