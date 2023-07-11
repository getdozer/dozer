use std::future::{ready, Ready};

use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use dozer_tracing::{API_LATENCY_HISTOGRAM_NAME, API_REQUEST_COUNTER_NAME};
use futures_util::future::LocalBoxFuture;
use metrics::{histogram, increment_counter};

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
        let request_path = req.path().to_string();
        let fut: <S as Service<ServiceRequest>>::Future = self.service.call(req);
        Box::pin(async move {
            let start_time = std::time::Instant::now();
            let res: Result<ServiceResponse<B>, Error> = fut.await;
            let labels = [("endpoint", request_path), ("api_type", "rest".to_owned())];
            histogram!(API_LATENCY_HISTOGRAM_NAME, start_time.elapsed(), &labels);
            increment_counter!(API_REQUEST_COUNTER_NAME);
            res
        })
    }
}
