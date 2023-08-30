use std::{
    future::{ready, Ready},
    sync::Arc,
};

use actix_http::HttpMessage;
use actix_web::{
    dev::{forward_ready, Service, ServiceRequest, ServiceResponse, Transform},
    Error,
};
use dozer_tracing::LabelsAndProgress;
use futures_util::future::LocalBoxFuture;
use metrics::{histogram, increment_counter};

use crate::{
    api_helper::{API_LATENCY_HISTOGRAM_NAME, API_REQUEST_COUNTER_NAME},
    CacheEndpoint,
};

pub struct RestMetric {
    labels: LabelsAndProgress,
}

impl RestMetric {
    pub fn new(labels: LabelsAndProgress) -> Self {
        Self { labels }
    }
}

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
        ready(Ok(RestMetricMiddleware {
            labels: self.labels.clone(),
            service,
        }))
    }
}

pub struct RestMetricMiddleware<S> {
    labels: LabelsAndProgress,
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
        let cache_data = req.extensions().get::<Arc<CacheEndpoint>>().cloned();
        let mut labels = self.labels.labels().clone();
        let fut: <S as Service<ServiceRequest>>::Future = self.service.call(req);

        Box::pin(async move {
            let start_time = std::time::Instant::now();
            let res: Result<ServiceResponse<B>, Error> = fut.await;
            if let Some(endpoint) = cache_data {
                labels.push("endpoint", endpoint.endpoint.name.clone());
                labels.push("api_type", "rest".to_string());
                histogram!(
                    API_LATENCY_HISTOGRAM_NAME,
                    start_time.elapsed(),
                    labels.clone()
                );
                increment_counter!(API_REQUEST_COUNTER_NAME, labels);
            }
            res
        })
    }
}
