use dozer_types::models::api_security::ApiSecurity;
use futures_util::future::BoxFuture;
use hyper::{Body, Method};
use std::task::{Context, Poll};
use tonic::{
    body::{empty_body, BoxBody},
    codegen::http,
    transport::NamedService,
};
use tower::{Layer, Service};

use crate::auth::Authorizer;

#[derive(Debug, Clone, Default)]
pub struct AuthMiddlewareLayer {
    security: Option<ApiSecurity>,
}
impl AuthMiddlewareLayer {
    pub fn new(security: Option<ApiSecurity>) -> Self {
        Self { security }
    }
}

impl<S> Layer<S> for AuthMiddlewareLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            security: self.security.to_owned(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    security: Option<ApiSecurity>,
}

impl<S> Service<hyper::Request<Body>> for AuthMiddleware<S>
where
    S: Service<hyper::Request<Body>, Response = hyper::Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let security = if req.method() == Method::OPTIONS {
            // During OPTIONS request, authorization should not be applied
            None
        } else {
            self.security.to_owned()
        };

        Box::pin(async move {
            match security {
                Some(security) => {
                    let auth_header = req.headers().get("authorization");
                    if let Some(auth_header) = auth_header {
                        let auth_header_str = auth_header.to_str().unwrap();
                        let authorizer = Authorizer::from(&security);
                        if auth_header_str.starts_with("Bearer ") {
                            let token_array: Vec<&str> = auth_header_str.split(' ').collect();
                            let token_data = authorizer.validate_token(token_array[1]);
                            return match token_data {
                                Ok(claims) => {
                                    let mut modified_request = req;
                                    modified_request.extensions_mut().insert(claims.access);
                                    let response = inner.call(modified_request).await?;
                                    Ok(response)
                                }
                                Err(_) => Ok(http::Response::builder()
                                    .status(401)
                                    .header("grpc-status", "7")
                                    .header("content-type", "application/grpc")
                                    .body(empty_body())
                                    .unwrap()),
                            };
                        }
                    }
                    Ok(http::Response::builder()
                        .status(401)
                        .header("grpc-status", "7")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }
                None => {
                    let response = inner.call(req).await?;
                    Ok(response)
                }
            }
        })
    }
}

impl<S: NamedService> NamedService for AuthMiddleware<S> {
    const NAME: &'static str = S::NAME;
}
