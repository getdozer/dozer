use super::cloud::login::CredentialInfo;
use http::{Request, Response};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::codegen::http;
use tonic::transport::Body;
use tonic::transport::Channel;
use tower::Service;

pub struct TokenLayer {
    inner: Channel,
    credential: CredentialInfo,
}

impl TokenLayer {
    pub fn new(inner: Channel, credential: CredentialInfo) -> Self {
        TokenLayer { inner, credential }
    }
}

impl Service<Request<BoxBody>> for TokenLayer {
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let credential = self.credential.clone();
        Box::pin(async move {
            // Do extra async work here...
            let token = credential.get_access_token().await?;
            let mut new_request = req;
            new_request.headers_mut().insert(
                http::header::AUTHORIZATION,
                format!("Bearer {}", token.access_token).parse().unwrap(),
            );
            let response = inner.call(new_request).await?;
            Ok(response)
        })
    }
}
