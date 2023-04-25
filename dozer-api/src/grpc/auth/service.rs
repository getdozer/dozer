use crate::auth::Access;

use tonic::{Request, Response, Status};

use crate::auth::api::auth_grpc;
use dozer_types::grpc_types::auth::auth_grpc_service_server::AuthGrpcService;
use dozer_types::grpc_types::auth::GetAuthTokenRequest;
use dozer_types::grpc_types::auth::GetAuthTokenResponse;
use dozer_types::models::api_security::ApiSecurity;

// #[derive(Clone)]
pub struct AuthService {
    /// For look up endpoint from its name. `key == value.endpoint.name`.
    pub security: Option<ApiSecurity>,
}

impl AuthService {
    pub fn new(security: Option<ApiSecurity>) -> Self {
        Self { security }
    }
}

#[tonic::async_trait]
impl AuthGrpcService for AuthService {
    async fn get_auth_token(
        &self,
        request: Request<GetAuthTokenRequest>,
    ) -> Result<Response<GetAuthTokenResponse>, Status> {
        let (_, extensions, request) = request.into_parts();
        let access = extensions.get::<Access>();

        auth_grpc(access, request.access_filter, self.security.clone())
    }
}
