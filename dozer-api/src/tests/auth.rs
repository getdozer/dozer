use super::super::api_server::ApiServer;
use crate::{
    api_server::{ApiSecurity, CorsOptions},
    auth::{Access, Authorizer},
    test_utils,
};
use actix_web::{body::MessageBody, dev::ServiceResponse};

async fn check_status(
    security: ApiSecurity,
    token: Option<String>,
) -> ServiceResponse<impl MessageBody> {
    let endpoint = test_utils::get_endpoint();
    let mut schema_name = endpoint.to_owned().path;
    schema_name.remove(0);
    let cache = test_utils::initialize_cache(&schema_name);
    let api_server = ApiServer::create_app_entry(
        security,
        CorsOptions::Permissive,
        vec![endpoint.to_owned()],
        cache,
    );
    let app = actix_web::test::init_service(api_server).await;

    let req = actix_web::test::TestRequest::get().uri(&endpoint.path);

    let req = match token {
        Some(token) => req.append_header(("Authorization", format!("Bearer {}", token))),
        None => req,
    };
    let req = req.to_request();
    let res = actix_web::test::call_service(&app, req).await;
    res
}
#[actix_web::test]
async fn verify_token_test() {
    let secret = "secret".to_string();

    // Without ApiSecurity
    let res = check_status(ApiSecurity::None, None).await;
    assert!(res.status().is_success());

    // With ApiSecurity but no token
    let res = check_status(ApiSecurity::Jwt(secret.to_owned()), None).await;
    assert_eq!(res.status().as_u16(), 401, "Should be unauthorized.");

    let auth = Authorizer::new(secret.to_owned(), None, None);
    let token = auth.generate_token(Access::All, None).unwrap();

    let res = check_status(ApiSecurity::Jwt("secret".to_string()), Some(token)).await;
    assert!(res.status().is_success());
}
