use std::sync::Arc;

use super::super::{ApiServer, CorsOptions};
use crate::{
    auth::{Access, Authorizer},
    test_utils, CacheEndpoint,
};
use actix_web::{body::MessageBody, dev::ServiceResponse};
use dozer_types::{
    models::api_security::ApiSecurity,
    serde,
    serde::{Deserialize, Serialize},
    serde_json::{json, Value},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
struct TokenResponse {
    token: String,
}
#[actix_web::test]
async fn call_auth_token_api() {
    let secret = "secret";

    // Shouldnt be able to create token without Master Token
    let res = _call_auth_token_api(secret.to_string(), None, None).await;
    assert_eq!(res.status().as_u16(), 401, "Should be unauthorized.");

    let auth = Authorizer::new(secret, None, None);
    let token = auth.generate_token(Access::All, None).unwrap();

    let json = json!({"Custom":{"films":{"filter":null,"fields":[]}}});
    let res = _call_auth_token_api(secret.to_string(), Some(token), Some(json)).await;
    assert_eq!(
        res.status().as_u16(),
        200,
        "Should be able to create a token."
    );

    let body: TokenResponse = actix_web::test::read_body_json(res).await;
    assert!(body.token.len() > 1, "Token must be present");
}

#[actix_web::test]
async fn verify_token_test() {
    let secret = "secret";

    // Without ApiSecurity
    let res = check_status(None, None).await;
    assert!(res.status().is_success());

    // With ApiSecurity but no token
    let res = check_status(Some(ApiSecurity::Jwt(secret.to_string())), None).await;
    assert_eq!(res.status().as_u16(), 401, "Should be unauthorized.");

    let auth = Authorizer::new(secret, None, None);
    let token = auth.generate_token(Access::All, None).unwrap();

    let res = check_status(Some(ApiSecurity::Jwt("secret".to_string())), Some(token)).await;
    assert!(res.status().is_success());
}

async fn check_status(
    security: Option<ApiSecurity>,
    token: Option<String>,
) -> ServiceResponse<impl MessageBody> {
    let endpoint = test_utils::get_endpoint();
    let cache_manager = test_utils::initialize_cache(&endpoint.name, None);
    let api_server = ApiServer::create_app_entry(
        security,
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint.clone()).unwrap(),
        )],
        Default::default(),
        50,
        None,
    );
    let app = actix_web::test::init_service(api_server).await;

    let req = actix_web::test::TestRequest::get().uri(&endpoint.path);

    let req = match token {
        Some(token) => req.append_header(("Authorization", format!("Bearer {token}"))),
        None => req,
    };
    let req = req.to_request();

    actix_web::test::call_service(&app, req).await
}

async fn _call_auth_token_api(
    secret: String,
    token: Option<String>,
    body: Option<Value>,
) -> ServiceResponse<impl MessageBody> {
    let endpoint = test_utils::get_endpoint();
    let schema_name = endpoint.name.clone();
    let cache_manager = test_utils::initialize_cache(&schema_name, None);
    let api_server = ApiServer::create_app_entry(
        Some(ApiSecurity::Jwt(secret)),
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint).unwrap(),
        )],
        Default::default(),
        50,
        None,
    );
    let app = actix_web::test::init_service(api_server).await;

    let req = actix_web::test::TestRequest::post().uri("/auth/token");

    let req = match token {
        Some(token) => req.append_header(("Authorization", format!("Bearer {token}"))),
        None => req,
    };

    let req = match body {
        Some(body) => req.set_json(body),
        None => req,
    };

    let req = req.to_request();
    actix_web::test::call_service(&app, req).await
}
