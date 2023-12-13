use std::{fmt::Debug, sync::Arc};

use super::super::{ApiServer, CorsOptions};
use crate::{generator::oapi::generator::OpenApiGenerator, test_utils, CacheEndpoint};
use actix_http::{body::MessageBody, Request};
use actix_web::dev::{Service, ServiceResponse};
use actix_web::http::header::ContentType;
use dozer_cache::Phase;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::serde_json::{json, Value};
use http::StatusCode;

#[test]
fn test_generate_oapi() {
    let (schema, secondary_indexes) = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();

    let oapi_generator = OpenApiGenerator::new(
        &schema,
        &secondary_indexes,
        endpoint,
        vec![format!("http://localhost:{}", "8080")],
    );
    let generated = oapi_generator.generate_oas3();

    assert_eq!(generated.paths.paths.len(), 4, " paths must be generated");
}

#[actix_web::test]
async fn list_route() {
    let endpoint = test_utils::get_endpoint();
    let cache_manager = test_utils::initialize_cache(&endpoint.name, None);
    let api_server = ApiServer::create_app_entry(
        None,
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint.clone()).unwrap(),
        )],
        Default::default(),
        50,
        None,
    );
    let app = actix_web::test::init_service(api_server).await;

    let req = actix_web::test::TestRequest::get()
        .uri(&endpoint.path)
        .to_request();
    let res = actix_web::test::call_service(&app, req).await;
    assert!(res.status().is_success());

    let body: Value = actix_web::test::read_body_json(res).await;
    assert!(body.is_array(), "Must return an array");
    assert!(!body.as_array().unwrap().is_empty(), "Must return records");
}

async fn count_and_query<S, B, E>(
    path: &str,
    service: &S,
    query: Option<Value>,
    content_type: Option<ContentType>,
) -> Result<(u64, Vec<Value>), StatusCode>
where
    S: Service<Request, Response = ServiceResponse<B>, Error = E>,
    B: MessageBody,
    E: Debug,
{
    let mut req = actix_web::test::TestRequest::post().uri(&format!("{path}/count"));
    if let Some(query) = query.clone() {
        req = req.set_json(query);
    }
    if let Some(content_type) = content_type.clone() {
        req = req.insert_header(content_type);
    }
    let req = req.to_request();
    let res = actix_web::test::call_service(service, req).await;
    if !res.status().is_success() {
        return Err(res.status());
    }

    let body: Value = actix_web::test::read_body_json(res).await;
    let count = body.as_u64().unwrap();

    let mut req = actix_web::test::TestRequest::post().uri(&format!("{path}/query"));
    if let Some(query) = query {
        req = req.set_json(query);
    }
    if let Some(content_type) = content_type.clone() {
        req = req.insert_header(content_type);
    }
    let req = req.to_request();
    let res = actix_web::test::call_service(service, req).await;
    if !res.status().is_success() {
        return Err(res.status());
    }

    let body: Value = actix_web::test::read_body_json(res).await;
    let records = body.as_array().unwrap().clone();

    Ok((count, records))
}

#[actix_web::test]
async fn count_and_query_route() {
    let (app, endpoint) = setup_service().await;

    // Empty query.
    let (count, records) = count_and_query(&endpoint.path, &app, None, None)
        .await
        .unwrap();
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);
    let (count, records) = count_and_query(&endpoint.path, &app, Some(json!({})), None)
        .await
        .unwrap();
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);

    // Query with filter.
    let (count, records) = count_and_query(
        &endpoint.path,
        &app,
        Some(json!({"$filter": {"film_id":  268}})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(records.len(), 1);
    let (count, records) = count_and_query(
        &endpoint.path,
        &app,
        Some(json!({"$filter": {"release_year": 2006}})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);

    // Query with limit.
    let (count, records) = count_and_query(&endpoint.path, &app, Some(json!({"$limit": 11})), None)
        .await
        .unwrap();
    assert_eq!(count, 11);
    assert_eq!(records.len(), 11);
}

#[actix_web::test]
async fn get_route() {
    let (app, endpoint) = setup_service().await;
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("{}/{}", endpoint.path, 268))
        .to_request();

    let res = actix_web::test::call_service(&app, req).await;
    assert!(res.status().is_success());

    let body: Value = actix_web::test::read_body_json(res).await;
    assert!(body.is_object(), "Must return an object");
    let val = body.as_object().unwrap();
    assert_eq!(
        val.get("film_id").unwrap().to_string(),
        "268".to_string(),
        "Must be equal"
    );
}

#[actix_web::test]
async fn get_phase_test() {
    let (app, endpoint) = setup_service().await;

    let req = actix_web::test::TestRequest::post()
        .uri(&format!("{}/{}", endpoint.path, "phase"))
        .to_request();

    let res = actix_web::test::call_service(&app, req).await;
    assert!(res.status().is_success());

    let phase: Phase = actix_web::test::read_body_json(res).await;
    assert_eq!(phase, Phase::Streaming);
}

#[actix_web::test]
async fn get_endpoint_paths_test() {
    let (app, endpoint) = setup_service().await;

    let req = actix_web::test::TestRequest::get().uri("/").to_request();
    let res = actix_web::test::call_service(&app, req).await;
    assert!(res.status().is_success());

    let body: Vec<String> = actix_web::test::read_body_json(res).await;
    assert_eq!(body, vec![endpoint.path.clone()]);
}

#[actix_web::test]
async fn path_collision_test() {
    let first_endpoint = ApiEndpoint {
        name: "films".to_string(),
        path: "/foo".to_string(),
        table_name: "film".to_string(),
        ..Default::default()
    };

    let second_endpoint = ApiEndpoint {
        name: "films_second".to_string(),
        path: "/foo/second".to_string(),
        table_name: "film".to_string(),
        ..Default::default()
    };

    let api_server = ApiServer::create_app_entry(
        None,
        CorsOptions::Permissive,
        vec![
            Arc::new(
                CacheEndpoint::open(
                    &*test_utils::initialize_cache(&first_endpoint.name, None),
                    Default::default(),
                    first_endpoint.clone(),
                )
                .unwrap(),
            ),
            Arc::new(
                CacheEndpoint::open(
                    &*test_utils::initialize_cache(&second_endpoint.name, None),
                    Default::default(),
                    second_endpoint.clone(),
                )
                .unwrap(),
            ),
        ],
        Default::default(),
        50,
        None,
    );
    let app = actix_web::test::init_service(api_server).await;

    let req = actix_web::test::TestRequest::get()
        .uri("/foo/second/query")
        .to_request();

    let res = actix_web::test::call_service(&app, req).await;

    //assert the route matched something
    assert_ne!(res.status(), 404);
}

async fn setup_service() -> (
    impl Service<Request, Response = ServiceResponse<impl MessageBody>, Error = actix_web::Error>,
    ApiEndpoint,
) {
    let endpoint = test_utils::get_endpoint();
    let cache_manager = test_utils::initialize_cache(&endpoint.name, None);
    let api_server = ApiServer::create_app_entry(
        None,
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint.clone()).unwrap(),
        )],
        Default::default(),
        50,
        None,
    );
    (actix_web::test::init_service(api_server).await, endpoint)
}
#[actix_web::test]
async fn test_invalid_content_type() {
    let (app, endpoint) = setup_service().await;
    let status = count_and_query(&endpoint.path, &app, None, Some(ContentType::plaintext()))
        .await
        .unwrap_err();
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[actix_web::test]
async fn test_correct_content_type() {
    let (app, endpoint) = setup_service().await;

    let (count, records) = count_and_query(&endpoint.path, &app, None, Some(ContentType::json()))
        .await
        .unwrap();
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);
}

#[actix_web::test]
async fn test_empty_content_type() {
    let (app, endpoint) = setup_service().await;

    let (count, records) = count_and_query(&endpoint.path, &app, None, None)
        .await
        .unwrap();
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);
}

#[actix_web::test]
async fn test_malformed_json() {
    let (app, endpoint) = setup_service().await;
    let status = count_and_query(
        &endpoint.path,
        &app,
        Some(json!({"$limit":"invalid"})),
        None,
    )
    .await
    .unwrap_err();
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
