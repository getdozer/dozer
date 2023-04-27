use std::{fmt::Debug, sync::Arc};

use super::super::{ApiServer, CorsOptions};
use crate::{generator::oapi::generator::OpenApiGenerator, test_utils, CacheEndpoint};
use actix_http::{body::MessageBody, Request};
use actix_web::dev::{Service, ServiceResponse};
use dozer_types::serde_json::{json, Value};

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
) -> (u64, Vec<Value>)
where
    S: Service<Request, Response = ServiceResponse<B>, Error = E>,
    B: MessageBody,
    E: Debug,
{
    let mut req = actix_web::test::TestRequest::post().uri(&format!("{path}/count"));
    if let Some(query) = query.clone() {
        req = req.set_json(query);
    }
    let req = req.to_request();
    let res = actix_web::test::call_service(service, req).await;
    assert!(res.status().is_success());

    let body: Value = actix_web::test::read_body_json(res).await;
    let count = body.as_u64().unwrap();

    let mut req = actix_web::test::TestRequest::post().uri(&format!("{path}/query"));
    if let Some(query) = query {
        req = req.set_json(query);
    }
    let req = req.to_request();
    let res = actix_web::test::call_service(service, req).await;
    assert!(res.status().is_success());

    let body: Value = actix_web::test::read_body_json(res).await;
    let records = body.as_array().unwrap().to_vec();

    (count, records)
}

#[actix_web::test]
async fn count_and_query_route() {
    let endpoint = test_utils::get_endpoint();
    let cache_manager = test_utils::initialize_cache(&endpoint.name, None);
    let api_server = ApiServer::create_app_entry(
        None,
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint.clone()).unwrap(),
        )],
    );
    let app = actix_web::test::init_service(api_server).await;

    // Empty query.
    let (count, records) = count_and_query(&endpoint.path, &app, None).await;
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);
    let (count, records) = count_and_query(&endpoint.path, &app, Some(json!({}))).await;
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);

    // Query with filter.
    let (count, records) = count_and_query(
        &endpoint.path,
        &app,
        Some(json!({"$filter": {"film_id":  268}})),
    )
    .await;
    assert_eq!(count, 1);
    assert_eq!(records.len(), 1);
    let (count, records) = count_and_query(
        &endpoint.path,
        &app,
        Some(json!({"$filter": {"release_year": 2006}})),
    )
    .await;
    assert_eq!(count, 52);
    assert_eq!(records.len(), 50);

    // Query with limit.
    let (count, records) = count_and_query(&endpoint.path, &app, Some(json!({"$limit": 11}))).await;
    assert_eq!(count, 11);
    assert_eq!(records.len(), 11);
}

#[actix_web::test]
async fn get_route() {
    let endpoint = test_utils::get_endpoint();
    let cache_manager = test_utils::initialize_cache(&endpoint.name, None);
    let api_server = ApiServer::create_app_entry(
        None,
        CorsOptions::Permissive,
        vec![Arc::new(
            CacheEndpoint::open(&*cache_manager, Default::default(), endpoint.clone()).unwrap(),
        )],
    );
    let app = actix_web::test::init_service(api_server).await;
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
