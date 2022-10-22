use super::api_server::ApiServer;
use crate::{
    api_server::{ApiSecurity, CorsOptions},
    generator::oapi::generator::OpenApiGenerator,
    test_utils,
};
use dozer_types::serde_json::{json, Value};

#[test]
fn test_generate_oapi() -> anyhow::Result<()> {
    let schema: dozer_types::types::Schema = test_utils::get_schema();
    let endpoint = test_utils::get_endpoint();

    let oapi_generator = OpenApiGenerator::new(
        schema,
        endpoint.name.to_owned(),
        endpoint,
        vec![format!("http://localhost:{}", "8080")],
    );
    let generated = oapi_generator.generate_oas3()?;

    assert_eq!(generated.paths.paths.len(), 3, " paths must be generated");
    Ok(())
}

#[actix_web::test]
async fn list_route() -> anyhow::Result<()> {
    let endpoint = test_utils::get_endpoint();
    let mut schema_name = endpoint.to_owned().path;
    schema_name.remove(0);
    let cache = test_utils::initialize_cache(&schema_name)?;
    let api_server = ApiServer::create_app_entry(
        ApiSecurity::None,
        CorsOptions::Permissive,
        vec![endpoint.to_owned()],
        cache,
    );
    let app = actix_web::test::init_service(api_server).await;
    let req = actix_web::test::TestRequest::get()
        .uri(&endpoint.path)
        .to_request();
    let resp: Value = actix_web::test::call_and_read_body_json(&app, req).await;
    assert!(resp.is_array());
    if let Value::Array(resp) = resp {
        assert!(!resp.is_empty());
    }
    Ok(())
}

#[actix_web::test]
async fn query_route() -> anyhow::Result<()> {
    let endpoint = test_utils::get_endpoint();
    let mut schema_name = endpoint.to_owned().path;
    schema_name.remove(0);
    let cache = test_utils::initialize_cache(&schema_name)?;
    let api_server = ApiServer::create_app_entry(
        ApiSecurity::None,
        CorsOptions::Permissive,
        vec![endpoint.to_owned()],
        cache,
    );
    let app = actix_web::test::init_service(api_server).await;
    let req = actix_web::test::TestRequest::post()
        .uri(&format!("{}/query", endpoint.path))
        .set_json(
            json!({"$filter": { "$and": [{"film_id":  {"$lt": 500}}, {"film_id":  {"$gte": 2}}]}}),
        )
        .to_request();
    let resp: Value = actix_web::test::call_and_read_body_json(&app, req).await;
    assert!(resp.is_array());
    Ok(())
}

#[actix_web::test]
async fn get_route() -> anyhow::Result<()> {
    let endpoint = test_utils::get_endpoint();
    let mut schema_name = endpoint.to_owned().path;
    schema_name.remove(0);
    let cache = test_utils::initialize_cache(&schema_name)?;
    let api_server = ApiServer::create_app_entry(
        ApiSecurity::None,
        CorsOptions::Permissive,
        vec![endpoint.to_owned()],
        cache,
    );
    let app = actix_web::test::init_service(api_server).await;
    let req = actix_web::test::TestRequest::get()
        .uri(&format!("{}/{}", endpoint.path, 268))
        .to_request();
    let resp: Value = actix_web::test::call_and_read_body_json(&app, req).await;
    assert!(resp.is_object());
    Ok(())
}
