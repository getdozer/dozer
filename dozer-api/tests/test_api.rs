#[cfg(test)]
mod tests {
    use actix_web::test;
    use dozer_api::server::ApiServer;
    use dozer_types::{
        models::api_endpoint::ApiEndpoint,
        types::{Field, Record},
    };
    use serde_json::{json, Value};
    use std::sync::Arc;

    use dozer_cache::cache::{Cache, LmdbCache};
    fn create_cache(schema_name: &str) -> anyhow::Result<Arc<LmdbCache>> {
        let cache = Arc::new(LmdbCache::new(true));
        let schema_str = include_str!("api-server/cache_film_schema.json");
        let schema: dozer_types::types::Schema = serde_json::from_str(schema_str)?;
        let records_str = include_str!("api-server/film_records.json");
        let records_value: Vec<Value> = serde_json::from_str(records_str)?;
        for record_str in records_value {
            let film_id = record_str["film_id"].as_i64();
            let description = record_str["description"].as_str();
            let release_year = record_str["release_year"].as_i64();
            if let (Some(film_id), Some(description), Some(release_year)) =
                (film_id, description, release_year)
            {
                let record = Record::new(
                    schema.identifier.clone(),
                    vec![
                        Field::Int(film_id),
                        Field::String(description.to_string()),
                        Field::Null,
                        Field::Int(release_year),
                    ],
                );
                cache.insert_schema(schema_name, &schema)?;
                cache.insert(&record)?;
            }
        }
        Ok(cache)
    }

    fn create_endpoint() -> anyhow::Result<ApiEndpoint> {
        let endpoint_str = include_str!("api-server/endpoint.json");
        let endpoint: ApiEndpoint = serde_json::from_str(endpoint_str)?;
        Ok(endpoint)
    }
    #[actix_web::test]
    async fn test_list() -> anyhow::Result<()> {
        let endpoint = create_endpoint()?;
        let mut schema_name = endpoint.to_owned().path;
        schema_name.remove(0);
        let cache = create_cache(&schema_name)?;
        let api_server = ApiServer::create_app_entry(vec![endpoint.to_owned()], cache);
        let app = test::init_service(api_server).await;
        let req = test::TestRequest::get().uri(&endpoint.path).to_request();
        let resp: Value = test::call_and_read_body_json(&app, req).await;
        assert!(resp.is_array());
        if let Value::Array(resp) = resp {
            assert!(!resp.is_empty());
        }
        Ok(())
    }

    #[actix_web::test]
    async fn test_query() -> anyhow::Result<()> {
        let endpoint = create_endpoint()?;
        let mut schema_name = endpoint.to_owned().path;
        schema_name.remove(0);
        let cache = create_cache(&schema_name)?;
        let api_server = ApiServer::create_app_entry(vec![endpoint.to_owned()], cache);
        let app = test::init_service(api_server).await;
        let req = test::TestRequest::post()
            .uri(&format!("{}/query", endpoint.path))
            .set_json(json!({"$and": [{"film_id":  {"$lt": 500}}, {"film_id":  {"$gte": 2}}]}))
            .to_request();
        let resp: Value = test::call_and_read_body_json(&app, req).await;
        assert!(resp.is_array());
        Ok(())
    }

    #[actix_web::test]
    async fn test_get_by_id() -> anyhow::Result<()> {
        let endpoint = create_endpoint()?;
        let mut schema_name = endpoint.to_owned().path;
        schema_name.remove(0);
        let cache = create_cache(&schema_name)?;
        let api_server = ApiServer::create_app_entry(vec![endpoint.to_owned()], cache);
        let app = test::init_service(api_server).await;
        let req = test::TestRequest::get()
            .uri(&format!("{}/{}", endpoint.path, 256))
            .to_request();
        let resp: Value = test::call_and_read_body_json(&app, req).await;
        assert!(resp.is_object());
        Ok(())
    }
}
