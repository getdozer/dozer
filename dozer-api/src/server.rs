use actix_web::{rt, web, App, HttpResponse, HttpServer, Responder};
use anyhow::Context;
use dozer_cache::cache::{get_primary_key, lmdb::cache::LmdbCache, Cache};
use dozer_types::{
    json_value_to_field, models::api_endpoint::ApiEndpoint, record_to_json, types::Field,
};
use serde_json::Value;
use std::sync::Arc;

fn get_record(cache: web::Data<Arc<LmdbCache>>, key: Value) -> anyhow::Result<String> {
    let key = match json_value_to_field(key.clone()) {
        Ok(key) => key,
        Err(e) => {
            panic!("error : {:?}", e);
        }
    };
    let key = get_primary_key(vec![0], vec![key]);

    let rec = cache.get(key).context("record not found")?;
    let schema = cache.get_schema(rec.schema_id.clone().context("schema_id not found")?)?;
    let str = record_to_json(rec, schema)?;
    Ok(str)
}

async fn get(path: web::Path<(String,)>, cache: web::Data<Arc<LmdbCache>>) -> impl Responder {
    let key_json: Value = serde_json::from_str(&path.into_inner().0).unwrap();

    match get_record(cache, key_json) {
        Ok(json) => HttpResponse::Ok().body(json),
        Err(e) => HttpResponse::NotFound().body(e.to_string()),
    }
}

async fn list(cache: web::Data<Arc<LmdbCache>>) -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[derive(Clone)]
pub struct ApiServer {
    shutdown_timeout: u64,
    port: u16,
}

// #[async_trait]
impl ApiServer {
    pub fn default() -> Self {
        Self {
            shutdown_timeout: 0,
            port: 8080,
        }
    }
    pub fn new(shutdown_timeout: u64, port: u16) -> Self {
        Self {
            shutdown_timeout,
            port,
        }
    }

    pub fn run(&self, endpoints: Vec<ApiEndpoint>, cache: Arc<LmdbCache>) -> std::io::Result<()> {
        let endpoints = endpoints.clone();

        rt::System::new().block_on(async move {
            HttpServer::new(move || {
                let app = App::new();
                let app = app.app_data(web::Data::new(cache.clone()));
                endpoints.iter().fold(app, |app, endpoint| {
                    let list_route = &endpoint.path.clone();
                    let get_route = format!("{}/{}", list_route, "{id}".to_string());
                    app.route(list_route, web::get().to(list))
                        .route(&get_route, web::get().to(get))
                })
            })
            .bind(("0.0.0.0", self.port.to_owned()))?
            .shutdown_timeout(self.shutdown_timeout.to_owned())
            .run()
            .await
        })
    }
}
