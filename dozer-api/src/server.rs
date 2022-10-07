use actix_web::{rt, web, App, HttpResponse, HttpServer, Responder};
use dozer_cache::cache::{get_primary_key, lmdb::cache::LmdbCache, Cache};
use dozer_types::{models::api_endpoint::ApiEndpoint, types::Field};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct CacheQuery {
    q: String,
}

async fn get(path: web::Path<(String,)>, cache: web::Data<Arc<LmdbCache>>) -> impl Responder {
    let id_str = path.into_inner().0;
    let id = id_str.parse::<i64>().unwrap();
    let key = get_primary_key(vec![0], vec![Field::Int(id)]);
    let val = cache.get(key).unwrap();

    HttpResponse::Ok().body(format!("key: {}, val: {:?}", id_str, val))
}

async fn list(cache: web::Data<Arc<LmdbCache>>, query: web::Json<CacheQuery>) -> impl Responder {
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
