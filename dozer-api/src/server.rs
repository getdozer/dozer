use actix_web::{
    body::MessageBody,
    dev::{ServiceFactory, ServiceRequest, ServiceResponse},
    http::header::ContentType,
    rt, web, App, HttpResponse, HttpServer, Responder,
};
use dozer_cache::cache::LmdbCache;
use dozer_types::models::api_endpoint::ApiEndpoint;
use std::sync::Arc;

use crate::api_helper;

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
    pub fn create_app_entry(
        endpoints: Vec<ApiEndpoint>,
        cache: Arc<LmdbCache>,
    ) -> App<
        impl ServiceFactory<
            ServiceRequest,
            Response = ServiceResponse<impl MessageBody>,
            Config = (),
            InitError = (),
            Error = actix_web::Error,
        >,
    > {
        let app = App::new();
        let app = app.app_data(web::Data::new((cache, endpoints.clone())));
        let app = endpoints.iter().fold(app, |app, endpoint| {
            let list_route = &endpoint.path.clone();
            let get_route = format!("{}/{}", list_route, "{id}");
            let query_route = format!("{}/query", list_route);
            app.route(list_route, web::get().to(api_helper::list))
                .route(&get_route, web::get().to(api_helper::get))
                .route(&query_route, web::post().to(api_helper::query))
                .route("oapi", web::post().to(api_helper::generate_oapi))
        });
        app
    }
    pub fn run(&self, endpoints: Vec<ApiEndpoint>, cache: Arc<LmdbCache>) -> std::io::Result<()> {
        let endpoints = endpoints;
        rt::System::new().block_on(async move {
            HttpServer::new(move || ApiServer::create_app_entry(endpoints.clone(), cache.clone()))
                .bind(("0.0.0.0", self.port.to_owned()))?
                .shutdown_timeout(self.shutdown_timeout.to_owned())
                .run()
                .await
        })
    }
}
