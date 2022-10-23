use actix_cors::Cors;
use actix_web::{
    body::MessageBody,
    dev::{Service, ServiceFactory, ServiceRequest, ServiceResponse},
    middleware::Condition,
    rt, web, App, HttpMessage, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use dozer_cache::cache::LmdbCache;
use dozer_types::models::api_endpoint::ApiEndpoint;
use dozer_types::serde::{self, Deserialize, Serialize};
use std::sync::Arc;

use crate::{api_generator, api_validator};

#[derive(Clone)]
pub struct PipelineDetails {
    pub schema_name: String,
    pub endpoint: ApiEndpoint,
}
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(crate = "self::serde")]
pub enum CorsOptions {
    Permissive,
    // origins, max_age
    Custom(Vec<String>, usize),
}
#[derive(Clone)]
pub enum ApiSecurity {
    None,
    // Initialize with a JWT_SECRET
    Jwt(String),
}
#[derive(Clone)]
pub struct ApiServer {
    shutdown_timeout: u64,
    port: u16,
    cors: CorsOptions,
    security: ApiSecurity,
}

impl ApiServer {
    pub fn default() -> Self {
        Self {
            shutdown_timeout: 0,
            port: 8080,
            cors: CorsOptions::Permissive,
            security: ApiSecurity::None,
        }
    }
    pub fn new(shutdown_timeout: u64, port: u16, cors: CorsOptions, security: ApiSecurity) -> Self {
        Self {
            shutdown_timeout,
            port,
            cors,
            security,
        }
    }
    fn get_cors(cors: CorsOptions) -> Cors {
        match cors {
            CorsOptions::Permissive => Cors::permissive(),
            CorsOptions::Custom(origins, max_age) => origins
                .into_iter()
                .fold(Cors::default(), |cors, origin| {
                    cors.allowed_origin(&origin.to_owned())
                })
                .max_age(max_age),
        }
    }
    pub fn create_app_entry(
        security: ApiSecurity,
        cors: CorsOptions,
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
        let app = app.app_data(web::Data::new(cache));

        let cors = Self::get_cors(cors);
        let app = endpoints.iter().fold(app, |app, endpoint| {
            let endpoint = endpoint.clone();
            let scope = endpoint.path.clone();
            let schema_name = endpoint.name.clone();

            app.service(
                web::scope(&scope)
                    // Inject pipeline_details for generated functions
                    .wrap_fn(move |req, srv| {
                        req.extensions_mut().insert(PipelineDetails {
                            schema_name: schema_name.to_owned(),
                            endpoint: endpoint.clone(),
                        });
                        srv.call(req)
                    })
                    .route("/query", web::post().to(api_generator::query))
                    .route("/oapi", web::post().to(api_generator::generate_oapi))
                    .route("/{id}", web::get().to(api_generator::get))
                    .route("", web::get().to(api_generator::list)),
            )
        });
        // Conditionally enable auth based on endpoint
        app.wrap(Condition::new(
            matches!(security, ApiSecurity::Jwt(_)),
            HttpAuthentication::bearer(api_validator::validate),
        ))
        // Cors need to wrap security to return the right headers
        .wrap(cors)
    }
    pub fn run(&self, endpoints: Vec<ApiEndpoint>, cache: Arc<LmdbCache>) -> std::io::Result<()> {
        let endpoints = endpoints;
        let cors = self.cors.clone();
        let security = self.security.clone();
        rt::System::new().block_on(async move {
            HttpServer::new(move || {
                ApiServer::create_app_entry(
                    security.to_owned(),
                    cors.to_owned(),
                    endpoints.clone(),
                    cache.clone(),
                )
            })
            .bind(("0.0.0.0", self.port.to_owned()))?
            .shutdown_timeout(self.shutdown_timeout.to_owned())
            .run()
            .await
        })
    }
}
