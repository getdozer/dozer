use super::api_generator;
use crate::{
    auth::{
        api::{auth_route, validate},
        Access,
    },
    CacheEndpoint, PipelineDetails,
};
use actix_cors::Cors;
use actix_web::{
    body::MessageBody,
    dev::{ServerHandle, Service, ServiceFactory, ServiceRequest, ServiceResponse},
    middleware::{Condition, Logger},
    rt, web, App, HttpMessage, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use dozer_types::{crossbeam::channel::Sender, log::info, models::api_config::ApiRest};
use dozer_types::{
    models::api_security::ApiSecurity,
    serde::{self, Deserialize, Serialize},
};
use futures_util::FutureExt;
use tracing_actix_web::TracingLogger;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(crate = "self::serde")]
pub enum CorsOptions {
    Permissive,
    // origins, max_age
    Custom(Vec<String>, usize),
}
#[derive(Clone)]
pub struct ApiServer {
    shutdown_timeout: u64,
    port: u16,
    cors: CorsOptions,
    security: Option<ApiSecurity>,
    host: String,
}

impl Default for ApiServer {
    fn default() -> Self {
        Self {
            shutdown_timeout: 0,
            port: 8080,
            cors: CorsOptions::Permissive,
            security: None,
            host: "0.0.0.0".to_owned(),
        }
    }
}

impl ApiServer {
    pub fn new(rest_config: ApiRest, security: Option<ApiSecurity>) -> Self {
        Self {
            shutdown_timeout: 0,
            port: rest_config.port as u16,
            cors: CorsOptions::Permissive,
            security,
            host: rest_config.host,
        }
    }
    fn get_cors(cors: CorsOptions) -> Cors {
        match cors {
            CorsOptions::Permissive => Cors::permissive(),
            CorsOptions::Custom(origins, max_age) => origins
                .into_iter()
                .fold(Cors::default(), |cors, origin| cors.allowed_origin(&origin))
                .max_age(max_age),
        }
    }

    pub fn create_app_entry(
        security: Option<ApiSecurity>,
        cors: CorsOptions,
        cache_endpoints: Vec<CacheEndpoint>,
    ) -> App<
        impl ServiceFactory<
            ServiceRequest,
            Response = ServiceResponse<impl MessageBody>,
            Config = (),
            InitError = (),
            Error = actix_web::Error,
        >,
    > {
        let mut app = App::new()
            .wrap(Logger::default())
            .wrap(TracingLogger::default());

        if let Some(api_security) = security.to_owned() {
            // Injecting API Security
            app = app.app_data(api_security);
        }
        let is_auth_configured = security.is_some();
        let auth_middleware =
            Condition::new(is_auth_configured, HttpAuthentication::bearer(validate));

        let cors_middleware = Self::get_cors(cors);

        cache_endpoints
            .iter()
            .cloned()
            .fold(app, |app, cache_endpoint| {
                let endpoint = cache_endpoint.endpoint.clone();
                let scope = endpoint.path.clone();
                let schema_name = endpoint.name;
                app.service(
                    web::scope(&scope)
                        // Inject pipeline_details for generated functions
                        .wrap_fn(move |req, srv| {
                            req.extensions_mut().insert(PipelineDetails {
                                schema_name: schema_name.to_owned(),
                                cache_endpoint: cache_endpoint.clone(),
                            });
                            srv.call(req)
                        })
                        .route("/query", web::post().to(api_generator::query))
                        .route("/oapi", web::post().to(api_generator::generate_oapi))
                        .route("/{id}", web::get().to(api_generator::get))
                        .route("/", web::get().to(api_generator::list))
                        .route("", web::get().to(api_generator::list)),
                )
            })
            // Attach token generation route
            .route("/auth/token", web::post().to(auth_route))
            // Wrap Api Validator
            .wrap(auth_middleware)
            // Insert None as Auth when no apisecurity configured
            .wrap_fn(move |req, srv| {
                if !is_auth_configured {
                    req.extensions_mut().insert(Access::All);
                }
                srv.call(req).map(|res| res)
            })
            // Wrap CORS around api validator. Neededto return the right headers.
            .wrap(cors_middleware)
    }

    pub async fn run(
        &self,
        cache_endpoints: Vec<CacheEndpoint>,
        tx: Sender<ServerHandle>,
    ) -> std::io::Result<()> {
        info!(
            "Starting Rest Api Server on host: {}, port: {}, security: {}",
            self.host,
            self.port,
            self.security
                .as_ref()
                .map_or("None".to_string(), |s| match s {
                    ApiSecurity::Jwt(_) => "JWT".to_string(),
                })
        );
        let cors = self.cors.clone();
        let security = self.security.clone();
        let server = HttpServer::new(move || {
            ApiServer::create_app_entry(
                security.to_owned(),
                cors.to_owned(),
                cache_endpoints.clone(),
            )
        })
        .bind(format!("{}:{}", self.host.to_owned(), self.port.to_owned()))?
        .shutdown_timeout(self.shutdown_timeout.to_owned())
        .run();

        let _ = tx.send(server.handle());
        server.await
    }

    pub fn stop(server_handle: ServerHandle) {
        rt::System::new().block_on(server_handle.stop(true));
    }
}
