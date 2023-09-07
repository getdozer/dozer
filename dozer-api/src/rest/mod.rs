use std::sync::Arc;

use crate::api_helper::get_api_security;
// Exports
use crate::errors::ApiInitError;
use crate::rest::api_generator::health_route;
use crate::{
    auth::api::{auth_route, validate},
    CacheEndpoint,
};
use actix_cors::Cors;
use actix_web::dev::Server;
use actix_web::middleware::DefaultHeaders;
use actix_web::web::PayloadConfig;
use actix_web::{
    body::MessageBody,
    dev::{Service, ServiceFactory, ServiceRequest, ServiceResponse},
    middleware::{Condition, Logger},
    web, App, HttpMessage, HttpServer,
};
use actix_web_httpauth::middleware::HttpAuthentication;
use dozer_tracing::LabelsAndProgress;
use dozer_types::{log::info, models::api_config::RestApiOptions};
use dozer_types::{
    models::api_security::ApiSecurity,
    serde::{self, Deserialize, Serialize},
};
use futures_util::Future;
use tracing_actix_web::TracingLogger;

mod api_generator;
mod rest_metric_middleware;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone)]
#[serde(crate = "self::serde")]
enum CorsOptions {
    Permissive,
    // origins, max_age
    Custom(Vec<String>, usize),
}

pub const DOZER_SERVER_NAME_HEADER: &str = "x-dozer-server-name";

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
    pub fn new(rest_config: RestApiOptions, security: Option<ApiSecurity>) -> Self {
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

    fn create_app_entry(
        security: Option<ApiSecurity>,
        cors: CorsOptions,
        mut cache_endpoints: Vec<Arc<CacheEndpoint>>,
        labels: LabelsAndProgress,
    ) -> App<
        impl ServiceFactory<
            ServiceRequest,
            Response = ServiceResponse<impl MessageBody>,
            Config = (),
            InitError = (),
            Error = actix_web::Error,
        >,
    > {
        let endpoint_paths: Vec<String> = cache_endpoints
            .iter()
            .map(|cache_endpoint| cache_endpoint.endpoint.path.clone())
            .collect();
        let cfg = PayloadConfig::default();
        let mut app = App::new()
            .app_data(web::Data::new(endpoint_paths))
            .app_data(cfg)
            .wrap(Logger::default())
            .wrap(TracingLogger::default())
            .wrap(DefaultHeaders::new().add((
                DOZER_SERVER_NAME_HEADER,
                gethostname::gethostname().to_string_lossy().into_owned(),
            )));

        let is_auth_configured = if let Some(api_security) = security {
            // Injecting API Security
            app = app.app_data(api_security);
            true
        } else {
            false
        };
        let auth_middleware =
            Condition::new(is_auth_configured, HttpAuthentication::bearer(validate));

        let cors_middleware = Self::get_cors(cors);

        //reverse sort cache endpoints by path length to ensure that the most specific path is matched first
        cache_endpoints.sort_by(|a, b| b.endpoint.path.len().cmp(&a.endpoint.path.len()));

        cache_endpoints
            .into_iter()
            .fold(app, |app, cache_endpoint| {
                let endpoint = &cache_endpoint.endpoint;
                let scope = &endpoint.path;
                app.service(
                    web::scope(scope)
                        .wrap(rest_metric_middleware::RestMetric::new(labels.clone()))
                        // Inject cache_endpoint for generated functions
                        .wrap_fn(move |req, srv| {
                            req.extensions_mut().insert(cache_endpoint.clone());
                            srv.call(req)
                        })
                        .route("/count", web::post().to(api_generator::count))
                        .route("/query", web::post().to(api_generator::query))
                        .route("/phase", web::post().to(api_generator::get_phase))
                        .route("/oapi", web::post().to(api_generator::generate_oapi))
                        .route("/{id}", web::get().to(api_generator::get))
                        .route("/", web::get().to(api_generator::list))
                        .route("", web::get().to(api_generator::list)),
                )
            })
            // Attach token generation route
            .route("/auth/token", web::post().to(auth_route))
            // Attach health route
            .route("/health", web::get().to(health_route))
            .route("/", web::get().to(list_endpoint_paths))
            .route("", web::get().to(list_endpoint_paths))
            // Wrap Api Validator
            .wrap(auth_middleware)
            // Wrap CORS around api validator. Required to return the right headers.
            .wrap(cors_middleware)
    }

    pub fn run(
        self,
        cache_endpoints: Vec<Arc<CacheEndpoint>>,
        shutdown: impl Future<Output = ()> + Send + 'static,
        labels: LabelsAndProgress,
    ) -> Result<Server, ApiInitError> {
        let security = get_api_security(self.security.to_owned());
        info!(
            "Starting Rest Api Server on http://{}:{} with security: {}",
            self.host,
            self.port,
            security.as_ref().map_or("None".to_string(), |s| match s {
                ApiSecurity::Jwt(_) => "JWT".to_string(),
            })
        );
        let cors = self.cors;

        let address = format!("{}:{}", self.host, self.port);
        let server = HttpServer::new(move || {
            ApiServer::create_app_entry(
                security.clone(),
                cors.clone(),
                cache_endpoints.clone(),
                labels.clone(),
            )
        })
        .bind(&address)
        .map_err(|e| ApiInitError::FailedToBindToAddress(address, e))?
        .disable_signals()
        .shutdown_timeout(self.shutdown_timeout)
        .run();

        let server_handle = server.handle();
        tokio::spawn(async move {
            shutdown.await;
            server_handle.stop(true).await;
        });

        Ok(server)
    }
}

async fn list_endpoint_paths(endpoints: web::Data<Vec<String>>) -> web::Json<Vec<String>> {
    web::Json(endpoints.get_ref().clone())
}

#[cfg(test)]
mod tests;
