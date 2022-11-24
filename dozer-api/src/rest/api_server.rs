use super::api_generator;
use crate::{
    auth::{
        api::{auth_route, validate, ApiSecurity},
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
use dozer_types::crossbeam::channel::Sender;
use dozer_types::serde::{self, Deserialize, Serialize};
use futures_util::FutureExt;

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
                .fold(Cors::default(), |cors, origin| cors.allowed_origin(&origin))
                .max_age(max_age),
        }
    }

    pub fn create_app_entry(
        security: ApiSecurity,
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
        let app = App::new().wrap(Logger::default());

        // Injecting API Security
        let app = app.app_data(security.to_owned());

        let is_auth_configured = matches!(security, ApiSecurity::Jwt(_));
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
                        .route("/", web::get().to(api_generator::list)),
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

    pub fn run(
        &self,
        cache_endpoints: Vec<CacheEndpoint>,
        tx: Sender<ServerHandle>,
    ) -> std::io::Result<()> {
        let cors = self.cors.clone();
        let security = self.security.clone();
        let server = HttpServer::new(move || {
            ApiServer::create_app_entry(
                security.to_owned(),
                cors.to_owned(),
                cache_endpoints.clone(),
            )
        })
        .bind(("0.0.0.0", self.port.to_owned()))?
        .shutdown_timeout(self.shutdown_timeout.to_owned())
        .run();

        let _ = tx.send(server.handle());
        rt::System::new().block_on(async move { server.await })
    }

    pub fn stop(server_handle: ServerHandle) {
        rt::System::new().block_on(server_handle.stop(true));
    }
}
