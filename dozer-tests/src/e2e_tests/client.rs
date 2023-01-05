use std::{net::SocketAddr, str::FromStr};

use dozer_api::{
    grpc::health_grpc::{
        health_check_response::ServingStatus, health_grpc_service_client::HealthGrpcServiceClient,
        HealthCheckRequest,
    },
    tonic::transport::{Channel, Endpoint},
};
use dozer_types::models::app_config::Config;

use super::expectation::Expectation;

pub struct Client {
    config: Config,
    rest_endpoint: SocketAddr,
    rest_client: reqwest::Client,
    health_grpc_client: HealthGrpcServiceClient<Channel>,
}

impl Client {
    pub async fn new(config: Config) -> Self {
        let api = config.api.clone().unwrap_or_default();

        let rest = api.rest.unwrap_or_default();
        let rest_endpoint = SocketAddr::from_str(&format!("{}:{}", rest.host, rest.port))
            .expect(&format!("Bad rest endpoint: {}:{}", rest.host, rest.port));

        let grpc = api.grpc.unwrap_or_default();
        let grpc_endpoint = format!("http://{}:{}", grpc.host, grpc.port);
        let grpc_endpoint = Endpoint::from_shared(grpc_endpoint.clone())
            .expect(&format!("Invalid grpc endpoint {}", grpc_endpoint));
        let health_grpc_client = HealthGrpcServiceClient::connect(grpc_endpoint.clone())
            .await
            .expect(&format!(
                "Cannot connect to grpc endpoint {:?}",
                grpc_endpoint
            ));

        Self {
            config,
            rest_endpoint,
            rest_client: reqwest::Client::new(),
            health_grpc_client,
        }
    }

    pub async fn check_expectation(&mut self, expectation: &Expectation) {
        match expectation {
            Expectation::HealthyService => self.check_healthy_service().await,
        }
    }

    async fn check_healthy_service(&mut self) {
        // REST health.
        let response = self
            .rest_client
            .get(&format!("http://{}/health", self.rest_endpoint))
            .send()
            .await
            .expect("Cannot get response from rest health endpoint");
        let status = response.status();
        if !status.is_success() {
            panic!("REST health endpoint responds {}", status);
        }

        // gRPC health.
        let services = if self.config.flags.clone().unwrap_or_default().dynamic {
            vec!["common", "typed", ""]
        } else {
            vec!["common", ""]
        };
        for service in services {
            check_grpc_health(&mut self.health_grpc_client, service.to_string()).await;
        }
    }
}

async fn check_grpc_health(client: &mut HealthGrpcServiceClient<Channel>, service: String) {
    let response = client
        .health_check(HealthCheckRequest {
            service: service.clone(),
        })
        .await
        .expect(&format!(
            "Cannot get response from grpc health endpoint for service {}",
            service,
        ));
    let status = response.into_inner().status;
    if status != ServingStatus::Serving as i32 {
        panic!(
            "gRPC health endpoint responds with not SERVING status {} for service {}",
            status, service
        );
    }
}
