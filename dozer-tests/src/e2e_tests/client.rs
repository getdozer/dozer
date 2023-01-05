use std::{net::SocketAddr, str::FromStr};

use dozer_api::{
    grpc::{
        common_grpc::{
            common_grpc_service_client::CommonGrpcServiceClient, GetEndpointsRequest,
            GetFieldsRequest,
        },
        health_grpc::{
            health_check_response::ServingStatus,
            health_grpc_service_client::HealthGrpcServiceClient, HealthCheckRequest,
        },
        types::Type,
    },
    tonic::transport::{Channel, Endpoint},
};
use dozer_types::{
    models::app_config::Config,
    types::{FieldDefinition, FieldType},
};

use super::expectation::{EndpointExpectation, Expectation};

pub struct Client {
    config: Config,
    rest_endpoint: SocketAddr,
    rest_client: reqwest::Client,
    health_grpc_client: HealthGrpcServiceClient<Channel>,
    common_grpc_client: CommonGrpcServiceClient<Channel>,
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
                "Health grpc client cannot connect to endpoint {:?}",
                grpc_endpoint
            ));
        let common_grpc_client = CommonGrpcServiceClient::connect(grpc_endpoint.clone())
            .await
            .expect(&format!(
                "Common grpc client cannot connect to endpoint {:?}",
                grpc_endpoint
            ));

        Self {
            config,
            rest_endpoint,
            rest_client: reqwest::Client::new(),
            health_grpc_client,
            common_grpc_client,
        }
    }

    pub async fn check_expectation(&mut self, expectation: &Expectation) {
        match expectation {
            Expectation::HealthyService => self.check_healthy_service().await,
            Expectation::Endpoint {
                endpoint,
                expectations,
            } => {
                self.check_endpoint_existence(endpoint).await;
                for expectation in expectations {
                    self.check_endpoint_expectation(endpoint, expectation).await;
                }
            }
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

    async fn check_endpoint_existence(&mut self, endpoint: &String) {
        // REST endpoint oapi.
        let path = &self
            .config
            .endpoints
            .iter()
            .find(|e| &e.name == endpoint)
            .expect(&format!("Cannot find endpoint {} in config", endpoint))
            .path;
        let response = self
            .rest_client
            .post(&format!("http://{}{}/oapi", self.rest_endpoint, path))
            .send()
            .await
            .expect(&format!(
                "Cannot get oapi response from rest endpoint {}, path is {}",
                endpoint, path
            ));
        let status = response.status();
        if !status.is_success() {
            panic!(
                "REST endpoint {} responds {}, path is {}",
                endpoint, status, path
            );
        }

        // Common service getEndpoints.
        let endpoints = self
            .common_grpc_client
            .get_endpoints(GetEndpointsRequest {})
            .await
            .expect("Cannot get endpoints from common grpc service")
            .into_inner()
            .endpoints;
        assert!(
            endpoints.contains(endpoint),
            "Endpoint {} is not found in common grpc service",
            endpoint
        );

        // TODO: Typed service endpoint.
    }

    async fn check_endpoint_expectation(
        &mut self,
        endpoint: &str,
        expectation: &EndpointExpectation,
    ) {
        match expectation {
            EndpointExpectation::Schema { fields } => {
                self.check_endpoint_schema(endpoint, fields).await;
            }
        }
    }

    async fn check_endpoint_schema(&mut self, endpoint: &str, fields: &[FieldDefinition]) {
        // TODO: check REST schema.

        // Common service getFields.
        let actual_fields = self
            .common_grpc_client
            .get_fields(GetFieldsRequest {
                endpoint: endpoint.to_string(),
            })
            .await
            .expect(&format!("Cannot get fields of endpoint {}", endpoint))
            .into_inner()
            .fields;
        assert_eq!(
            actual_fields.len(),
            fields.len(),
            "Check schema failed for endpoin {}, expected {} fields, got {}",
            endpoint,
            fields.len(),
            actual_fields.len()
        );
        for (actual_field, field) in actual_fields.iter().zip(fields.iter()) {
            assert_eq!(
                actual_field.name, field.name,
                "Check schema failed for endpoint {}, expected field name {}, got {}",
                endpoint, field.name, actual_field.name
            );
            assert!(
                type_matches(actual_field.typ, field.typ),
                "Check schema failed for endpoint {}, field {}, expected field type {}, got {}",
                endpoint,
                field.name,
                field.typ,
                actual_field.typ
            );
            assert_eq!(
                actual_field.nullable, field.nullable,
                "Check schema failed for endpoint {}, field {}, expected field nullable {}, got {}",
                field.name, endpoint, field.nullable, actual_field.nullable
            );
        }

        // TODO: Typed service schema.
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

fn type_matches(type1: i32, type2: FieldType) -> bool {
    match type2 {
        FieldType::UInt => type1 == Type::UInt as i32,
        FieldType::Int => type1 == Type::Int as i32,
        FieldType::Float => type1 == Type::Float as i32,
        FieldType::Boolean => type1 == Type::Boolean as i32,
        FieldType::String => type1 == Type::String as i32,
        FieldType::Text => type1 == Type::Text as i32,
        FieldType::Binary => type1 == Type::Binary as i32,
        FieldType::Decimal => type1 == Type::Decimal as i32,
        FieldType::Timestamp => type1 == Type::Timestamp as i32,
        FieldType::Date => type1 == Type::Date as i32,
        FieldType::Bson => type1 == Type::Bson as i32,
    }
}
