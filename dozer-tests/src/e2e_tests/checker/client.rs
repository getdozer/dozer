use dozer_api::{
    openapiv3::{OpenAPI, ReferenceOr, SchemaKind, StringFormat, VariantOrUnknownOrEmpty},
    tonic::transport::{Channel, Endpoint},
};
use dozer_types::{
    grpc_types::{
        common::{
            common_grpc_service_client::CommonGrpcServiceClient, GetEndpointsRequest,
            GetFieldsRequest,
        },
        health::{
            health_check_response::ServingStatus,
            health_grpc_service_client::HealthGrpcServiceClient, HealthCheckRequest,
        },
    },
    models::app_config::Config,
    types::{FieldDefinition, FieldType, DATE_FORMAT},
};

use super::super::expectation::{EndpointExpectation, Expectation};

pub struct Client {
    config: Config,
    rest_endpoint: String,
    rest_client: reqwest::Client,
    health_grpc_client: HealthGrpcServiceClient<Channel>,
    common_grpc_client: CommonGrpcServiceClient<Channel>,
}

impl Client {
    pub async fn new(config: Config) -> Self {
        let api = config.api.clone().unwrap_or_default();

        let rest = api.rest.unwrap_or_default();
        let rest_endpoint = format!("http://{}:{}", rest.host, rest.port);

        let grpc = api.grpc.unwrap_or_default();
        let grpc_endpoint_string = format!("http://{}:{}", grpc.host, grpc.port);
        let grpc_endpoint = Endpoint::from_shared(grpc_endpoint_string.clone())
            .unwrap_or_else(|e| panic!("Invalid grpc endpoint {grpc_endpoint_string}: {e}"));

        let health_grpc_client = HealthGrpcServiceClient::connect(grpc_endpoint.clone())
            .await
            .unwrap_or_else(|e| {
                panic!("Health grpc client cannot connect to endpoint {grpc_endpoint_string}: {e}")
            });
        let common_grpc_client = CommonGrpcServiceClient::connect(grpc_endpoint.clone())
            .await
            .unwrap_or_else(|e| {
                panic!("Common grpc client cannot connect to endpoint {grpc_endpoint_string}: {e}")
            });

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
                let rest_path = self
                    .config
                    .endpoints
                    .iter()
                    .find(|e| &e.name == endpoint)
                    .unwrap_or_else(|| panic!("Cannot find endpoint {endpoint} in config"))
                    .path
                    .clone();
                self.check_endpoint_existence(endpoint, &rest_path).await;
                for expectation in expectations {
                    self.check_endpoint_expectation(endpoint, &rest_path, expectation)
                        .await;
                }
            }
        }
    }

    async fn check_healthy_service(&mut self) {
        // REST health.
        let response = self
            .rest_client
            .get(&format!("{}/health", self.rest_endpoint))
            .send()
            .await
            .expect("Cannot get response from rest health endpoint");
        let status = response.status();
        if !status.is_success() {
            panic!("REST health endpoint responds {status}");
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

    async fn check_endpoint_existence(&mut self, endpoint: &String, rest_path: &str) {
        // REST endpoint oapi.
        let response = self
            .rest_client
            .post(&format!("{}{}/oapi", self.rest_endpoint, rest_path))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Cannot get oapi response from rest endpoint {endpoint}, path is {rest_path}: {e}"
                )
            });
        let status = response.status();
        if !status.is_success() {
            panic!("REST oapi endpoint {endpoint} responds {status}, path is {rest_path}");
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
            "Endpoint {endpoint} is not found in common grpc service"
        );

        // TODO: Typed service endpoint.
    }

    async fn check_endpoint_expectation(
        &mut self,
        endpoint: &str,
        rest_path: &str,
        expectation: &EndpointExpectation,
    ) {
        match expectation {
            EndpointExpectation::Schema { fields } => {
                self.check_endpoint_schema(endpoint, rest_path, fields)
                    .await;
            }
        }
    }

    async fn check_endpoint_schema(
        &mut self,
        endpoint: &str,
        rest_path: &str,
        fields: &[FieldDefinition],
    ) {
        // REST OpenAPI schema.
        let response = self
            .rest_client
            .post(&format!("{}{}/oapi", self.rest_endpoint, rest_path))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Cannot get oapi response from rest endpoint {endpoint}, path is {rest_path}: {e}"
                )
            });
        let status = response.status();
        if !status.is_success() {
            panic!("REST oapi endpoint {endpoint} responds {status}, path is {rest_path}");
        }
        let open_api: OpenAPI = response.json().await.unwrap_or_else(|e| {
            panic!(
                "Cannot parse oapi response from rest endpoint {endpoint}, path is {rest_path}: {e}"
            )
        });
        let schema = open_api
            .components
            .as_ref()
            .unwrap_or_else(|| {
                panic!(
                    "Cannot find components in oapi response from rest endpoint {endpoint}, path is {rest_path}"
                )
            })
            .schemas
            .get(endpoint)
            .unwrap_or_else(|| {
                panic!(
                    "Cannot find schema for endpoint {endpoint} in oapi response, path is {rest_path}"
                )
            });
        let schema = schema.as_item().unwrap_or_else(|| {
            panic!(
                "Expecting schema item for endpoint {endpoint} in oapi response, path is {rest_path}"
            )
        });
        let (properties, required) = match &schema.schema_kind {
            SchemaKind::Type(dozer_api::openapiv3::Type::Object(object_type)) => {
                (&object_type.properties, &object_type.required)
            }
            _ => panic!(
                "Expecting object schema for endpoint {endpoint} in oapi response, path is {rest_path}"
            ),
        };
        assert_eq!(
            properties.len(),
            fields.len(),
            "Check REST schema failed for endpoint {}, expected {} fields, got {}",
            endpoint,
            fields.len(),
            properties.len()
        );
        for (property, field) in properties.iter().zip(fields.iter()) {
            assert_eq!(
                property.0, &field.name,
                "Check REST schema failed for endpoint {}, expected field name {}, got {}",
                endpoint, field.name, property.0
            );
            let schema = property.1.as_item().unwrap_or_else(|| {
                panic!(
                    "Expecting schema item for endpoint {}, field {} in oapi response, path is {}",
                    endpoint, field.name, rest_path
                )
            });
            let oapi_type = match &schema.schema_kind {
                SchemaKind::Type(oapi_type) => oapi_type,
                _ => panic!(
                    "Expecting type schema for endpoint {}, field {} in oapi response, path is {}",
                    endpoint, field.name, rest_path
                ),
            };
            assert!(
                oapi_type_matches(oapi_type, field.typ),
                "Check REST schema failed for endpoint {}, field {}, expected field type {}, got {:?}",
                endpoint,
                field.name,
                field.typ,
                oapi_type
            );
            if field.nullable {
                assert!(!required.contains(&field.name), "Check REST schema failed for endpoint {}, field {} is nullable, but it is required", endpoint, field.name);
            } else {
                assert!(required.contains(&field.name), "Check REST schema failed for endpoint {}, field {} is not nullable, but it is not required", endpoint, field.name);
            }
        }

        // Common service getFields.
        let actual_fields = self
            .common_grpc_client
            .get_fields(GetFieldsRequest {
                endpoint: endpoint.to_string(),
            })
            .await
            .unwrap_or_else(|e| panic!("Cannot get fields of endpoint {endpoint}: {e}"))
            .into_inner()
            .fields;
        assert_eq!(
            actual_fields.len(),
            fields.len(),
            "Check common gRPC schema failed for endpoint {}, expected {} fields, got {}",
            endpoint,
            fields.len(),
            actual_fields.len()
        );
        for (actual_field, field) in actual_fields.iter().zip(fields.iter()) {
            assert_eq!(
                actual_field.name, field.name,
                "Check common gRPC schema failed for endpoint {}, expected field name {}, got {}",
                endpoint, field.name, actual_field.name
            );
            assert!(
                grpc_type_matches(actual_field.typ, field.typ),
                "Check common gRPC schema failed for endpoint {}, field {}, expected field type {}, got {}",
                endpoint,
                field.name,
                field.typ,
                actual_field.typ
            );
            assert_eq!(
                actual_field.nullable,
                field.nullable,
                "Check common gRPC schema failed for endpoint {}, field {}, expected field nullable {}, got {}",
                endpoint,
                field.name,
                field.nullable,
                actual_field.nullable
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
        .unwrap_or_else(|e| {
            panic!("Cannot get response from grpc health endpoint for service {service}: {e}")
        });
    let status = response.into_inner().status;
    if status != ServingStatus::Serving as i32 {
        panic!(
            "gRPC health endpoint responds with not SERVING status {status} for service {service}"
        );
    }
}

fn grpc_type_matches(grpc_type: i32, field_type: FieldType) -> bool {
    use dozer_types::grpc_types::types::Type;

    match field_type {
        FieldType::UInt => grpc_type == Type::UInt as i32,
        FieldType::U128 => grpc_type == Type::UInt as i32,
        FieldType::Int => grpc_type == Type::Int as i32,
        FieldType::I128 => grpc_type == Type::Int as i32,
        FieldType::Float => grpc_type == Type::Float as i32,
        FieldType::Boolean => grpc_type == Type::Boolean as i32,
        FieldType::String => grpc_type == Type::String as i32,
        FieldType::Text => grpc_type == Type::Text as i32,
        FieldType::Binary => grpc_type == Type::Binary as i32,
        FieldType::Decimal => grpc_type == Type::Decimal as i32,
        FieldType::Timestamp => grpc_type == Type::Timestamp as i32,
        FieldType::Date => grpc_type == Type::Date as i32,
        FieldType::Json => grpc_type == Type::Bson as i32,
        FieldType::Point => grpc_type == Type::Point as i32,
        FieldType::Duration => grpc_type == Type::Duration as i32,
    }
}

fn oapi_type_matches(oapi_type: &dozer_api::openapiv3::Type, field_type: FieldType) -> bool {
    use dozer_api::openapiv3::Type::{Array, Boolean, Integer, Number, String};

    match (oapi_type, field_type) {
        (Integer(_), FieldType::UInt | FieldType::U128 | FieldType::Int | FieldType::I128) => true,
        (Number(_), FieldType::Float) => true,
        (Boolean {}, FieldType::Boolean) => true,
        (
            String(string_type),
            FieldType::String
            | FieldType::Text
            | FieldType::Decimal
            | FieldType::Timestamp
            | FieldType::Date,
        ) => {
            if field_type == FieldType::Timestamp {
                string_type.format == VariantOrUnknownOrEmpty::Item(StringFormat::DateTime)
                    && string_type.pattern == Some("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'".to_string())
            } else if field_type == FieldType::Date {
                string_type.format == VariantOrUnknownOrEmpty::Item(StringFormat::Date)
                    && string_type.pattern == Some(DATE_FORMAT.to_string())
            } else {
                true
            }
        }
        (Array(array_type), FieldType::Binary | FieldType::Json) => {
            let Some(ReferenceOr::Item(schema)) = array_type.items.as_ref() else {
                return false;
            };
            matches!(schema.schema_kind, SchemaKind::Type(Integer(_)))
        }
        _ => false,
    }
}
