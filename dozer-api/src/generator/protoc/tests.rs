use super::generator::ProtoGenerator;
use crate::generator::protoc::utils::{create_descriptor_set, get_proto_descriptor};
use crate::{test_utils, CacheEndpoint, PipelineDetails};
use dozer_types::models::api_security::ApiSecurity;
use std::collections::HashMap;
use tempdir::TempDir;

#[test]
fn test_generate_proto_and_descriptor() {
    let schema_name = "films".to_string();
    let schema = test_utils::get_schema();

    let endpoint = test_utils::get_endpoint();

    let mut map = HashMap::new();
    let details = PipelineDetails {
        schema_name: schema_name.clone(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, Some(schema)),
            endpoint: endpoint.clone(),
        },
    };
    map.insert(schema_name, details.clone());

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());
    let api_security: Option<ApiSecurity> = None;

    ProtoGenerator::generate(tmp_dir_path.to_owned(), details, &api_security).unwrap();

    let descriptor_path = create_descriptor_set(tmp_dir_path, &[endpoint.name]).unwrap();
    let (_, descriptor) = get_proto_descriptor(descriptor_path).unwrap();

    let msg = descriptor.get_message_by_name("dozer.generated.films.Film");
    let token_response = descriptor.get_message_by_name("dozer.generated.films.TokenResponse");
    let token_request = descriptor.get_message_by_name("dozer.generated.films.TokenRequest");

    assert!(msg.is_some(), "descriptor is not decoded properly");
    assert!(
        token_request.is_none(),
        "Token request should not be generated with empty security config"
    );
    assert!(
        token_response.is_none(),
        "Token response should not be generated with empty security config"
    );
}

#[test]
fn test_generate_proto_and_descriptor_with_security() {
    let schema_name = "films".to_string();
    let schema = test_utils::get_schema();

    let endpoint = test_utils::get_endpoint();

    let mut map = HashMap::new();
    let details = PipelineDetails {
        schema_name: schema_name.clone(),
        cache_endpoint: CacheEndpoint {
            cache: test_utils::initialize_cache(&schema_name, Some(schema)),
            endpoint: endpoint.clone(),
        },
    };
    map.insert(schema_name, details.clone());

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());

    let api_security = Some(ApiSecurity::Jwt("vDKrSDOrVY".to_owned()));

    ProtoGenerator::generate(tmp_dir_path.to_owned(), details, &api_security).unwrap();

    let descriptor_path = create_descriptor_set(tmp_dir_path, &[endpoint.name]).unwrap();
    let (_, descriptor) = get_proto_descriptor(descriptor_path).unwrap();

    let msg = descriptor.get_message_by_name("dozer.generated.films.Film");
    let token_response = descriptor.get_message_by_name("dozer.generated.films.TokenResponse");
    let token_request = descriptor.get_message_by_name("dozer.generated.films.TokenRequest");
    assert!(msg.is_some(), "descriptor is not decoded properly");
    assert!(
        token_request.is_some(),
        "Missing Token request generated with security config"
    );
    assert!(
        token_response.is_some(),
        "Missing Token response generated with security config"
    );
}
