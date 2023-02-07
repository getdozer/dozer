use super::generator::ProtoGenerator;
use crate::generator::protoc::utils::{create_descriptor_set, get_proto_descriptor};
use crate::test_utils;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use prost_reflect::{MethodDescriptor, ServiceDescriptor};
use tempdir::TempDir;

#[test]
fn test_generate_proto_and_descriptor() {
    let schema_name = "films";
    let schema = test_utils::get_schema().0;

    let endpoint = test_utils::get_endpoint();

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = tmp_dir.path();
    let api_security: Option<ApiSecurity> = None;
    let flags = Flags::default();

    ProtoGenerator::generate(
        tmp_dir_path,
        schema_name,
        schema,
        &api_security,
        &Some(flags),
    )
    .unwrap();

    let descriptor_path = create_descriptor_set(tmp_dir_path, &[endpoint.name]).unwrap();
    let (_, descriptor) = get_proto_descriptor(&descriptor_path).unwrap();

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
    let schema_name = "films";
    let schema = test_utils::get_schema().0;

    let endpoint = test_utils::get_endpoint();

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = tmp_dir.path();

    let api_security = Some(ApiSecurity::Jwt("vDKrSDOrVY".to_owned()));
    let flags = Flags::default();
    ProtoGenerator::generate(
        tmp_dir_path,
        schema_name,
        schema,
        &api_security,
        &Some(flags),
    )
    .unwrap();

    let descriptor_path = create_descriptor_set(tmp_dir_path, &[endpoint.name]).unwrap();
    let (_, descriptor) = get_proto_descriptor(&descriptor_path).unwrap();

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

#[test]
fn test_generate_proto_and_descriptor_with_push_event_off() {
    let schema_name = "films";
    let schema = test_utils::get_schema().0;

    let endpoint = test_utils::get_endpoint();

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = tmp_dir.path();
    let api_security = ApiSecurity::Jwt("vDKrSDOrVY".to_owned());
    ProtoGenerator::generate(
        tmp_dir_path,
        schema_name,
        schema,
        &Some(api_security),
        &None,
    )
    .unwrap();
    let descriptor_path = create_descriptor_set(tmp_dir_path, &[endpoint.name]).unwrap();
    let (_, descriptor) = get_proto_descriptor(&descriptor_path).unwrap();

    let msg = descriptor.get_message_by_name("dozer.generated.films.Film");
    let token_response = descriptor.get_message_by_name("dozer.generated.films.TokenResponse");
    let token_request = descriptor.get_message_by_name("dozer.generated.films.TokenRequest");
    let event_request = descriptor.get_message_by_name("dozer.generated.films.FilmEventRequest");
    let event_response = descriptor.get_message_by_name("dozer.generated.films.FilmEventResponse");
    let svcs: Vec<ServiceDescriptor> = descriptor.services().collect();
    let methods = svcs[0]
        .methods()
        .collect::<Vec<MethodDescriptor>>()
        .iter()
        .map(|m| m.name().to_string())
        .collect::<Vec<String>>();
    assert!(msg.is_some(), "descriptor is not decoded properly");
    assert!(
        token_request.is_some(),
        "Missing Token request generated with security config"
    );
    assert!(
        token_response.is_some(),
        "Missing Token response generated with security config"
    );
    assert!(
        event_request.is_none(),
        "Event request should not be generated with push_event flag off"
    );
    assert!(
        event_response.is_none(),
        "Event response should not be generated with push_event flag off"
    );
    assert!(svcs.len() == 1, "Only one service should be generated");
    assert!(
        methods.contains(&"query".to_string()),
        "query method should be generated"
    );
    assert!(
        methods.contains(&"token".to_string()),
        "token method should be generated"
    );
    assert!(
        !methods.contains(&"on_event".to_string()),
        "on_event method should not be generated"
    );
}
