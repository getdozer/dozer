use std::path::Path;

use super::generator::{ProtoGenerator, ServiceDesc};
use crate::test_utils;
use dozer_types::models::api_security::ApiSecurity;
use dozer_types::models::flags::Flags;
use tempdir::TempDir;

fn read_service_desc(proto_folder_path: &Path, endpoint_name: &str) -> ServiceDesc {
    let descriptor_path = proto_folder_path.join("descriptor.bin");
    ProtoGenerator::generate_descriptor(proto_folder_path, &descriptor_path, &[endpoint_name])
        .unwrap();
    ProtoGenerator::read_schema(&descriptor_path, endpoint_name).unwrap()
}

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
        &schema,
        &api_security,
        &Some(flags),
    )
    .unwrap();

    let service_desc = read_service_desc(tmp_dir_path, &endpoint.name);

    assert_eq!(
        service_desc
            .query
            .response_desc
            .record_with_id_desc
            .record_desc
            .message
            .full_name(),
        "dozer.generated.films.Film"
    );
    assert!(service_desc.token.is_none());
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
        &schema,
        &api_security,
        &Some(flags),
    )
    .unwrap();

    let service_desc = read_service_desc(tmp_dir_path, &endpoint.name);

    assert_eq!(
        service_desc
            .query
            .response_desc
            .record_with_id_desc
            .record_desc
            .message
            .full_name(),
        "dozer.generated.films.Film"
    );
    assert_eq!(
        service_desc
            .token
            .unwrap()
            .response_desc
            .message
            .full_name(),
        "dozer.generated.films.TokenResponse"
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
        &schema,
        &Some(api_security),
        &None,
    )
    .unwrap();

    let service_desc = read_service_desc(tmp_dir_path, &endpoint.name);

    assert_eq!(
        service_desc
            .query
            .response_desc
            .record_with_id_desc
            .record_desc
            .message
            .full_name(),
        "dozer.generated.films.Film"
    );
    assert_eq!(
        service_desc
            .token
            .unwrap()
            .response_desc
            .message
            .full_name(),
        "dozer.generated.films.TokenResponse"
    );
    assert!(service_desc.on_event.is_none());
}
