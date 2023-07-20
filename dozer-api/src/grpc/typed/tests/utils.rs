use super::super::helper::count_response_to_typed_response;
use super::super::helper::query_response_to_typed_response;
use crate::{generator::protoc::generator::ProtoGenerator, test_utils::get_sample_records};
use std::env;

#[test]
fn test_records_to_typed_response() {
    let res = env::current_dir().unwrap();
    let path = res.join("src/grpc/typed/tests/generated_films.bin");
    let bytes = std::fs::read(path).unwrap();

    let service_desc = ProtoGenerator::read_schema(&bytes, "films").unwrap();

    let records = get_sample_records();
    let res = query_response_to_typed_response(records, service_desc.query.response_desc.clone())
        .unwrap();
    let records = res
        .message
        .get_field_by_name(service_desc.query.response_desc.records_field.name());
    assert!(records.is_some(), "records must be present");
}

#[test]
fn test_count_records_to_typed_response() {
    let res = env::current_dir().unwrap();
    let path = res.join("src/grpc/typed/tests/generated_films.bin");
    let bytes = std::fs::read(path).unwrap();

    let service_desc = ProtoGenerator::read_schema(&bytes, "films").unwrap();

    let records = get_sample_records();
    let res =
        count_response_to_typed_response(records.len(), service_desc.count.response_desc.clone())
            .unwrap();
    let res_records = res
        .message
        .get_field_by_name(service_desc.count.response_desc.count_field.name());
    assert_eq!(records.len() as u64, res_records.unwrap().as_u64().unwrap());
}
