use std::env;

use crate::{
    generator::protoc::generator::ProtoGenerator,
    test_utils::{self, get_sample_records},
};

use super::super::helper::query_response_to_typed_response;

#[test]
fn test_records_to_typed_response() {
    let res = env::current_dir().unwrap();
    let path = res.join("src/grpc/typed/tests/generated_films.bin");

    let (schema, _) = test_utils::get_schema();
    let service_desc = ProtoGenerator::read_schema(&path, "films").unwrap();

    let records = get_sample_records(schema);
    let res = query_response_to_typed_response(records, service_desc.query.response_desc.clone())
        .unwrap();
    let records = res
        .message
        .get_field_by_name(service_desc.query.response_desc.records_field.name());
    assert!(records.is_some(), "records must be present");
}
