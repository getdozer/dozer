use std::{env, path::PathBuf};

use crate::{
    generator::protoc::utils::get_proto_descriptor,
    test_utils::{self, get_sample_records},
};

use super::super::helper::query_response_to_typed_response;

#[test]
fn test_records_to_typed_response() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let path = out_dir.join("generated_films.bin");

    let (_, desc) = get_proto_descriptor(&path).unwrap();

    let (schema, _) = test_utils::get_schema();
    let endpoint_name = "films".to_string();

    let records = get_sample_records(schema);
    let res = query_response_to_typed_response(records, &desc, &endpoint_name);
    let data = res.message.get_field_by_name("data");
    assert!(data.is_some(), "data must be present");
}
