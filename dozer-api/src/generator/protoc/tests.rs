use super::generator::ProtoGenerator;
use crate::{test_utils, CacheEndpoint, PipelineDetails};
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
            endpoint,
        },
    };
    map.insert(schema_name, details);

    let tmp_dir = TempDir::new("proto_generated").unwrap();
    let tmp_dir_path = String::from(tmp_dir.path().to_str().unwrap());

    let res = ProtoGenerator::generate(tmp_dir_path, map, None).unwrap();

    let msg = res
        .descriptor
        .get_message_by_name("dozer.generated.films.Film");
    assert!(msg.is_some(), "descriptor is not decoded properly");
}
