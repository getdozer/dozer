use dozer_types::{
    models::{api_config::default_api_config, app_config::Config},
    serde_yaml,
};

use crate::cli::tests::helper::*;

#[test]
fn test_deserialize_config() {
    let test_str = test_yml_content_full();
    let deserializer_result = serde_yaml::from_str::<Config>(test_str).unwrap();
    let expected = test_config();
    assert_eq!(deserializer_result.api, expected.api);
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_deserialize_default_api_config() {
    let test_str = test_yml_content_missing_api_config();
    let deserializer_result = serde_yaml::from_str::<Config>(test_str).unwrap();
    let expected = test_config();
    let default_api_config = default_api_config();
    assert_eq!(deserializer_result.api, Some(default_api_config));
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_deserialize_yaml_missing_internal_config() {
    let test_str = test_yml_content_missing_internal_config();
    let deserializer_result = serde_yaml::from_str::<Config>(test_str).unwrap();
    let expected = test_config();
    let default_api_config = default_api_config();
    assert_eq!(deserializer_result.api, Some(default_api_config.to_owned()));
    assert_eq!(
        deserializer_result
            .api
            .to_owned()
            .unwrap()
            .api_internal
            .unwrap(),
        default_api_config.api_internal.unwrap()
    );
    assert_eq!(
        deserializer_result
            .api
            .to_owned()
            .unwrap()
            .pipeline_internal
            .unwrap(),
        default_api_config.pipeline_internal.unwrap()
    );
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_serialize_config() {
    let config = test_config();
    let serialize_yml = serde_yaml::to_string(&config).unwrap();
    let deserializer_result = serde_yaml::from_str::<Config>(&serialize_yml).unwrap();
    assert_eq!(deserializer_result, config);
}
