use crate::models::config::Config;
use crate::models::udf::{OnnxConfig, UdfConfig, UdfTypeConfig};

#[test]
fn standard() {
    let udf_config = r#"
    udf:
      name: is_fraudolent
      config: !Onnx
        path: ./models/mode_file
  "#;
    let deserializer_result = serde_yaml::from_str::<Config>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: Some(UdfTypeConfig::Onnx(OnnxConfig {
            path: Some("./models/mode_file".to_string()),
        })),
        name: Some("is_fraudolent".to_string()),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);

    if let UdfConfig{ config, name } = expected {
        let expected_config = config.unwrap();
        if let UdfConfig{ config: deserialized_config, name: deserialized_name } = deserializer_result {
            let deserialized = deserialized_config.unwrap();
            assert_eq!(expected_config, deserialized);
            assert_eq!(name, deserialized_name);
        }
    }
}
