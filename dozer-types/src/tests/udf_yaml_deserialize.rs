use crate::models::config::Config;
use crate::models::udf_config::{OnnxConfig, UdfConfig, UdfType, WasmConfig};

#[test]
fn standard() {
    let udf_config = r#"
    name: is_fraudulent
    config: !Onnx
      path: ./models/model_file
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: UdfType::Onnx(OnnxConfig {
            path: "./models/model_file".to_string(),
        }),
        name: "is_fraudulent".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}

#[test]
fn wasm_udf_tagged() {
    let udf_config = r#"
    name: my_function
    config: !Wasm
      path: udfs/wasm/my_function
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let expected = UdfConfig {
        name: "my_function".to_string(),
        config: UdfType::Wasm(WasmConfig {
            path: "udfs/wasm/my_function".to_string(),
            function: None,
        }),
    };
    assert_eq!(expected, deserializer_result);
}

#[test]
fn wasm_udf_map_in_config() {
    let config_yaml = r#"
version: 1
app_name: wasm_test_app
udfs:
  wasm:
    my_function: udfs/wasm/my_function
"#;
    let config = serde_yaml::from_str::<Config>(config_yaml).unwrap();
    assert_eq!(
        config.udfs,
        vec![UdfConfig {
            name: "my_function".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: "udfs/wasm/my_function".to_string(),
                function: None,
            }),
        }]
    );
}
