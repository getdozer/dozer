use crate::models::udf_config::{OnnxConfig, UdfConfig, UdfType};
use crate::models::udf_config::WasmConfig;

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
fn wasm_map_form() {
    let config = r#"
version: 1
app_name: test
udfs:
  wasm:
    my_function: udfs/wasm/my_function
"#;
    let config: crate::models::config::Config = serde_yaml::from_str(config).unwrap();
    assert_eq!(
        config.udfs,
        vec![UdfConfig {
            name: "my_function".to_string(),
            config: UdfType::Wasm(WasmConfig {
                path: "udfs/wasm/my_function".to_string(),
            }),
        }]
    );
}

#[test]
fn wasm() {
    let udf_config = r#"
    name: my_function
    config: !Wasm
      path: ./udfs/wasm/my_function.wasm
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: UdfType::Wasm(WasmConfig {
            path: "./udfs/wasm/my_function.wasm".to_string(),
        }),
        name: "my_function".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}
