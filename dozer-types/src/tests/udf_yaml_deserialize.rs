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
fn wasm_standard() {
    let udf_config = r#"
    name: add_one
    config: !Wasm
      path: ./udfs/wasm/add_one.wasm
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: UdfType::Wasm(WasmConfig {
            path: "./udfs/wasm/add_one.wasm".to_string(),
        }),
        name: "add_one".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}
