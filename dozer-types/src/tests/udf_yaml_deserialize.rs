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
fn wasm() {
    let udf_config = r#"
    name: is_fraudulent
    config: !Wasm
      module: ./models/fraud.wasm
      function: score
      return_type: boolean
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: UdfType::Wasm(WasmConfig {
            module: "./models/fraud.wasm".to_string(),
            function: Some("score".to_string()),
            return_type: "boolean".to_string(),
        }),
        name: "is_fraudulent".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}

#[test]
fn wasm_defaults_function_name() {
    let udf_config = r#"
    name: score
    config: !Wasm
      module: ./models/score.wasm
      return_type: int
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: UdfType::Wasm(WasmConfig {
            module: "./models/score.wasm".to_string(),
            function: None,
            return_type: "int".to_string(),
        }),
        name: "score".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}
