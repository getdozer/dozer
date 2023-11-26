use crate::models::udf_config::{OnnxConfig, UdfConfig, UdfType};

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

#[cfg(feature = "wasm")]
#[test]
fn standard_wasm() {
    let udf_config = r#"
    name: is_fraudulent
    config: !Onnx
      path: ./models/model_file
  "#;
    let deserializer_result = serde_yaml::from_str::<UdfConfig>(udf_config).unwrap();
    let udf_conf = UdfConfig {
        config: Some(UdfType::Wasm(WasmConfig {
            path: "./models/model_file".to_string(),
        })),
        name: "is_fraudulent".to_string(),
    };
    let expected = udf_conf;
    assert_eq!(expected, deserializer_result);
}
