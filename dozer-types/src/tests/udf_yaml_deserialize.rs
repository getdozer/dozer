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
    assert_eq!(udf_conf, deserializer_result);
}

#[test]
fn wasm_udf_deserialize() {
    let wasm_yaml = r#"
    name: my_function
    config: !Wasm
      module: udfs/wasm/my_function.wasm
  "#;
    let parsed = serde_yaml::from_str::<UdfConfig>(wasm_yaml).unwrap();
    let expected = UdfConfig {
        name: "my_function".to_string(),
        config: UdfType::Wasm(WasmConfig {
            module: "udfs/wasm/my_function.wasm".to_string(),
        }),
    };
    assert_eq!(expected, parsed);

    let wasm_path_alias_yaml = r#"
    name: my_function
    config: !Wasm
      path: udfs/wasm/my_function
  "#;
    let parsed_alias = serde_yaml::from_str::<UdfConfig>(wasm_path_alias_yaml).unwrap();
    assert_eq!(
        parsed_alias,
        UdfConfig {
            name: "my_function".to_string(),
            config: UdfType::Wasm(WasmConfig {
                module: "udfs/wasm/my_function".to_string(),
            }),
        }
    );
}
