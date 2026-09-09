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
fn wasm_defaults_to_the_sql_function_name() {
    let config: UdfConfig =
        serde_yaml::from_str("name: increment\nconfig: !Wasm\n  path: ./math.wasm\n").unwrap();
    assert_eq!(
        config.config,
        UdfType::Wasm(WasmConfig {
            path: "./math.wasm".into(),
            function: None,
        })
    );
}

#[test]
fn wasm_can_use_a_different_export_name() {
    let config: UdfConfig = serde_yaml::from_str(
        "name: increment\nconfig: !Wasm\n  path: ./math.wasm\n  function: addOne\n",
    )
    .unwrap();
    assert_eq!(
        config.config,
        UdfType::Wasm(WasmConfig {
            path: "./math.wasm".into(),
            function: Some("addOne".into()),
        })
    );
    let serialized = serde_yaml::to_string(&config).unwrap();
    assert_eq!(
        serde_yaml::from_str::<UdfConfig>(&serialized).unwrap(),
        config
    );
}

#[test]
fn wasm_requires_a_path_and_rejects_unknown_options() {
    for yaml in [
        "name: f\nconfig: !Wasm {}\n",
        "name: f\nconfig: !Wasm\n  path: ./math.wasm\n  typo: ignored\n",
    ] {
        assert!(serde_yaml::from_str::<UdfConfig>(yaml).is_err());
    }
}
