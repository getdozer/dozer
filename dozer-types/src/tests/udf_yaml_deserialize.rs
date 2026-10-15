use crate::models::udf_config::{OnnxConfig, UdfConfig, UdfType};
use crate::models::{config::Config, udf_config::WasmConfig};

#[test]
fn wasm_original_mapping_and_tagged_list() {
    let original: Config = serde_yaml::from_str(
        "version: 1\napp_name: wasm\nudfs:\n  wasm:\n    my_function: udfs/wasm/my_function\n",
    )
    .unwrap();
    assert_eq!(
        original.udfs,
        vec![UdfConfig {
            name: "my_function".into(),
            config: UdfType::Wasm(WasmConfig::new("udfs/wasm/my_function".into()))
        }]
    );
    let tagged: Config = serde_yaml::from_str("version: 1\napp_name: wasm\nudfs:\n  - name: my_function\n    config: !Wasm\n      path: udfs/wasm/my_function\n").unwrap();
    assert_eq!(original, tagged);
    assert_eq!(
        original,
        serde_yaml::from_str(&serde_yaml::to_string(&original).unwrap()).unwrap()
    );
}

#[test]
fn wasm_mapping_does_not_hide_invalid_fields_or_break_existing_tags() {
    assert!(serde_yaml::from_str::<Config>(
        "version: 1\napp_name: wasm\nudfs:\n  wasmm:\n    f: a.wasm"
    )
    .is_err());
    let config: Config = serde_yaml::from_str("version: 1\napp_name: wasm\nudfs:\n  - name: model\n    config: !Onnx\n      path: model.onnx\n  - name: js\n    config: !JavaScript\n      module: file.js\n").unwrap();
    assert!(matches!(config.udfs[0].config, UdfType::Onnx(_)));
    assert!(matches!(config.udfs[1].config, UdfType::JavaScript(_)));
}

#[test]
fn wasm_mapping_rejects_duplicate_functions_and_requires_wasm_key() {
    let duplicate = serde_yaml::from_str::<Config>(
        "version: 1\napp_name: wasm\nudfs:\n  wasm:\n    f: first.wasm\n    f: second.wasm\n",
    )
    .unwrap_err();
    assert!(duplicate
        .to_string()
        .contains("duplicate wasm function `f`"));
    assert!(
        serde_yaml::from_str::<Config>("version: 1\napp_name: wasm\nudfs: {}")
            .unwrap_err()
            .to_string()
            .contains("missing field `wasm`")
    );
    for empty in ["[]", "{wasm: {}}"] {
        let config: Config =
            serde_yaml::from_str(&format!("version: 1\napp_name: wasm\nudfs: {empty}")).unwrap();
        assert!(config.udfs.is_empty());
    }
}

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
