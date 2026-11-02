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

#[test]
fn wasm_config_with_default_export() {
    let config = "name: add\nconfig: !Wasm\n  path: ./udfs/numeric.wasm\n";
    let parsed = serde_yaml::from_str::<UdfConfig>(config).unwrap();
    match parsed.config {
        UdfType::Wasm(wasm) => {
            assert_eq!(wasm.path, "./udfs/numeric.wasm");
            assert_eq!(wasm.export, None);
        }
        _ => panic!("expected WASM config"),
    }
    assert!(serde_yaml::from_str::<UdfConfig>(&format!("{config}  unexpected: true\n")).is_err());
}
