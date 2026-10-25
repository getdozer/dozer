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
fn explicit_wasm_type_is_order_independent_and_round_trips() {
    use crate::models::udf_config::WasmConfig;
    let expected = UdfConfig {
        name: "my_function".into(),
        config: UdfType::Wasm(WasmConfig {
            path: "f.wasm".into(),
        }),
    };
    for yaml in [
        "type: wasm\nname: my_function\nconfig: {path: f.wasm}",
        "config: {path: f.wasm}\nname: my_function\ntype: wasm",
    ] {
        assert_eq!(serde_yaml::from_str::<UdfConfig>(yaml).unwrap(), expected);
    }
    let json = r#"{"name":"my_function","config":{"path":"f.wasm"},"type":"wasm"}"#;
    assert_eq!(serde_json::from_str::<UdfConfig>(json).unwrap(), expected);
    for config in [
        expected,
        UdfConfig {
            name: "model".into(),
            config: UdfType::Onnx(OnnxConfig {
                path: "m.onnx".into(),
            }),
        },
        UdfConfig {
            name: "js".into(),
            config: UdfType::JavaScript(crate::models::udf_config::JavaScriptConfig {
                module: "f.js".into(),
            }),
        },
    ] {
        assert_eq!(
            serde_yaml::from_str::<UdfConfig>(&serde_yaml::to_string(&config).unwrap()).unwrap(),
            config
        );
        assert_eq!(
            serde_json::from_str::<UdfConfig>(&serde_json::to_string(&config).unwrap()).unwrap(),
            config
        );
    }
}

#[test]
fn wasm_mapping_and_tagged_list_are_equivalent() {
    use crate::models::config::Config;
    let base = "version: 1\napp_name: wasm-test\nudfs:\n";
    let mapping: Config = serde_yaml::from_str(&format!(
        "{base}  wasm:\n    my_function: udfs/wasm/my_function\n"
    ))
    .unwrap();
    let list: Config = serde_yaml::from_str(&format!(
        "{base}  - name: my_function\n    config: !Wasm\n      path: udfs/wasm/my_function\n"
    ))
    .unwrap();
    assert_eq!(mapping, list);
    let explicit: Config = serde_yaml::from_str(&format!(
        "{base}  - type: wasm\n    name: my_function\n    config:\n      path: udfs/wasm/my_function\n"
    )).unwrap();
    assert_eq!(mapping, explicit);
    let serialized = serde_yaml::to_string(&mapping).unwrap();
    assert_eq!(
        serde_yaml::from_str::<Config>(&serialized).unwrap(),
        mapping
    );
    assert!(
        matches!(&mapping.udfs[0].config, UdfType::Wasm(config) if config.path == "udfs/wasm/my_function")
    );
}

#[test]
fn mixed_udf_list_remains_supported() {
    use crate::models::config::Config;
    let config: Config = serde_yaml::from_str("version: 1\napp_name: mixed\nudfs:\n  - name: model\n    config: !Onnx\n      path: model.onnx\n  - name: js\n    config: !JavaScript\n      module: function.js\n  - name: f\n    config: !Wasm\n      path: f.wasm\n").unwrap();
    assert!(matches!(config.udfs[0].config, UdfType::Onnx(_)));
    assert!(matches!(config.udfs[1].config, UdfType::JavaScript(_)));
    assert!(matches!(config.udfs[2].config, UdfType::Wasm(_)));
}

#[test]
fn malformed_wasm_configuration_is_rejected() {
    use crate::models::config::Config;
    for value in [
        "  {}",
        "  typo: {}",
        "  wasm: []",
        "  wasm: {f: {path: f.wasm}}",
        "  wasm: {f: f.wasm, f: other.wasm}",
        "  wasm: {}\n  wasm: {}",
        "  - name: f\n    config: !Wasm\n      path: f.wasm\n      typo: true",
    ] {
        let input = format!("version: 1\napp_name: invalid\nudfs:\n{value}\n");
        assert!(
            serde_yaml::from_str::<Config>(&input).is_err(),
            "accepted {input}"
        );
    }
}

#[test]
fn explicit_wasm_rejects_ambiguous_missing_and_duplicate_fields() {
    for yaml in [
        "name: f\nconfig: {path: f.wasm}",
        "name: f\ntype: wasm",
        "type: wasm\nconfig: {path: f.wasm}",
        "name: f\ntype: unknown\nconfig: {path: f.wasm}",
        "name: f\ntype: Wasm\nconfig: {path: f.wasm}",
        "name: f\ntype: null\nconfig: !Wasm {path: f.wasm}",
        "name: f\ntype: wasm\nconfig: !Wasm {path: f.wasm}",
        "name: f\ntype: wasm\nconfig: !Onnx {path: f.wasm}",
        "name: f\ntype: wasm\nconfig: {Onnx: {path: f.wasm}}",
        "name: f\ntype: wasm\nconfig: {}",
        "name: f\ntype: wasm\nconfig: {module: f.wasm}",
        "name: f\ntype: wasm\nconfig: {path: null}",
        "name: f\ntype: wasm\nconfig: {path: 123}",
        "name: f\ntype: wasm\nconfig: {path: false}",
        "name: f\ntype: wasm\nconfig: {path: f.wasm, path: other.wasm}",
        "name: f\ntype: wasm\nconfig: {path: f.wasm, typo: true}",
        "name: f\ntype: wasm\nconfig: {path: f.wasm}\ntypo: true",
        "name: f\nname: g\ntype: wasm\nconfig: {path: f.wasm}",
        "name: f\ntype: wasm\ntype: wasm\nconfig: {path: f.wasm}",
        "name: f\ntype: wasm\nconfig: {path: f.wasm}\nconfig: {path: g.wasm}",
        "name: f\nconfig: {Wasm: {path: f.wasm}, Onnx: {path: m.onnx}}",
    ] {
        assert!(
            serde_yaml::from_str::<UdfConfig>(yaml).is_err(),
            "accepted {yaml}"
        );
    }
    for json in [
        r#"{"name":"f","type":"wasm","config":{"path":"f.wasm","path":"g.wasm"}}"#,
        r#"{"name":"f","type":"wasm","type":"wasm","config":{"path":"f.wasm"}}"#,
        r#"{"name":"f","type":"wasm","config":{"Wasm":{"path":"f.wasm"}}}"#,
    ] {
        assert!(
            serde_json::from_str::<UdfConfig>(json).is_err(),
            "accepted {json}"
        );
    }
}

#[test]
fn explicit_wasm_can_mix_with_legacy_udfs() {
    use crate::models::config::Config;
    let config: Config = serde_yaml::from_str("version: 1\napp_name: mixed\nudfs:\n  - name: model\n    config: !Onnx\n      path: model.onnx\n  - name: f\n    type: wasm\n    config:\n      path: f.wasm\n  - name: js\n    config: !JavaScript\n      module: function.js\n").unwrap();
    assert!(matches!(config.udfs[0].config, UdfType::Onnx(_)));
    assert!(matches!(config.udfs[1].config, UdfType::Wasm(_)));
    assert!(matches!(config.udfs[2].config, UdfType::JavaScript(_)));
    assert_eq!(
        serde_yaml::from_str::<Config>(&serde_yaml::to_string(&config).unwrap()).unwrap(),
        config
    );
}

#[test]
fn wasm_schema_describes_mapping_and_both_list_entries() {
    let schema: serde_json::Value =
        serde_json::from_str(&crate::models::get_dozer_schema().unwrap()).unwrap();
    assert_eq!(
        schema["properties"]["udfs"]["anyOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        schema["definitions"]["WasmMapping"]["properties"]["wasm"]["additionalProperties"]["type"],
        "string"
    );
    assert!(schema["definitions"]["WasmConfig"]["properties"]["path"].is_object());
    assert_eq!(
        schema["definitions"]["UdfEntry"]["anyOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        schema["definitions"]["ExplicitWasmUdf"]["required"],
        serde_json::json!(["config", "name", "type"])
    );
    assert_eq!(
        schema["definitions"]["WasmKind"]["enum"],
        serde_json::json!(["wasm"])
    );
    assert_eq!(
        schema["definitions"]["ExplicitWasmUdf"]["additionalProperties"],
        false
    );
}
