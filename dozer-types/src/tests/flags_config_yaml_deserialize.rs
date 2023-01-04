use crate::models::app_config::Config;

#[test]
fn test_full_flag_config_input() {
    let input_config_with_flag = r#"
  app_name: working_app
  flags:
    dynamic: true
    grpc_web: false
    push_events: false
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_with_flag).unwrap();

    assert!(deserializer_result.flags.is_some());
    let flags_deserialize = deserializer_result.flags.unwrap();
    assert_eq!(flags_deserialize.dynamic, true);
    assert_eq!(flags_deserialize.grpc_web, false);
    assert_eq!(flags_deserialize.push_events, false);
}

#[test]
fn test_flag_config_missing_field_should_throw_error() {
    let input_config_with_flag = r#"
  app_name: working_app
  flags:
    dynamic: true
    push_events: false
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_with_flag);
    assert!(deserializer_result.is_err());
    assert!(deserializer_result
        .err()
        .unwrap()
        .to_string()
        .starts_with("flags: missing field `grpc_web`"));
}

#[test]
fn test_config_without_flag_config() {
    let input_config_without_flag = r#"
  app_name: working_app
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_without_flag).unwrap();
    assert!(deserializer_result.flags.is_none());
}
