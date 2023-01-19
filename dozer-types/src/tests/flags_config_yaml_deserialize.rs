use crate::models::{app_config::Config, flags::Flags};

#[test]
fn test_partial_flag_config_input() {
    let input_config_with_flag = r#"
  app_name: working_app
  flags:
    dynamic: true
    grpc_web: false
    push_events: false
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_with_flag).unwrap();
    let default_flags = Flags::default();
    assert!(deserializer_result.flags.is_some());
    let flags_deserialize = deserializer_result.flags.unwrap();
    assert!(flags_deserialize.dynamic);
    assert!(!flags_deserialize.grpc_web);
    assert!(!flags_deserialize.push_events);
    assert_eq!(
        flags_deserialize.authenticate_server_reflection,
        default_flags.authenticate_server_reflection,
    );
}

#[test]
fn test_config_without_flag_config() {
    let input_config_without_flag = r#"
  app_name: working_app
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_without_flag).unwrap();
    let default_flags = Flags::default();
    assert!(deserializer_result.flags.is_some());
    assert_eq!(deserializer_result.flags, Some(default_flags));
}
