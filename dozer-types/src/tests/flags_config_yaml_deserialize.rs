use crate::models::{config::Config, flags::Flags};

#[test]
fn test_partial_flag_config_input() {
    let input_config_with_flag = r#"
  app_name: working_app
  version: 1
  flags:
    grpc_web: false
    push_events: false
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_with_flag).unwrap();
    let default_flags = Flags::default();
    let flags_deserialize = deserializer_result.flags;
    assert_eq!(flags_deserialize.dynamic, None);
    assert_eq!(flags_deserialize.grpc_web, Some(false));
    assert_eq!(flags_deserialize.push_events, Some(false));
    assert_eq!(
        flags_deserialize.authenticate_server_reflection,
        default_flags.authenticate_server_reflection,
    );
    assert_eq!(flags_deserialize.dynamic, default_flags.dynamic);
}
