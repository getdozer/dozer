use crate::models::{
    config::Config,
    flags::{EnableProbabilisticOptimizations, Flags},
};

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
    assert!(deserializer_result.flags.is_some());
    let flags_deserialize = deserializer_result.flags.unwrap();
    assert!(flags_deserialize.dynamic);
    assert!(!flags_deserialize.grpc_web);
    assert!(!flags_deserialize.push_events);
    assert_eq!(
        flags_deserialize.authenticate_server_reflection,
        default_flags.authenticate_server_reflection,
    );
    assert_eq!(flags_deserialize.dynamic, default_flags.dynamic);
}

#[test]
fn test_storage_params_config() {
    let input_config_without_flag = r#"
    app_name: working_app
    version: 1
    cache_max_map_size: 1073741824
"#;
    let deserializer_result = serde_yaml::from_str::<Config>(input_config_without_flag).unwrap();
    assert_eq!(deserializer_result.cache_max_map_size, Some(1073741824));
}

#[test]
fn test_flags_default() {
    assert_eq!(
        Flags::default(),
        Flags {
            dynamic: true,
            grpc_web: true,
            push_events: true,
            authenticate_server_reflection: false,
            enable_probabilistic_optimizations: None,
        }
    );

    assert_eq!(
        EnableProbabilisticOptimizations::default(),
        EnableProbabilisticOptimizations {
            in_sets: false,
            in_joins: false,
            in_aggregations: false
        }
    )
}
