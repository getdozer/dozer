use crate::models::{
    api_config::{GrpcApiOptions, RestApiOptions},
    api_security::ApiSecurity,
    config::Config,
};

#[test]
fn override_rest_port() {
    let input_config = r#"
    app_name: working_app
    version: 1
    api:
      rest:
        port: 9876
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let expected_rest_config = RestApiOptions {
        port: Some(9876),
        host: None,
        cors: None,
        enabled: None,
    };
    assert_eq!(api_config.rest, expected_rest_config);
}

#[test]
fn override_rest_host() {
    let input_config = r#"
    app_name: working_app
    version: 1
    api:
      rest:
        host: localhost
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let expected_rest_config = RestApiOptions {
        port: None,
        host: Some("localhost".to_owned()),
        cors: None,
        enabled: None,
    };
    assert_eq!(api_config.rest, expected_rest_config);
}

#[test]
fn override_rest_enabled() {
    let input_config = r#"
    app_name: working_app
    version: 1
    api:
      rest:
        enabled: false
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let expected_rest_config = RestApiOptions {
        port: None,
        host: None,
        cors: None,
        enabled: Some(false),
    };
    assert_eq!(api_config.rest, expected_rest_config);
}

#[test]
fn override_grpc_port() {
    let input_config = r#"
  app_name: working_app
  version: 1
  api:
    grpc:
      port: 4232
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let expected_grpc_config = GrpcApiOptions {
        port: Some(4232),
        host: None,
        cors: None,
        web: None,
        enabled: None,
    };
    assert_eq!(api_config.grpc, expected_grpc_config);
}

#[test]
fn override_grpc_enabled() {
    let input_config = r#"
  app_name: working_app
  version: 1
  api:
    grpc:
      enabled: false
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let expected_grpc_config = GrpcApiOptions {
        enabled: Some(false),
        port: None,
        host: None,
        cors: None,
        web: None,
    };
    assert_eq!(api_config.grpc, expected_grpc_config);
}

#[test]
fn override_jwt() {
    let input_config = r#"
  app_name: working_app
  version: 1
  api:
    api_security: !Jwt
      Vv44T1GugX
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let api_security = api_config.api_security;
    assert!(api_security.is_some());
    let api_security = api_security.unwrap();
    let expected_api_security = ApiSecurity::Jwt("Vv44T1GugX".to_owned());
    assert_eq!(api_security, expected_api_security);
}

#[test]
fn override_pipeline_port() {
    let input_config = r#"
  app_name: working_app
  version: 1
  api:
    grpc:
      port: 4232
    rest:
      port: 3324
    api_security: !Jwt
      Vv44T1GugX      
    app_grpc:
      port: 3993
    
  home_dir: './.dozer' 
"#;

    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let api_config = deserialize_result.unwrap().api;
    let app_grpc = api_config.app_grpc;
    assert_eq!(app_grpc.port, Some(3993));
    assert_eq!(app_grpc.host, None);
}
