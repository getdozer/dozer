use crate::models::{
    api_config::{
        default_api_grpc, default_api_rest, default_app_grpc, GrpcApiOptions, RestApiOptions,
    },
    api_security::ApiSecurity,
    app_config::Config,
};

#[test]
fn override_rest_port() {
    let input_config = r#"
    app_name: working_app
    api:
      rest:
        port: 9876
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_rest_config = RestApiOptions {
        port: 9876,
        host: default_api_rest.host,
        cors: default_api_rest.cors,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), default_api_grpc);
}
#[test]
fn override_rest_host() {
    let input_config = r#"
    app_name: working_app
    api:
      rest:
        host: localhost
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_rest_config = RestApiOptions {
        port: default_api_rest.port,
        host: "localhost".to_owned(),
        cors: default_api_rest.cors,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), default_api_grpc);
}
#[test]
fn override_rest_enabled() {
    let input_config = r#"
    app_name: working_app
    api:
      rest:
        enabled: false
    home_dir: './.dozer' 
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_rest_config = RestApiOptions {
        port: default_api_rest.port,
        host: default_api_rest.host,
        cors: default_api_rest.cors,
        enabled: false,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), default_api_grpc);
}
#[test]
fn override_grpc_port() {
    let input_config = r#"
  app_name: working_app
  api:
    grpc:
      port: 4232
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_grpc_config = GrpcApiOptions {
        port: 4232,
        host: default_api_grpc.host,
        cors: default_api_grpc.cors,
        web: default_api_grpc.web,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), default_api_rest);
    assert_eq!(api_config.grpc.unwrap(), expected_grpc_config);
}

#[test]
fn override_grpc_enabled() {
    let input_config = r#"
  app_name: working_app
  api:
    grpc:
      enabled: false
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_grpc_config = GrpcApiOptions {
        enabled: false,
        port: default_api_grpc.port,
        host: default_api_grpc.host,
        cors: default_api_grpc.cors,
        web: default_api_grpc.web,
        ..Default::default()
    };
    assert_eq!(api_config.rest.unwrap(), default_api_rest);
    assert_eq!(api_config.grpc.unwrap(), expected_grpc_config);
}

#[test]
fn override_grpc_and_rest_port() {
    let input_config = r#"
  app_name: working_app
  api:
    grpc:
      port: 4232
    rest:
      port: 3324
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_grpc_config = GrpcApiOptions {
        port: 4232,
        host: default_api_grpc.host,
        cors: default_api_grpc.cors,
        web: default_api_grpc.web,
        enabled: true,
    };
    let expected_rest_config = RestApiOptions {
        port: 3324,
        host: default_api_rest.host,
        cors: default_api_rest.cors,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), expected_grpc_config);
}

#[test]
fn override_grpc_and_rest_port_jwt() {
    let input_config = r#"
  app_name: working_app
  api:
    grpc:
      port: 4232
    rest:
      port: 3324
    api_security: !Jwt
      Vv44T1GugX
  home_dir: './.dozer' 
"#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_grpc_config = GrpcApiOptions {
        port: 4232,
        host: default_api_grpc.host,
        cors: default_api_grpc.cors,
        web: default_api_grpc.web,
        enabled: true,
    };
    let expected_rest_config = RestApiOptions {
        port: 3324,
        host: default_api_rest.host,
        cors: default_api_rest.cors,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), expected_grpc_config);
    let api_security = api_config.api_security;
    assert!(api_security.is_some());
    let api_security = api_security.unwrap();
    let expected_api_security = ApiSecurity::Jwt("Vv44T1GugX".to_owned());
    assert_eq!(api_security, expected_api_security);
}
#[test]
fn override_grpc_and_rest_port_jwt_pipeline_home_dir() {
    let input_config = r#"
  app_name: working_app
  api:
    grpc:
      port: 4232
    rest:
      port: 3324
    api_security: !Jwt
      Vv44T1GugX      
    app_grpc:
      home_dir: './pipeline_folder'
      port: 3993
    
  home_dir: './.dozer' 
"#;

    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    assert!(deserialize_result.is_ok());
    let api_config = deserialize_result.unwrap().api;
    assert!(api_config.is_some());
    let api_config = api_config.unwrap();
    assert!(api_config.rest.is_some());
    let default_api_rest = default_api_rest().unwrap();
    let default_api_grpc = default_api_grpc().unwrap();
    let expected_grpc_config = GrpcApiOptions {
        port: 4232,
        host: default_api_grpc.host,
        cors: default_api_grpc.cors,
        web: default_api_grpc.web,
        enabled: true,
    };
    let expected_rest_config = RestApiOptions {
        port: 3324,
        host: default_api_rest.host,
        cors: default_api_rest.cors,
        enabled: true,
    };
    assert_eq!(api_config.rest.unwrap(), expected_rest_config);
    assert_eq!(api_config.grpc.unwrap(), expected_grpc_config);
    let api_security = api_config.api_security;
    assert!(api_security.is_some());
    let api_security = api_security.unwrap();
    let expected_api_security = ApiSecurity::Jwt("Vv44T1GugX".to_owned());
    assert_eq!(api_security, expected_api_security);

    let app_grpc = api_config.app_grpc.unwrap();
    let default_app_grpc = default_app_grpc().unwrap();
    assert_eq!(app_grpc.port, 3993);
    assert_eq!(app_grpc.host, default_app_grpc.host);
}
