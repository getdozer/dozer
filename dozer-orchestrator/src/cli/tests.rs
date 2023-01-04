use super::Config;
use dozer_types::{
    constants::DEFAULT_HOME_DIR,
    models::{
        api_config::{
            default_api_config, ApiConfig, ApiGrpc, ApiInternal, ApiPipelineInternal, ApiRest,
        },
        api_endpoint::ApiEndpoint,
        app_config::Flags,
        connection::{Authentication, Connection, PostgresAuthentication},
        source::{RefreshConfig, Source},
    },
    serde_yaml,
};
fn test_yml_content_full() -> (&'static str, Config) {
    let test_connection = test_connection();
    let test_source = test_source(test_connection.to_owned());
    let api_endpoint = test_api_endpoint();
    let api_config = test_api_config();
    let flags = Some(Flags {
        dynamic: true,
        grpc_web: true,
        push_events: false,
    });
    let config = Config {
        app_name: "dozer-config-sample".to_owned(),
        home_dir: DEFAULT_HOME_DIR.to_owned(),
        api: Some(api_config),
        connections: vec![test_connection],
        sources: vec![test_source],
        endpoints: vec![api_endpoint],
        flags: flags,
        ..Default::default()
    };
    (
        r#"
    app_name: dozer-config-sample
    home_dir: './.dozer'
    api:
      rest:
        port: 8080
        host: "[::0]"
        cors: true
      grpc:
        port: 50051
        host: "[::0]"
        cors: true
        web: true
      auth: false
      api_internal:
        port: 50052
        host: '[::1]'
        home_dir: './.dozer/api'
      pipeline_internal:
        port: 50053
        host: '[::1]'
        home_dir: './.dozer/pipeline'
    flags:
      grpc_web: true
      dynamic: true
      push_events: false
    connections:
      - db_type: Postgres
        authentication: !Postgres
          user: postgres
          password: postgres
          host: localhost
          port: 5432
          database: users
        name: users
    sources:
      - name: users
        table_name: users
        columns:
          - id
          - email
          - phone
        connection: !Ref users
    endpoints:
      - id: null
        name: users
        path: /users
        sql: select id, email, phone from users where 1=1;
        index:
          primary_key:
            - id    
    "#,
        config,
    )
}
fn test_yml_content_missing_api_config() -> &'static str {
    r#"
    app_name: dozer-config-sample
    connections:
      - db_type: Postgres
        authentication: !Postgres
          user: postgres
          password: postgres
          host: localhost
          port: 5432
          database: users
        name: users
    sources:
      - name: users
        table_name: users
        columns:
          - id
          - email
          - phone
        connection: !Ref users
    endpoints:
      - id: null
        name: users
        path: /users
        sql: select id, email, phone from users where 1=1;
        index:
          primary_key:
            - id    
    "#
}
fn test_yml_content_missing_internal_config() -> &'static str {
    r#"
    app_name: dozer-config-sample
    api:
      rest:
        port: 8080
        host: "[::0]"
        cors: true
      grpc:
        port: 50051
        host: "[::0]"
        cors: true
        web: true
      auth: false
    connections:
      - db_type: Postgres
        authentication: !Postgres
          user: postgres
          password: postgres
          host: localhost
          port: 5432
          database: users
        name: users
    sources:
      - name: users
        table_name: users
        columns:
          - id
          - email
          - phone
        connection: !Ref users
    endpoints:
      - id: null
        name: users
        path: /users
        sql: select id, email, phone from users where 1=1;
        index:
          primary_key:
            - id    
    "#
}

fn test_connection() -> Connection {
    Connection {
        authentication: Some(Authentication::Postgres(PostgresAuthentication {
            user: "postgres".to_owned(),
            password: "postgres".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "users".to_owned(),
        })),
        db_type: dozer_types::models::connection::DBType::Postgres as i32,
        name: "users".to_owned(),
        ..Default::default()
    }
}
fn test_api_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        name: "users".to_owned(),
        path: "/users".to_owned(),
        sql: "select id, email, phone from users where 1=1;".to_owned(),
        index: Some(dozer_types::models::api_endpoint::ApiIndex {
            primary_key: vec!["id".to_owned()],
        }),
        ..Default::default()
    }
}
fn test_source(connection: Connection) -> Source {
    Source {
        id: None,
        name: "users".to_owned(),
        table_name: "users".to_owned(),
        columns: vec!["id".to_owned(), "email".to_owned(), "phone".to_owned()],
        connection: Some(connection),
        refresh_config: Some(RefreshConfig::default()),
        ..Default::default()
    }
}
fn test_api_config() -> ApiConfig {
    ApiConfig {
        rest: Some(ApiRest {
            port: 8080,
            host: "[::0]".to_owned(),
            cors: true,
        }),
        grpc: Some(ApiGrpc {
            port: 50051,
            host: "[::0]".to_owned(),
            cors: true,
            web: true,
        }),
        auth: false,
        api_internal: Some(ApiInternal {
            home_dir: format!("{:}/api", DEFAULT_HOME_DIR.to_owned()),
        }),
        pipeline_internal: Some(ApiPipelineInternal {
            port: 50053,
            host: "[::1]".to_owned(),
            home_dir: format!("{:}/pipeline", DEFAULT_HOME_DIR.to_owned()),
        }),
        ..Default::default()
    }
}
fn test_config() -> Config {
    let test_connection = test_connection();
    let test_source = test_source(test_connection.to_owned());
    let api_endpoint = test_api_endpoint();
    let api_config = test_api_config();
    Config {
        app_name: "dozer-config-sample".to_owned(),
        home_dir: DEFAULT_HOME_DIR.to_owned(),
        api: Some(api_config),
        connections: vec![test_connection],
        sources: vec![test_source],
        endpoints: vec![api_endpoint],
        ..Default::default()
    }
}
#[test]
fn test_deserialize_config() {
    let test_full = test_yml_content_full();
    let test_full_str = test_full.0;
    let deserializer_result = serde_yaml::from_str::<Config>(test_full_str).unwrap();
    let expected = test_full.1;
    assert_eq!(deserializer_result.api, expected.api);
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_deserialize_default_api_config() {
    let test_str = test_yml_content_missing_api_config();
    let deserializer_result = serde_yaml::from_str::<Config>(test_str).unwrap();
    let expected = test_config();
    let default_api_config = default_api_config();
    assert_eq!(deserializer_result.api, Some(default_api_config));
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_deserialize_yaml_missing_internal_config() {
    let test_str = test_yml_content_missing_internal_config();
    let deserializer_result = serde_yaml::from_str::<Config>(test_str).unwrap();
    let expected = test_config();
    let default_api_config = default_api_config();
    assert_eq!(deserializer_result.api, Some(default_api_config.to_owned()));
    assert_eq!(
        deserializer_result
            .api
            .to_owned()
            .unwrap()
            .api_internal
            .unwrap(),
        default_api_config.api_internal.unwrap()
    );
    assert_eq!(
        deserializer_result
            .api
            .to_owned()
            .unwrap()
            .pipeline_internal
            .unwrap(),
        default_api_config.pipeline_internal.unwrap()
    );
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_serialize_config() {
    let config = test_config();
    let serialize_yml = serde_yaml::to_string(&config).unwrap();
    let deserializer_result = serde_yaml::from_str::<Config>(&serialize_yml).unwrap();
    assert_eq!(deserializer_result, config);
}
