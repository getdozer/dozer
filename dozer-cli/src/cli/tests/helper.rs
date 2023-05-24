use dozer_types::{
    constants::DEFAULT_HOME_DIR,
    models::{
        api_config::{ApiConfig, ApiGrpc, ApiInternal, ApiPipelineInternal, ApiRest},
        api_endpoint::ApiEndpoint,
        app_config::{Config, Flags},
        connection::{Authentication, Connection, PostgresAuthentication},
        source::{RefreshConfig, Source},
    },
};

pub fn test_yml_content_full() -> &'static str {
    r#"
  app_name: dozer-config-sample
  home_dir: './.dozer'
  api:
    rest:
      port: 8080
      host: "0.0.0.0"
      cors: true
    grpc:
      port: 50051
      host: "0.0.0.0"
      cors: true
      web: true
    auth: false
    api_internal:
      port: 50052
      host: '0.0.0.0'
      home_dir: './.dozer/api'
    pipeline_internal:
      port: 50053
      host: '0.0.0.0'
      home_dir: './.dozer/pipeline'
  flags:
    grpc_web: true
    dynamic: true
    push_events: false
    authenticate_server_reflection: false
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
pub fn test_yml_content_missing_api_config() -> &'static str {
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
pub fn test_yml_content_missing_internal_config() -> &'static str {
    r#"
  app_name: dozer-config-sample
  api:
    rest:
      port: 8080
      host: "0.0.0.0"
      cors: true
    grpc:
      port: 50051
      host: "0.0.0.0"
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

pub fn minimal_config() -> &'static str {
    r#"
app_name: minimal-app
connections:
  - db_type: Postgres
    authentication: !Postgres
      user: postgres
      password: postgres
      host: localhost
      port: 5432
      database: users
    name: users
"#
}
pub fn test_connection() -> Connection {
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
pub fn test_api_endpoint() -> ApiEndpoint {
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
pub fn test_source(connection: Connection) -> Source {
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
pub fn test_api_config() -> ApiConfig {
    ApiConfig {
        rest: Some(ApiRest {
            port: 8080,
            host: "0.0.0.0".to_owned(),
            cors: true,
        }),
        grpc: Some(ApiGrpc {
            port: 50051,
            host: "0.0.0.0".to_owned(),
            cors: true,
            web: true,
        }),
        auth: false,
        api_internal: Some(ApiInternal {
            home_dir: format!("{:}/api", DEFAULT_HOME_DIR.to_owned()),
        }),
        pipeline_internal: Some(ApiPipelineInternal {
            port: 50053,
            host: "0.0.0.0".to_owned(),
            home_dir: format!("{:}/pipeline", DEFAULT_HOME_DIR.to_owned()),
        }),
        ..Default::default()
    }
}
pub fn test_config() -> Config {
    let test_connection = test_connection();
    let test_source = test_source(test_connection.to_owned());
    let api_endpoint = test_api_endpoint();
    let api_config = test_api_config();
    let flags = Flags::default();
    Config {
        app_name: "dozer-config-sample".to_owned(),
        home_dir: DEFAULT_HOME_DIR.to_owned(),
        api: Some(api_config),
        flags: Some(flags),
        connections: vec![test_connection],
        sources: vec![test_source],
        endpoints: vec![api_endpoint],
        ..Default::default()
    }
}
