use super::Config;
use dozer_types::models::{
    api_config::{ApiConfig, ApiGrpc, ApiInternal, ApiRest},
    api_endpoint::ApiEndpoint,
    connection::{Authentication, Connection},
    source::Source,
};
fn test_yml_content() -> String {
    r#"
    app_name: dozer-config-sample
api:
  rest:
    port: 8080
    url: '[::0]'
    cors: true
  grpc:
    port: 50051
    url: '[::0]'
    cors: true
    web: true
  auth: false
  internal:
    port: 50052
    host: '[::1]'
connections:
- db_type: Postgres
  authentication: !PostgresAuthentication
    user: postgres
    password: postgres
    host: localhost
    port: 5432
    database: users
  name: users
  id: null
sources:
- name: users
  table_name: users
  columns:
  - id
  - email
  - phone
  connection: !Ref users
  history_type: null
  refresh_config: RealTime
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
        db_type: dozer_types::models::connection::DBType::Postgres,
        authentication: Authentication::PostgresAuthentication {
            user: "postgres".to_owned(),
            password: "postgres".to_owned(),
            host: "localhost".to_owned(),
            port: 5432,
            database: "users".to_owned(),
        },
        name: "users".to_owned(),
        id: None,
    }
}
fn test_api_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        id: None,
        name: "users".to_owned(),
        path: "/users".to_owned(),
        sql: "select id, email, phone from users where 1=1;".to_owned(),
        index: dozer_types::models::api_endpoint::ApiIndex {
            primary_key: vec!["id".to_owned()],
        },
    }
}
fn test_source(connection: Connection) -> Source {
    Source {
        id: None,
        name: "users".to_owned(),
        table_name: "users".to_owned(),
        columns: Some(vec![
            "id".to_owned(),
            "email".to_owned(),
            "phone".to_owned(),
        ]),
        connection,
        history_type: None,
        refresh_config: dozer_types::models::source::RefreshConfig::RealTime,
    }
}
fn test_api_config() -> ApiConfig {
    ApiConfig {
        rest: ApiRest {
            port: 8080,
            url: "[::0]".to_owned(),
            cors: true,
        },
        grpc: ApiGrpc {
            port: 50051,
            url: "[::0]".to_owned(),
            cors: true,
            web: true,
        },
        auth: false,
        internal: ApiInternal {
            port: 50052,
            host: "[::1]".to_owned(),
        },
    }
}
fn test_config() -> Config {
    let test_connection = test_connection();
    let test_source = test_source(test_connection.to_owned());
    let api_endpoint = test_api_endpoint();
    let api_config = test_api_config();
    Config {
        app_name: "dozer-config-sample".to_owned(),
        api: api_config,
        connections: vec![test_connection],
        sources: vec![test_source],
        endpoints: vec![api_endpoint],
    }
}
#[test]
fn test_deserialize_config() {
    let deserializer_result = serde_yaml::from_str::<Config>(test_yml_content()).unwrap();
    let expected = test_config();
    assert_eq!(deserializer_result.api, expected.api);
    assert_eq!(deserializer_result.app_name, expected.app_name);
    assert_eq!(deserializer_result.connections, expected.connections);
    assert_eq!(deserializer_result.endpoints, expected.endpoints);
    assert_eq!(deserializer_result.sources, expected.sources);
    assert_eq!(deserializer_result, expected);
}
#[test]
fn test_serialize_config() {
    let config = test_config();
    let test_str = test_yml_content();
    let serialize_yml = serde_yaml::to_string(&config).unwrap();
    let f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open("/Users/anhthu/Dozer/pdozer/dozer-orchestrator/src/cli/test.yml")
        .expect("Couldn't open file");
    serde_yaml::to_writer(f, &config).unwrap();
    assert_eq!(serialize_yml, test_str);
}
