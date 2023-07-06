use crate::models::config::Config;

#[test]
#[ignore = "We removed the connection name validation, but should add it back in the future as part of a `validation` step"]
fn error_wrong_reference_connection_name() {
    let input_config = r#"
    app_name: working_app
    home_dir: './.dozer'
    connections:
    - authentication: !Postgres
        user: postgres
        password: postgres
        host: localhost
        port: 5432
        database: users
      db_type: Postgres
      name: users
    sources:
    - name: users
      table_name: users
      columns:
      - id
      - email
      - phone
      connection: wrong_connection_name
    endpoints:
    - name: users
      path: /users
      sql: select id, email, phone from users where 1=1;
      index:
        primary_key:
        - id
    
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let error = deserialize_result.err();
    assert!(error.is_some());
    assert!(error
        .unwrap()
        .to_string()
        .starts_with("sources[0]: Cannot find Ref connection name: wrong_connection_name"));
}

#[test]
fn error_missing_field_general() {
    let input_config = r#"
    app_name: working_app
    home_dir: './.dozer'
    connections:
    - authentication: !Postgres
        user: postgres
        password: postgres
        host: localhost
        port: 5432
        database: users
      db_type: Postgres
      name: users
    sources:
    - table_name: users
      columns:
      - id
      - email
      - phone
      connection: users
    endpoints:
    - path: /eth/stats
      sql: select block_number, sum(id) from eth_logs where 1=1 group by block_number;
      index:
        primary_key:
        - block_number  
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let error = deserialize_result.err();
    assert!(error.is_some());
    assert!(error
        .unwrap()
        .to_string()
        .starts_with("sources[0]: missing field `name`"));
}
#[test]
fn error_missing_field_in_source() {
    let input_config = r#"
    app_name: working_app
    home_dir: './.dozer'
    connections:
    - authentication: !Postgres
        user: postgres
        password: postgres
        host: localhost
        port: 5432
        database: users
      db_type: Postgres
      name: users
    sources:
    - table_name: users
      columns:
      - id
      - email
      - phone
      connection: users    
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let error = deserialize_result.err();
    assert!(error.is_some());
    assert!(error
        .unwrap()
        .to_string()
        .starts_with("sources[0]: missing field `name`"));
}

#[test]
fn error_missing_field_connection_ref_in_source() {
    let input_config = r#"
    app_name: working_app
    home_dir: './.dozer'
    connections:
    - authentication: !Postgres
        user: postgres
        password: postgres
        host: localhost
        port: 5432
        database: users
      db_type: Postgres
      name: users
    sources:
    - name: users
      table_name: users
      columns:
      - id
      - email
      - phone   
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let error = deserialize_result.err();
    assert!(error.is_some());
    assert!(error
        .unwrap()
        .to_string()
        .starts_with("sources[0]: missing field `connection`"));
}

#[test]
fn error_missing_connection_ref() {
    let input_config = r#"
    app_name: working_app
    home_dir: './.dozer'
    connections:
    - config: !Postgres
        user: postgres
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
  "#;
    let deserialize_result = serde_yaml::from_str::<Config>(input_config);
    let error = deserialize_result.unwrap_err();
    assert!(error
        .to_string()
        .starts_with("sources[0]: missing field `connection`"));
}
