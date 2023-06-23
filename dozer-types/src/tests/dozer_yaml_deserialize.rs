use crate::models::app_config::Config;

#[test]
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
      connection: !Ref wrong_connection_name
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
      connection: !Ref users
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
      connection: !Ref users    
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
        .starts_with("sources[0]: missing connection ref"));
}
