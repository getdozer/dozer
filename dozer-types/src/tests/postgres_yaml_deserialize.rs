use crate::models::connection::{ConnectionConfig, PostgresConfig};
#[test]
fn standard() {
    let posgres_config = r#"
    !Postgres
    user: postgres
    password: postgres
    host: localhost
    port: 5432
    database: users
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(posgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: "postgres".to_owned(),
        password: "postgres".to_owned(),
        host: "localhost".to_owned(),
        port: 5432,
        database: "users".to_owned(),
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);
}

#[test]
fn error_missing_field() {
    let posgres_config = r#"
    !Postgres
    user: postgres
    host: localhost
    port: 5432
    database: users
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(posgres_config);
    assert!(deserializer_result.is_err());
    assert!(deserializer_result
        .err()
        .unwrap()
        .to_string()
        .starts_with("missing field `password`"))
}

#[test]
fn error_wrong_tag() {
    let posgres_config = r#"
    !Postgres112
    user: postgres
    host: localhost
    port: 5432
    database: users
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(posgres_config);
    assert!(deserializer_result.is_err());
    assert!(deserializer_result
        .err()
        .unwrap()
        .to_string()
        .starts_with("unknown variant `Postgres112`"))
}
