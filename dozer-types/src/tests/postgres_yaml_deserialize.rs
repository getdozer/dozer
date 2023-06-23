use crate::models::connection::{ConnectionConfig, PostgresConfig};
#[test]
fn standard() {
    let postgres_config = r#"
    !Postgres
    user: postgres
    password: postgres
    host: localhost
    port: 5432
    database: users
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: Some("postgres".to_string()),
        password: Some("postgres".to_string()),
        host: Some("localhost".to_string()),
        port: Some(5432),
        database: Some("users".to_string()),
        sslmode: None,
        connection_url: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
fn standard_with_ssl_mode() {
    let postgres_config = r#"
    !Postgres
    user: postgres
    password: postgres
    host: localhost
    port: 5432
    database: users
    sslmode: verify-full
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: Some("postgres".to_string()),
        password: Some("postgres".to_string()),
        host: Some("localhost".to_string()),
        port: Some(5432),
        database: Some("users".to_string()),
        sslmode: Some("verify-full".to_string()),
        connection_url: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
fn standard_url() {
    let postgres_config = r#"
    !Postgres
    connection_url: postgres://postgres:postgres@ep-silent-bread-370191.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=verify-ca
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: None,
        password: None,
        host: None,
        port: None,
        database: None,
        sslmode: None,
        connection_url: Some("postgres://postgres:postgres@ep-silent-bread-370191.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=verify-ca".to_string()),
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
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
