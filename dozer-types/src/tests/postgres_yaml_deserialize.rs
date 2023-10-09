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
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
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
    sslmode: require
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: Some("postgres".to_string()),
        password: Some("postgres".to_string()),
        host: Some("localhost".to_string()),
        port: Some(5432),
        database: Some("users".to_string()),
        sslmode: Some("require".to_string()),
        connection_url: None,
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
fn standard_url() {
    let postgres_config = r#"
    !Postgres
    connection_url: postgres://postgres:postgres@ep-silent-bread-370191.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=prefer
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: None,
        password: None,
        host: None,
        port: None,
        database: None,
        sslmode: None,
        connection_url: Some("postgres://postgres:postgres@ep-silent-bread-370191.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=prefer".to_string()),
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
#[should_panic(expected = "MissingFieldInPostgresConfig(\"user\")")]
fn standard_url_missing_user() {
    let postgres_config = r#"
    !Postgres
    connection_url: postgresql://localhost:5432/stocks?sslmode=prefer
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: None,
        password: None,
        host: None,
        port: None,
        database: None,
        sslmode: None,
        connection_url: Some("postgresql://localhost:5432/stocks?sslmode=prefer".to_string()),
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
#[should_panic(expected = "MissingFieldInPostgresConfig(\"password\")")]
fn standard_url_missing_password() {
    let postgres_config = r#"
    !Postgres
    user: postgres
    connection_url: postgresql://localhost:5432/stocks?sslmode=prefer
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: Some("postgres".to_string()),
        password: None,
        host: None,
        port: None,
        database: None,
        sslmode: None,
        connection_url: Some("postgresql://localhost:5432/stocks?sslmode=prefer".to_string()),
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
            assert_eq!(expected_replenished, deserialized_replenished);
        }
    }
}

#[test]
fn standard_url_2() {
    let postgres_config = r#"
    !Postgres
    user: postgres
    password: postgres
    connection_url: postgresql://localhost:5432/stocks?sslmode=prefer
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(postgres_config).unwrap();
    let postgres_auth = PostgresConfig {
        user: Some("postgres".to_string()),
        password: Some("postgres".to_string()),
        host: None,
        port: None,
        database: None,
        sslmode: None,
        connection_url: Some("postgresql://localhost:5432/stocks?sslmode=prefer".to_string()),
        schema: None,
    };
    let expected = ConnectionConfig::Postgres(postgres_auth);
    assert_eq!(expected, deserializer_result);

    if let ConnectionConfig::Postgres(config) = expected {
        let expected_replenished = config.replenish().unwrap();
        if let ConnectionConfig::Postgres(deserialized) = deserializer_result {
            let deserialized_replenished = deserialized.replenish().unwrap();
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
