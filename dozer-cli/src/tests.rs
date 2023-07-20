use crate::config_helper::add_file_content_to_config;
use dozer_types::models::config::Config;
use dozer_types::serde_yaml;
use dozer_types::serde_yaml::Mapping;

#[test]
fn test_sql_merge_in_config() {
    let query_a = "select * from table_a";
    let query_b = "select * from table_b";

    let yaml = format!(
        r#"
    app_name: dozer-config-sample
    sql:
        {}
    "#,
        query_a
    );

    let mut combined_yaml = serde_yaml::Value::Mapping(Mapping::new());

    add_file_content_to_config(&mut combined_yaml, "config.yaml", yaml).unwrap();
    add_file_content_to_config(&mut combined_yaml, "query.sql", query_b.to_string()).unwrap();

    let config = serde_yaml::from_value::<Config>(combined_yaml).unwrap();

    assert_eq!(config.sql, Some(format!("{};{}", query_a, query_b)));
}

#[test]
fn test_sql_from_single_sql_source_in_config() {
    let query = "select * from table_b";

    let yaml = r#"app_name: dozer-config-sample"#;

    let mut combined_yaml = serde_yaml::Value::Mapping(Mapping::new());

    add_file_content_to_config(&mut combined_yaml, "config.yaml", yaml.to_string()).unwrap();
    add_file_content_to_config(&mut combined_yaml, "query.sql", query.to_string()).unwrap();

    let config = serde_yaml::from_value::<Config>(combined_yaml).unwrap();

    assert_eq!(config.sql, Some(query.to_string()));
}

#[test]
fn test_sql_from_single_yaml_source_in_config() {
    let query = "select * from table_b";

    let yaml = format!(
        r#"
    app_name: dozer-config-sample
    sql:
        {}
    "#,
        query
    );

    let mut combined_yaml = serde_yaml::Value::Mapping(Mapping::new());

    add_file_content_to_config(&mut combined_yaml, "config.yaml", yaml).unwrap();

    let config = serde_yaml::from_value::<Config>(combined_yaml).unwrap();

    assert_eq!(config.sql, Some(query.to_string()));
}
