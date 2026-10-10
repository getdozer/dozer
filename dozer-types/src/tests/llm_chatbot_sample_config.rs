use crate::models::config::Config;
use crate::models::connection::ConnectionConfig;

#[test]
fn llm_chatbot_sample_pipeline_config_matches_current_schema() {
    let sample_yaml = include_str!("../../../samples/llm-chatbot/dozer-config.yaml");
    let mut sample_value: serde_yaml::Value =
        serde_yaml::from_str(sample_yaml).expect("sample YAML must be valid");

    let endpoints = sample_value
        .get("endpoints")
        .and_then(|endpoints| endpoints.as_sequence())
        .expect("sample must define API endpoints");
    let endpoint_names: Vec<&str> = endpoints
        .iter()
        .filter_map(|endpoint| endpoint.get("name").and_then(|name| name.as_str()))
        .collect();
    assert_eq!(
        endpoint_names,
        ["customers", "transactions", "credit_cards"]
    );

    sample_value
        .as_mapping_mut()
        .expect("sample YAML root must be a mapping")
        .remove("endpoints");
    let current_schema_yaml = serde_yaml::to_string(&sample_value).expect("YAML serialization");
    let config: Config =
        serde_yaml::from_str(&current_schema_yaml).expect("current schema deserialization");

    assert_eq!(config.app_name, "llm-credit-card-chatbot");
    assert_eq!(config.connections.len(), 1);
    let postgres = match &config.connections[0].config {
        ConnectionConfig::Postgres(config) => config,
        _ => panic!("sample connection must use PostgreSQL"),
    };
    assert_eq!(postgres.database, Some("dozer_llm_chatbot".to_owned()));
    assert_eq!(
        config
            .sources
            .iter()
            .map(|source| source.name.as_str())
            .collect::<Vec<_>>(),
        ["customers", "transactions", "credit_cards"]
    );
    assert_eq!(config.api.rest.enabled, Some(true));
    assert_eq!(config.api.grpc.enabled, Some(true));
}
