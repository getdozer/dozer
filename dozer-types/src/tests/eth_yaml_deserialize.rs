use crate::{
    ingestion_types::{EthConfig, EthFilter},
    models::connection::Authentication,
};
#[test]
fn test_deserialize_eth_config_standard() {
    let eth_config = r#"
  !Ethereum  
  filter:
    from_block: 0
    addresses: []
    topics: []
  wss_url: wss://link
  contracts: []
  "#;
    let deserializer_result = serde_yaml::from_str::<Authentication>(eth_config).unwrap();
    let expected_eth_filter = EthFilter {
        from_block: Some(0),
        to_block: None,
        addresses: vec![],
        topics: vec![],
    };
    let expected_eth_config = EthConfig {
        filter: Some(expected_eth_filter),
        wss_url: "wss://link".to_owned(),
        contracts: vec![],
    };
    let expected = Authentication::Ethereum(expected_eth_config);
    assert_eq!(expected, deserializer_result);
}

#[test]
fn test_deserialize_eth_config_without_empty_array() {
    let eth_config = r#"
  !Ethereum  
  filter:
    from_block: 499203
  wss_url: wss://link
  "#;
    let deserializer_result = serde_yaml::from_str::<Authentication>(eth_config).unwrap();
    let expected_eth_filter = EthFilter {
        from_block: Some(499203),
        to_block: None,
        addresses: vec![],
        topics: vec![],
    };
    let expected_eth_config = EthConfig {
        filter: Some(expected_eth_filter),
        wss_url: "wss://link".to_owned(),
        contracts: vec![],
    };
    let expected = Authentication::Ethereum(expected_eth_config);
    assert_eq!(expected, deserializer_result);
}
