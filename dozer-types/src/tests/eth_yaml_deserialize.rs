use crate::{
    ingestion_types::{EthConfig, EthFilter, EthLogConfig},
    models::connection::ConnectionConfig,
};
#[test]
fn standard() {
    let eth_config = r#"
  !Ethereum  
    provider: !Log
        wss_url: wss://link
        filter:
            from_block: 0
            addresses: []
            topics: []
        contracts: []
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(eth_config).unwrap();
    let expected_eth_filter = EthFilter {
        from_block: Some(0),
        to_block: None,
        addresses: vec![],
        topics: vec![],
    };
    let expected_eth_config = EthConfig {
        provider: crate::ingestion_types::EthProviderConfig::Log(EthLogConfig {
            filter: Some(expected_eth_filter),
            wss_url: "wss://link".to_owned(),
            contracts: vec![],
        }),
    };
    let expected = ConnectionConfig::Ethereum(expected_eth_config);
    assert_eq!(expected, deserializer_result);
}

#[test]
fn config_without_empty_array() {
    let eth_config = r#"
  !Ethereum  
    provider: !Log
        wss_url: wss://link
        filter:
            from_block: 499203
  "#;
    let deserializer_result = serde_yaml::from_str::<ConnectionConfig>(eth_config).unwrap();
    let expected_eth_filter = EthFilter {
        from_block: Some(499203),
        to_block: None,
        addresses: vec![],
        topics: vec![],
    };
    let expected_eth_config = EthConfig {
        provider: crate::ingestion_types::EthProviderConfig::Log(EthLogConfig {
            wss_url: "wss://link".to_owned(),
            filter: Some(expected_eth_filter),
            contracts: vec![],
        }),
    };
    let expected = ConnectionConfig::Ethereum(expected_eth_config);
    assert_eq!(expected, deserializer_result);
}
