use dozer_types::{models::app_config::Config, serde_yaml};

use crate::cli::tests::helper::*;

#[test]
fn test_minimal_config() {
    let test_str = minimal_config();
    let config: Config = serde_yaml::from_str::<Config>(test_str).unwrap();
    assert!(!config.connections.is_empty(), "connections are not empty");
}
