use crate::connectors::postgres::tests::client::TestPostgresClient;

use crate::test_util::load_config;
use dozer_types::models::connection::Authentication;

pub fn get_client() -> TestPostgresClient {
    let config =
        serde_yaml::from_str::<Authentication>(load_config("/test.postgres.auth.yaml")).unwrap();

    TestPostgresClient::new(&config)
}
