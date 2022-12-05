use crate::connectors::postgres::tests::client::TestPostgresClient;

use dozer_types::models::connection::Authentication;

pub fn get_client() -> TestPostgresClient {
    let config = serde_yaml::from_str::<Authentication>(include_str!(
        "../../../../config/tests/test.postgres.auth.yaml"
    ))
    .unwrap();

    TestPostgresClient::new(&config)
}
