use crate::connectors::postgres::tests::client::TestPostgresClient;

use dozer_types::models::connection::Authentication;

pub fn get_client() -> TestPostgresClient {
    let config = serde_yaml::from_str::<Authentication>(include_str!(
        "../../../../config/test.postgres.sample.yaml"
    ))
    .unwrap();

    TestPostgresClient::new(&config)
}
