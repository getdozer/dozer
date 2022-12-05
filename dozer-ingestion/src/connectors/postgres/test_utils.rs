use crate::connectors::postgres::tests::client::TestPostgresClient;

use dozer_types::models::connection::Authentication;

use load_file::load_str;

pub fn get_client() -> TestPostgresClient {
    let config = serde_yaml::from_str::<Authentication>(load_str!(
        "../../../../config/tests/local/test.postgres.auth.yaml"
    ))
    .unwrap();

    TestPostgresClient::new(&config)
}
