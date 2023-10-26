use dozer_ingestion_connector::dozer_types::models::ingestion_types::SnowflakeConfig;
use odbc::create_environment_v3;

use crate::{connection::client::Client, stream_consumer::StreamConsumer, SnowflakeError};

pub fn remove_streams(
    connection: SnowflakeConfig,
    table_name: &str,
) -> Result<bool, SnowflakeError> {
    let env = create_environment_v3().unwrap();
    let client = Client::new(connection, &env);

    client.drop_stream(&StreamConsumer::get_stream_table_name(
        table_name,
        &client.get_name(),
    ))
}
