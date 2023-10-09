use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;
use crate::errors::SnowflakeError;
use dozer_types::models::ingestion_types::SnowflakeConfig;
use odbc::create_environment_v3;

pub fn remove_streams(
    connection: &SnowflakeConfig,
    table_name: &str,
) -> Result<bool, SnowflakeError> {
    let env = create_environment_v3().unwrap();
    let client = Client::new(connection, &env);

    client.drop_stream(&StreamConsumer::get_stream_table_name(
        table_name,
        &client.get_name(),
    ))
}
