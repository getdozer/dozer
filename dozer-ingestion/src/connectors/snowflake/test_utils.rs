use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;
use crate::errors::SnowflakeError;
use dozer_types::ingestion_types::SnowflakeConfig;
use odbc::create_environment_v3;

pub fn remove_streams(
    connection: &SnowflakeConfig,
    table_name: &str,
) -> Result<bool, SnowflakeError> {
    let client = Client::new(connection);

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let conn = env
        .connect_with_connection_string(&client.get_conn_string())
        .unwrap();

    client.drop_stream(
        &conn,
        &StreamConsumer::get_stream_table_name(table_name, &client.get_name()),
    )
}
