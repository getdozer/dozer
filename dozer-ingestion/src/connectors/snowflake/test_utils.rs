use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;
use crate::errors::SnowflakeError;
use dozer_types::models::connection::{Connection, ConnectionConfig};
use odbc::create_environment_v3;

pub fn get_client(connection: &Connection) -> Client {
    let ConnectionConfig::Snowflake(config) = connection.config.as_ref().expect("Expecting connection config") else {
        panic!("Expecting Snowflake connection config")
    };

    Client::new(config)
}

pub fn remove_streams(connection: Connection, table_name: &str) -> Result<bool, SnowflakeError> {
    let client = get_client(&connection);

    let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
    let conn = env
        .connect_with_connection_string(&client.get_conn_string())
        .unwrap();

    client.drop_stream(
        &conn,
        &StreamConsumer::get_stream_table_name(table_name, &client.get_name()),
    )
}
