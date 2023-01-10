use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::error;
use dozer_types::models::connection::Authentication;
use postgres::{Client, Config};
use tokio_postgres::NoTls;

pub fn map_connection_config(
    auth_details: &Authentication,
) -> Result<tokio_postgres::Config, ConnectorError> {
    if let Authentication::Postgres(postgres) = auth_details {
        Ok(tokio_postgres::Config::new()
            .host(&postgres.host)
            .port(postgres.port as u16)
            .user(&postgres.user)
            .dbname(&postgres.database)
            .password(&postgres.password)
            .to_owned())
    } else {
        Err(ConnectorError::WrongConnectionConfiguration)
    }
}

pub fn connect(config: tokio_postgres::Config) -> Result<Client, PostgresConnectorError> {
    Config::from(config)
        .connect(NoTls)
        .map_err(PostgresConnectorError::ConnetionFailure)
}

pub async fn async_connect(
    config: tokio_postgres::Config,
) -> Result<tokio_postgres::Client, PostgresConnectorError> {
    match config.connect(NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("connection error: {}", e);
                }
            });
            Ok(client)
        }
        Err(e) => Err(PostgresConnectorError::ConnetionFailure(e)),
    }
}
