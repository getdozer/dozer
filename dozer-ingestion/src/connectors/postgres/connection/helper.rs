use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::error;
use dozer_types::models::connection::ConnectionConfig;
use tokio_postgres::{Client, NoTls, config::SslMode};

pub fn map_connection_config(
    auth_details: &ConnectionConfig,
) -> Result<tokio_postgres::Config, ConnectorError> {
    if let ConnectionConfig::Postgres(postgres) = auth_details {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&postgres.host)
            .port(postgres.port as u16)
            .user(&postgres.user)
            .dbname(&postgres.database)
            .ssl_mode(if postgres.ssl_verify { SslMode::Require } else { SslMode::Disable })
            .password(&postgres.password);
        Ok(config)
    } else {
        Err(ConnectorError::WrongConnectionConfiguration)
    }
}

pub async fn connect(config: tokio_postgres::Config) -> Result<Client, PostgresConnectorError> {
    let (client, connection) = config
        .connect(NoTls)
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres connection error: {}", e);
        }
    });

    Ok(client)
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
        Err(e) => Err(PostgresConnectorError::ConnectionFailure(e)),
    }
}
