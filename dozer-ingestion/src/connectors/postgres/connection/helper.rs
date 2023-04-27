use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::error;
use dozer_types::models::connection::ConnectionConfig;
use tokio_postgres::Client;

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
            .password(&postgres.password);
        Ok(config)
    } else {
        Err(ConnectorError::WrongConnectionConfiguration)
    }
}

pub async fn connect(config: tokio_postgres::Config) -> Result<Client, PostgresConnectorError> {
    let rustls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    let (client, connection) = config
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config))
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres connection error: {}", e);
        }
    });

    Ok(client)
}
