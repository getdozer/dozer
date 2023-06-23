use crate::errors::PostgresConnectorError::InvalidSslError;
use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::{debug, error};
use dozer_types::models::connection::ConnectionConfig;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, NoTls};

pub fn map_connection_config(
    auth_details: &ConnectionConfig,
) -> Result<tokio_postgres::Config, ConnectorError> {
    if let ConnectionConfig::Postgres(postgres) = auth_details {
        let config_replenished = postgres.replenish();
        let mut config = tokio_postgres::Config::new();
        config
            .host(&config_replenished.host)
            .port(config_replenished.port as u16)
            .user(&config_replenished.user)
            .dbname(&config_replenished.database)
            .password(&config_replenished.password)
            .ssl_mode(config_replenished.ssl_mode);
        Ok(config)
    } else {
        Err(ConnectorError::WrongConnectionConfiguration)
    }
}

pub async fn connect(config: tokio_postgres::Config) -> Result<Client, PostgresConnectorError> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in
        rustls_native_certs::load_native_certs().map_err(PostgresConnectorError::LoadNativeCerts)?
    {
        if let Err(e) = roots.add(&rustls::Certificate(cert.0)) {
            debug!("Failed to add certificate: {}", e);
        }
    }
    let rustls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    match config.get_ssl_mode() {
        SslMode::Disable => {
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
        SslMode::Prefer => {
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
        SslMode::Require => todo!(),
        SslMode::VerifyCa => todo!(),
        SslMode::VerifyFull => todo!(),
        ssl_mode => Err(InvalidSslError(format!("{:?}", ssl_mode))),
    }
}
