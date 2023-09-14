use super::client::Client;
use crate::errors::ConnectorError::WrongConnectionConfiguration;
use crate::errors::PostgresConnectorError::InvalidSslError;
use crate::errors::{ConnectorError, PostgresConnectorError};
use crate::retry_on_network_failure;
use dozer_types::log::{debug, error};
use dozer_types::models::connection::ConnectionConfig;
use rustls::client::{ServerCertVerified, ServerCertVerifier};
use rustls::{Certificate, Error, ServerName};
use std::sync::Arc;
use std::time::SystemTime;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Connection, NoTls, Socket};

pub fn map_connection_config(
    auth_details: &ConnectionConfig,
) -> Result<tokio_postgres::Config, ConnectorError> {
    if let ConnectionConfig::Postgres(postgres) = auth_details {
        let config_replenished = match postgres.replenish() {
            Ok(conf) => conf,
            Err(e) => return Err(WrongConnectionConfiguration(e)),
        };
        let mut config = tokio_postgres::Config::new();
        config
            .host(&config_replenished.host)
            .port(config_replenished.port as u16)
            .user(&config_replenished.user)
            .dbname(&config_replenished.database)
            .password(&config_replenished.password)
            .ssl_mode(config_replenished.sslmode);
        Ok(config)
    } else {
        Err(ConnectorError::UnavailableConnectionConfiguration(
            "Unable to map connection config".to_string(),
        ))
    }
}

pub struct AcceptAllVerifier {}
impl ServerCertVerifier for AcceptAllVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
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
    let mut rustls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();

    match config.get_ssl_mode() {
        SslMode::Disable => {
            // tokio-postgres::Config::SslMode::Disable + NoTLS connection
            let (client, connection) = connect_helper(config, NoTls)
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
            // tokio-postgres::Config::SslMode::Prefer + TLS connection with no verification
            rustls_config
                .dangerous()
                .set_certificate_verifier(Arc::new(AcceptAllVerifier {}));
            let (client, connection) = connect_helper(
                config,
                tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config),
            )
            .await
            .map_err(PostgresConnectorError::ConnectionFailure)?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Postgres connection error: {}", e);
                }
            });
            Ok(client)
        }
        // SslMode::Allow => unimplemented!(),
        SslMode::Require => {
            // tokio-postgres::Config::SslMode::Require + TLS connection with verification
            let (client, connection) = connect_helper(
                config,
                tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config),
            )
            .await
            .map_err(PostgresConnectorError::ConnectionFailure)?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("Postgres connection error: {}", e);
                }
            });
            Ok(client)
        }
        ssl_mode => Err(InvalidSslError(ssl_mode)),
    }
}

async fn connect_helper<T>(
    config: tokio_postgres::Config,
    tls: T,
) -> Result<(Client, Connection<Socket, T::Stream>), tokio_postgres::Error>
where
    T: tokio_postgres::tls::MakeTlsConnect<Socket> + Clone,
{
    retry_on_network_failure!(
        "connect",
        config.connect(tls.clone()).await,
        is_network_failure
    )
    .map(|(client, connection)| (Client::new(config, client), connection))
}

pub fn is_network_failure(err: &tokio_postgres::Error) -> bool {
    let err_str = err.to_string();
    err_str.starts_with("error communicating with the server")
        || err_str.starts_with("error performing TLS handshake")
        || err_str.starts_with("connection closed")
        || err_str.starts_with("error connecting to server")
        || err_str.starts_with("timeout waiting for server")
        || (err_str.starts_with("db error")
            && err_str.contains("canceling statement due to statement timeout"))
}
