use crate::PostgresConnectorError;

use super::client::Client;
use dozer_ingestion_connector::{
    dozer_types::{
        self,
        log::{debug, error},
        models::connection::ConnectionConfig,
    },
    retry_on_network_failure, tokio,
};
use rustls::Error;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    SignatureScheme,
};
use std::sync::Arc;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Connection, NoTls, Socket};

pub fn map_connection_config(
    auth_details: &ConnectionConfig,
) -> Result<tokio_postgres::Config, PostgresConnectorError> {
    if let ConnectionConfig::Postgres(postgres) = auth_details {
        let config_replenished = match postgres.replenish() {
            Ok(conf) => conf,
            Err(e) => return Err(PostgresConnectorError::WrongConnectionConfiguration(e)),
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
        panic!("Postgres config was expected")
    }
}

#[derive(Debug)]
pub struct AcceptAllVerifier {}

impl ServerCertVerifier for AcceptAllVerifier {
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }

    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }
}

pub async fn connect(config: tokio_postgres::Config) -> Result<Client, PostgresConnectorError> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in
        rustls_native_certs::load_native_certs().map_err(PostgresConnectorError::LoadNativeCerts)?
    {
        if let Err(e) = roots.add(cert) {
            debug!("Failed to add certificate: {}", e);
        }
    }
    let mut rustls_config = rustls::ClientConfig::builder()
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
        ssl_mode => Err(PostgresConnectorError::InvalidSslError(ssl_mode)),
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
