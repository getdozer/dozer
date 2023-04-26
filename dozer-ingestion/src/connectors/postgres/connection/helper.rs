use crate::errors::{ConnectorError, PostgresConnectorError};
use dozer_types::log::error;
use dozer_types::models::connection::ConnectionConfig;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::config::SslMode;
use tokio_postgres::{Client, Config, NoTls};

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
    match make_tls_connector(&config)? {
        None => connect_no_tls(&config).await,
        Some(tls) => connect_tls(&config, tls).await,
    }
}

async fn connect_tls(
    config: &Config,
    tls: MakeTlsConnector,
) -> Result<Client, PostgresConnectorError> {
    let (client, connection) = config
        .clone()
        .connect(tls)
        .await
        .map_err(PostgresConnectorError::ConnectionFailure)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres connection error: {}", e);
        }
    });

    Ok(client)
}

async fn connect_no_tls(config: &Config) -> Result<Client, PostgresConnectorError> {
    let (client, connection) = config
        .clone()
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

fn make_tls_connector(
    pg_config: &Config,
) -> Result<Option<MakeTlsConnector>, PostgresConnectorError> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())
        .map_err(|e| PostgresConnectorError::SSLError(e.into()))?;
    let ssl_mode = pg_config.get_ssl_mode();
    let (verify_ca, verify_hostname) = match ssl_mode {
        SslMode::Disable | SslMode::Prefer => (false, false),
        // TODO! support tls & multiple ssl modes.
        _ => return Ok(None),
    };

    if !verify_ca {
        builder.set_verify(SslVerifyMode::NONE); // do not verify CA
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    if !verify_hostname {
        tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        });
    }

    Ok(Some(tls_connector))
}
