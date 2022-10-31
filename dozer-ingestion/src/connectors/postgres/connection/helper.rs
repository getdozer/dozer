use dozer_types::errors::connector::ConnectorError;
use dozer_types::log::error;
use dozer_types::models::connection::Authentication;
use postgres::{Client, Config};
use tokio_postgres::NoTls;

pub fn map_connection_config(auth_details: Authentication) -> tokio_postgres::Config {
    let Authentication::PostgresAuthentication {
        host,
        port,
        user,
        database,
        password,
    } = auth_details;
    tokio_postgres::Config::new()
        .host(&host)
        .port(port)
        .user(&user)
        .dbname(&database)
        .password(password)
        .to_owned()
}

pub fn connect(config: tokio_postgres::Config) -> Result<Client, ConnectorError> {
    Config::from(config)
        .connect(NoTls)
        .map_err(|e| ConnectorError::InternalError(Box::new(e)))
}

pub async fn async_connect(
    config: tokio_postgres::Config,
) -> Result<tokio_postgres::Client, ConnectorError> {
    match config.connect(NoTls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("connection error: {}", e);
                }
            });
            Ok(client)
        }
        Err(e) => Err(ConnectorError::InternalError(Box::new(e))),
    }
}
