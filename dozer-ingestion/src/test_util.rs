use std::sync::Arc;

use dozer_types::log::error;
use futures::stream::{AbortHandle, Abortable};
use tokio::runtime::Runtime;

use crate::{
    connectors::{Connector, TableInfo, TableToIngest},
    ingestion::{IngestionIterator, Ingestor},
};

#[cfg(test)]
pub async fn run_connector_test<
    F: futures::Future,
    T: (FnOnce(dozer_types::models::config::Config) -> F) + std::panic::UnwindSafe,
>(
    db_type: &str,
    test: T,
) {
    use dozer_types::{
        constants::DEFAULT_CONFIG_PATH,
        models::{config::Config, connection::ConnectionConfig},
    };

    use crate::connectors::postgres::tests::client::TestPostgresClient;

    let dozer_config_path =
        std::path::PathBuf::from(format!("src/tests/cases/{db_type}/{DEFAULT_CONFIG_PATH}"));

    let dozer_config = std::fs::read_to_string(dozer_config_path).unwrap();
    let dozer_config = dozer_types::serde_yaml::from_str::<Config>(&dozer_config).unwrap();

    let connection = dozer_config.connections.get(0).unwrap();
    if let Some(ConnectionConfig::Postgres(connection_config)) = connection.config.clone() {
        let mut config = tokio_postgres::Config::new();
        let replenished_config = connection_config.replenish().unwrap();
        config
            .user(&replenished_config.user)
            .host(&replenished_config.host)
            .password(&replenished_config.password)
            .port(replenished_config.port as u16)
            .ssl_mode(replenished_config.sslmode);

        let mut client = TestPostgresClient::new_with_postgres_config(config).await;
        client
            .execute_query(&format!(
                "DROP DATABASE IF EXISTS {}",
                replenished_config.database
            ))
            .await;
        client
            .execute_query(&format!("CREATE DATABASE {}", replenished_config.database))
            .await;
    }

    test(dozer_config).await;
}

pub fn create_test_runtime() -> Arc<Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    )
}

pub fn spawn_connector(
    runtime: Arc<Runtime>,
    connector: impl Connector + 'static,
    tables: Vec<TableInfo>,
) -> (IngestionIterator, AbortHandle) {
    let (ingestor, iterator) = Ingestor::initialize_channel(Default::default());
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let tables = tables
        .into_iter()
        .map(TableToIngest::from_scratch)
        .collect();
    runtime.clone().spawn_blocking(move || {
        runtime.block_on(async move {
            if let Ok(Err(e)) =
                Abortable::new(connector.start(&ingestor, tables), abort_registration).await
            {
                error!("Connector `start` returned error: {e}")
            }
        })
    });
    (iterator, abort_handle)
}

pub fn spawn_connector_all_tables(
    runtime: Arc<Runtime>,
    connector: impl Connector + 'static,
) -> (IngestionIterator, AbortHandle) {
    let tables = runtime.block_on(list_all_table(&connector));
    spawn_connector(runtime, connector, tables)
}

pub fn create_runtime_and_spawn_connector_all_tables(
    connector: impl Connector + 'static,
) -> (IngestionIterator, AbortHandle) {
    let runtime = create_test_runtime();
    spawn_connector_all_tables(runtime.clone(), connector)
}

async fn list_all_table(connector: &impl Connector) -> Vec<TableInfo> {
    let tables = connector.list_tables().await.unwrap();
    connector.list_columns(tables).await.unwrap()
}
