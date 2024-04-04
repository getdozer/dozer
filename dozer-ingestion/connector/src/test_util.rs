use std::sync::Arc;

use dozer_types::{
    constants::DEFAULT_CONFIG_PATH,
    log::error,
    models::{config::Config, connection::ConnectionConfig},
    node::SourceState,
};
use futures::stream::{AbortHandle, Abortable};
use tokio::runtime::Runtime;

use crate::{Connector, IngestionIterator, Ingestor, TableInfo};

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
    mut connector: impl Connector + 'static,
    tables: Vec<TableInfo>,
) -> (IngestionIterator, AbortHandle) {
    let (ingestor, iterator) = Ingestor::initialize_channel(Default::default());
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    runtime.clone().spawn_blocking(move || {
        runtime.block_on(async move {
            if let Ok(Err(e)) = Abortable::new(
                connector.start(&ingestor, tables, SourceState::NotStarted),
                abort_registration,
            )
            .await
            {
                error!("Connector `start` returned error: {e}")
            }
        })
    });
    (iterator, abort_handle)
}

pub fn spawn_connector_all_tables(
    runtime: Arc<Runtime>,
    mut connector: impl Connector + 'static,
) -> (IngestionIterator, AbortHandle) {
    let tables = runtime.block_on(list_all_table(&mut connector));
    spawn_connector(runtime, connector, tables)
}

pub fn create_runtime_and_spawn_connector_all_tables(
    connector: impl Connector + 'static,
) -> (IngestionIterator, AbortHandle) {
    let runtime = create_test_runtime();
    spawn_connector_all_tables(runtime.clone(), connector)
}

async fn list_all_table(connector: &mut impl Connector) -> Vec<TableInfo> {
    let tables = connector.list_tables().await.unwrap();
    connector.list_columns(tables).await.unwrap()
}

pub fn load_test_connection_config() -> ConnectionConfig {
    let config_path = std::path::PathBuf::from(format!("src/tests/{DEFAULT_CONFIG_PATH}"));

    let dozer_config = std::fs::read_to_string(config_path).unwrap();
    let mut dozer_config = dozer_types::serde_yaml::from_str::<Config>(&dozer_config).unwrap();
    dozer_config.connections.remove(0).config
}
