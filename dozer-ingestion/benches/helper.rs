use std::sync::Arc;

use dozer_ingestion_connector::{
    dozer_types::{
        indicatif::{ProgressBar, ProgressStyle},
        log::error,
        models::connection::Connection,
        serde::{Deserialize, Serialize},
    },
    Connector, IngestionIterator, Ingestor, TableInfo,
};
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "dozer_ingestion_connector::dozer_types::serde")]
pub struct TestConfig {
    pub connection: Connection,
    pub tables_filter: Option<Vec<String>>,
    #[serde(default = "default_size")]
    pub size: usize,
}
fn default_size() -> usize {
    1000
}

pub fn get_progress() -> ProgressBar {
    let pb = ProgressBar::new_spinner();

    pb.set_style(
        ProgressStyle::with_template("{spinner:.blue} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}

pub fn get_connection_iterator(runtime: Arc<Runtime>, config: TestConfig) -> IngestionIterator {
    let connector =
        dozer_ingestion::get_connector(runtime.clone(), config.connection, None).unwrap();
    let tables = runtime.block_on(list_tables(&*connector));
    let (ingestor, iterator) = Ingestor::initialize_channel(Default::default());
    runtime.clone().spawn_blocking(move || async move {
        if let Err(e) = runtime.block_on(connector.start(&ingestor, tables, None)) {
            error!("Error starting connector: {:?}", e);
        }
    });
    iterator
}

async fn list_tables(connector: &dyn Connector) -> Vec<TableInfo> {
    let tables = connector.list_tables().await.unwrap();
    connector.list_columns(tables).await.unwrap()
}
