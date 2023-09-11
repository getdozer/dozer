use dozer_ingestion::connectors::{TableIdentifier, TableToIngest};
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_types::indicatif::{ProgressBar, ProgressStyle};
use dozer_types::log::error;
use dozer_types::models::connection::Connection;
use dozer_types::serde::{self, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
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

pub async fn get_connection_iterator(config: TestConfig) -> IngestionIterator {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
    tokio::spawn(async move {
        let grpc_connector = dozer_ingestion::connectors::get_connector(config.connection).unwrap();

        let tables = match config.tables_filter.map(|table_names| {
            table_names
                .into_iter()
                .map(TableIdentifier::from_table_name)
                .collect()
        }) {
            Some(tables) => tables,
            None => grpc_connector.list_tables().await.unwrap(),
        };
        let tables = grpc_connector.list_columns(tables).await.unwrap();
        let tables = tables
            .into_iter()
            .map(TableToIngest::from_scratch)
            .collect();

        let res = grpc_connector.start(&ingestor, tables).await;
        if let Err(e) = res {
            error!("Error: {:?}", e);
        }
    });
    iterator
}
