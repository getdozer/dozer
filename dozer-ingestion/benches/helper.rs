use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_orchestrator::Connection;
use dozer_types::indicatif::{ProgressBar, ProgressStyle};
use dozer_types::log::error;
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
    std::thread::spawn(move || {
        let grpc_connector = dozer_ingestion::connectors::get_connector(config.connection).unwrap();

        let mut tables = grpc_connector.get_tables(None).unwrap();
        if let Some(tables_filter) = config.tables_filter {
            tables.retain(|t| tables_filter.contains(&t.table_name));
        }

        let res = grpc_connector.start(None, &ingestor, tables);
        if let Err(e) = res {
            error!("Error: {:?}", e);
        }
    });
    iterator
}
