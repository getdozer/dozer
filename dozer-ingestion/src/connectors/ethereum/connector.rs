use core::time;
use std::{str::FromStr, sync::Arc};

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{
    connectors::{ethereum::helper, TableInfo},
    errors::ConnectorError,
};
use dozer_types::ingestion_types::{EthConfig, EthFilter, IngestionMessage};
use dozer_types::parking_lot::RwLock;
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::runtime::Runtime;
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, H256, U64};

pub struct EthConnector {
    pub id: u64,
    filter: Filter,
    config: EthConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

const TABLE_NAME: &str = "eth_logs";
impl EthConnector {
    pub fn build_filter(filter: &EthFilter) -> Filter {
        let builder = FilterBuilder::default();

        // Optionally add a block_no filter
        let builder = match filter.from_block {
            Some(block_no) => builder.from_block(BlockNumber::Number(U64::from(block_no))),
            None => builder,
        };
        // Optionally Add Address filter
        let builder = match filter.addresses.is_empty() {
            false => {
                let addresses = filter
                    .addresses
                    .iter()
                    .map(|a| Address::from_str(a).unwrap())
                    .collect();
                builder.address(addresses)
            }
            true => builder,
        };

        // Optionally add topics
        let builder = match filter.topics.is_empty() {
            false => {
                let topics: Vec<Vec<H256>> = filter
                    .topics
                    .iter()
                    .map(|t| vec![H256::from_str(t).unwrap()])
                    .collect();
                builder.topics(
                    topics.get(0).cloned(),
                    topics.get(1).cloned(),
                    topics.get(2).cloned(),
                    topics.get(3).cloned(),
                )
            }
            true => builder,
        };

        builder.build()
    }

    pub fn new(id: u64, config: EthConfig) -> Self {
        let filter = Self::build_filter(&config.to_owned().filter.unwrap());
        Self {
            id,
            config,
            filter,
            ingestor: None,
        }
    }
}

impl Connector for EthConnector {
    fn get_schemas(
        &self,
        _: Option<Vec<TableInfo>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        Ok(vec![(TABLE_NAME.to_string(), helper::get_eth_schema())])
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        Ok(vec![TableInfo {
            name: TABLE_NAME.to_string(),
            id: 1,
            columns: Some(helper::get_columns()),
        }])
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        _: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        Ok(())
    }

    fn start(&self) -> Result<(), ConnectorError> {
        // Start a new thread that interfaces with ETH node
        let wss_url = self.config.wss_url.to_owned();

        let https_url = self.config.https_url.to_owned();

        let filter = self.filter.to_owned();
        let filter_https = filter.clone();

        let connector_id = self.id;

        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();

        let ingestor_https = ingestor.clone();

        Runtime::new().unwrap().block_on(async {
            let counter = Arc::new(AtomicUsize::new(0));

            let counter_https = counter.clone();
            // get past events separately
            tokio::spawn(async move {
                get_past_events(
                    https_url,
                    filter_https,
                    ingestor_https,
                    connector_id,
                    counter_https,
                )
                .await
            });

            _run(wss_url, filter, ingestor, connector_id, counter).await
        })
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[allow(unreachable_code)]
async fn _run(
    wss_url: String,
    filter: Filter,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    counter: Arc<AtomicUsize>,
) -> Result<(), ConnectorError> {
    let client = helper::get_wss_client(&wss_url).await.unwrap();

    let stream = client
        .eth_subscribe()
        .subscribe_logs(filter.clone())
        .await
        .unwrap();

    tokio::pin!(stream);

    loop {
        let idx = counter.fetch_add(1, Ordering::Relaxed);

        let msg = stream.next().await;

        let msg = msg
            .map_or(Err(ConnectorError::EmptyMessage), Ok)?
            .map_err(ConnectorError::EthError)?;

        let msg = helper::map_log_to_event(msg, idx);
        ingestor
            .write()
            .handle_message((connector_id, IngestionMessage::OperationEvent(msg)))
            .map_err(ConnectorError::IngestorError)?;
    }
    Ok(())
}

#[allow(unreachable_code)]
async fn get_past_events(
    https_url: String,
    filter: Filter,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    counter: Arc<AtomicUsize>,
) -> Result<(), ConnectorError> {
    let client = helper::get_https_client(&https_url).await.unwrap();

    let filter = client
        .eth_filter()
        .create_logs_filter(filter)
        .await
        .map_err(ConnectorError::EthError)?;

    let stream = filter.stream(time::Duration::from_secs(1));
    tokio::pin!(stream);

    loop {
        let idx = counter.fetch_add(1, Ordering::Relaxed);
        let msg = stream.next().await;

        let msg = msg
            .map_or(Err(ConnectorError::EmptyMessage), Ok)?
            .map_err(ConnectorError::EthError)?;

        let msg = helper::map_log_to_event(msg, idx);
        ingestor
            .write()
            .handle_message((connector_id, IngestionMessage::OperationEvent(msg)))
            .map_err(ConnectorError::IngestorError)?;
    }
    Ok(())
}
