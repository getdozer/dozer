use core::time;
use std::{str::FromStr, sync::Arc};

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{
    connectors::{ethereum::helper, TableInfo},
    errors::ConnectorError,
};
use dozer_types::ingestion_types::{EthConfig, EthFilter, IngestionMessage};
use dozer_types::log::info;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_json;
use futures::StreamExt;

use tokio::runtime::Runtime;
use web3::ethabi::Contract;
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, Log, H256, U64};

pub struct EthConnector {
    pub id: u64,
    config: EthConfig,
    contract: Option<Contract>,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

const ETH_LOGS_TABLE: &str = "eth_logs";
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
        let contract: Option<Contract> = config
            .contract_abi
            .to_owned()
            .map_or(None, |s| Some(serde_json::from_str(&s).unwrap()));

        Self {
            id,
            config,
            contract,
            ingestor: None,
        }
    }
}

impl Connector for EthConnector {
    fn get_schemas(
        &self,
        _: Option<Vec<TableInfo>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        let schemas = vec![(ETH_LOGS_TABLE.to_string(), helper::get_eth_schema())];
        let schemas = if let Some(contract) = &self.contract {
            let event_schemas = helper::get_contract_event_schemas(contract);

            Ok([schemas, event_schemas].concat())
        } else {
            Ok(schemas)
        };
        info!("Initializing schemas: {:?}", schemas);
        schemas
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        let schemas = self.get_schemas(None)?;

        let tables = schemas
            .iter()
            .enumerate()
            .map(|(id, (name, schema))| TableInfo {
                name: name.to_string(),
                id: id as u32,
                columns: Some(schema.fields.iter().map(|f| f.name.to_owned()).collect()),
            })
            .collect();
        Ok(tables)
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
        let filter = self
            .config
            .filter
            .to_owned()
            .unwrap_or(EthFilter::default());

        let connector_id = self.id;

        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();

        Runtime::new().unwrap().block_on(async {
            run(
                wss_url,
                filter,
                ingestor,
                connector_id,
                self.contract.to_owned(),
            )
            .await
        })
    }

    fn stop(&self) {}

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if let Some(contract_abi) = self.config.contract_abi.to_owned() {
            let res: Result<Contract, serde_json::Error> = serde_json::from_str(&contract_abi);

            // Return contract parsing error
            if let Err(e) = res {
                return Err(ConnectorError::map_serialization_error(e));
            }
        }

        Ok(())
    }
}

#[allow(unreachable_code)]
pub async fn run(
    wss_url: String,
    filter: EthFilter,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    contract: Option<Contract>,
) -> Result<(), ConnectorError> {
    let client = helper::get_wss_client(&wss_url).await.unwrap();

    // Get current block no.
    let current_block = client
        .eth()
        .block_number()
        .await
        .map_err(ConnectorError::EthError)?
        .as_u64();

    let from_block = filter.from_block();

    // Get past logs till last block per block
    for block_no in from_block..(current_block) {
        let mut filter = filter.clone();
        filter.from_block = Some(block_no);

        let logs = client
            .eth()
            .logs(EthConnector::build_filter(&filter))
            .await
            .map_err(ConnectorError::EthError)?;

        info!("Block: {}, logs length: {}", block_no, logs.len());
        for msg in logs {
            process_log(msg, ingestor.clone(), connector_id, contract.to_owned())?;
        }
    }

    // Create a filter from the last block to check for changes
    let mut filter = filter.clone();
    filter.from_block = Some(current_block);

    info!("Fetching from current_block ..Block: {}", current_block);

    let filter = client
        .eth_filter()
        .create_logs_filter(EthConnector::build_filter(&filter))
        .await
        .map_err(ConnectorError::EthError)?;

    let stream = filter.stream(time::Duration::from_secs(1));

    tokio::pin!(stream);

    loop {
        let msg = stream.next().await;

        let msg = msg
            .map_or(Err(ConnectorError::EmptyMessage), Ok)?
            .map_err(ConnectorError::EthError)?;

        process_log(msg, ingestor.clone(), connector_id, contract.to_owned())?;
    }
    Ok(())
}

fn process_log(
    msg: Log,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
    contract: Option<Contract>,
) -> Result<(), ConnectorError> {
    if let Some(op) = helper::map_log_to_event(msg.to_owned()) {
        // Write eth_log record
        ingestor
            .write()
            .handle_message((connector_id, IngestionMessage::OperationEvent(op)))
            .map_err(ConnectorError::IngestorError)?;

        // write event record optionally
        if let Some(ref contract) = contract {
            let op = helper::decode_event(msg.to_owned(), contract.to_owned());
            ingestor
                .write()
                .handle_message((connector_id, IngestionMessage::OperationEvent(op)))
                .map_err(ConnectorError::IngestorError)?;
        }
    }
    Ok(())
}
