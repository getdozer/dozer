use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};

use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{
    connectors::{ethereum::helper, TableInfo},
    errors::ConnectorError,
};
use dozer_types::ingestion_types::{EthConfig, EthFilter};
use dozer_types::log::info;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_json;

use super::sender::{run, EthDetails};
use tokio::runtime::Runtime;
use web3::ethabi::{Contract, Event};
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, H256, U64};
pub struct EthConnector {
    pub id: u64,
    config: EthConfig,
    contract: Option<Contract>,
    tables: Option<Vec<TableInfo>>,
    schema_map: HashMap<String, usize>,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
}

pub const ETH_LOGS_TABLE: &str = "eth_logs";
impl EthConnector {
    pub fn build_filter(filter: &EthFilter) -> Filter {
        let builder = FilterBuilder::default();

        // Optionally add a from_block filter
        let builder = match filter.from_block {
            Some(block_no) => builder.from_block(BlockNumber::Number(U64::from(block_no))),
            None => builder,
        };
        // Optionally add a to_block filter
        let builder = match filter.to_block {
            Some(block_no) => builder.to_block(BlockNumber::Number(U64::from(block_no))),
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
            .to_owned().map(|s| serde_json::from_str(&s).unwrap());

        let schema_map = Self::build_schema_map(&contract);
        Self {
            id,
            config,
            contract,
            schema_map,
            tables: None,
            ingestor: None,
        }
    }

    fn build_schema_map(contract: &Option<Contract>) -> HashMap<String, usize> {
        let mut schema_map = HashMap::new();
        schema_map.insert(ETH_LOGS_TABLE.to_string(), 1);

        if let Some(contract) = contract {
            let mut events: Vec<&Event> = contract.events.values().flatten().collect();
            events.sort_by(|a, b| a.name.to_string().cmp(&b.name.to_string()));

            for (idx, evt) in events.iter().enumerate() {
                schema_map.insert(evt.name.to_string(), 2 + idx);
            }
        }
        schema_map
    }
}

impl Connector for EthConnector {
    fn get_schemas(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
        let schemas = vec![(ETH_LOGS_TABLE.to_string(), helper::get_eth_schema())];
        let schemas = if let Some(contract) = &self.contract {
            let event_schemas =
                helper::get_contract_event_schemas(contract, self.schema_map.to_owned());

            [schemas, event_schemas].concat()
        } else {
            schemas
        };
        info!("Initializing schemas: {:?}", schemas);

        let schemas = if let Some(tables) = tables {
            schemas
                .iter()
                .filter(|(n, _)| tables.iter().any(|t| t.name == *n))
                .cloned()
                .collect()
        } else {
            schemas
        };
        info!("Initializing schemas: {:?}", schemas);
        Ok(schemas)
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
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self) -> Result<(), ConnectorError> {
        // Start a new thread that interfaces with ETH node
        let wss_url = self.config.wss_url.to_owned();
        let filter = self
            .config
            .filter
            .to_owned()
            .unwrap_or_default();

        let connector_id = self.id;

        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();

        Runtime::new().unwrap().block_on(async {
            let details = Arc::new(EthDetails::new(
                wss_url,
                filter,
                ingestor,
                connector_id,
                self.contract.to_owned(),
                self.tables.to_owned(),
                self.schema_map.to_owned(),
            ));
            run(details).await
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
