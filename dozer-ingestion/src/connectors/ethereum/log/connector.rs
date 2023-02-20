use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};

use crate::connectors::{Connector, ValidationResults};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::{EthFilter, EthLogConfig};

use dozer_types::serde_json;

use super::helper;
use super::sender::{run, EthDetails};
use dozer_types::types::{ReplicationChangesTrackingType, SourceSchema};
use tokio::runtime::Runtime;
use web3::ethabi::{Contract, Event};
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, H256, U64};

pub struct EthLogConnector {
    pub id: u64,
    config: EthLogConfig,
    // Address -> (contract, contract_name)
    contracts: HashMap<String, ContractTuple>,
    // contract_signacture -> SchemaID
    schema_map: HashMap<H256, usize>,
    conn_name: String,
}

#[derive(Debug, Clone)]
// (Contract, Name)
pub struct ContractTuple(pub Contract, pub String);

pub const ETH_LOGS_TABLE: &str = "eth_logs";
impl EthLogConnector {
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

    pub fn new(id: u64, config: EthLogConfig, conn_name: String) -> Self {
        let mut contracts = HashMap::new();

        for c in &config.contracts {
            let contract = serde_json::from_str(&c.abi).expect("unable to parse contract from abi");
            contracts.insert(
                c.address.to_string().to_lowercase(),
                ContractTuple(contract, c.name.to_string()),
            );
        }

        let schema_map = Self::build_schema_map(&contracts);
        Self {
            id,
            config,
            contracts,
            schema_map,
            conn_name,
        }
    }

    fn build_schema_map(contracts: &HashMap<String, ContractTuple>) -> HashMap<H256, usize> {
        let mut schema_map = HashMap::new();

        let mut signatures = vec![];
        for contract_tuple in contracts.values() {
            let contract = contract_tuple.0.clone();
            let events: Vec<&Event> = contract.events.values().flatten().collect();
            for evt in events {
                signatures.push(evt.signature());
            }
        }
        signatures.sort();

        for (idx, signature) in signatures.iter().enumerate() {
            schema_map.insert(signature.to_owned(), 2 + idx);
        }
        schema_map
    }
}

impl Connector for EthLogConnector {
    fn get_schemas(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        let mut schemas = vec![SourceSchema::new(
            ETH_LOGS_TABLE.to_string(),
            helper::get_eth_schema(),
            ReplicationChangesTrackingType::Nothing,
        )];

        let event_schemas = helper::get_contract_event_schemas(
            self.contracts.to_owned(),
            self.schema_map.to_owned(),
        );
        schemas.extend(event_schemas);

        let schemas = if let Some(tables) = tables {
            schemas
                .iter()
                .filter(|s| tables.iter().any(|t| t.table_name == s.name))
                .cloned()
                .collect()
        } else {
            schemas
        };

        Ok(schemas)
    }

    fn start(
        &self,
        from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        // Start a new thread that interfaces with ETH node
        let wss_url = self.config.wss_url.to_owned();
        let filter = self.config.filter.to_owned().unwrap_or_default();

        Runtime::new().unwrap().block_on(async {
            let details = Arc::new(EthDetails::new(
                wss_url,
                filter,
                ingestor,
                self.contracts.to_owned(),
                tables,
                self.schema_map.to_owned(),
                from_seq,
                self.conn_name.clone(),
            ));
            run(details).await
        })
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        // Return contract parsing error
        for contract in &self.config.contracts {
            let res: Result<Contract, serde_json::Error> = serde_json::from_str(&contract.abi);
            if let Err(e) = res {
                return Err(ConnectorError::map_serialization_error(e));
            }
        }
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }
}
