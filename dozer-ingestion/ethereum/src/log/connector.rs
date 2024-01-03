use std::collections::HashMap;
use std::{str::FromStr, sync::Arc};

use super::helper;
use super::sender::{run, EthDetails};
use dozer_ingestion_connector::utils::TableNotFound;
use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        log::warn,
        models::ingestion_types::{EthFilter, EthLogConfig},
        serde_json,
        types::FieldType,
    },
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
    TableToIngest,
};
use web3::ethabi::{Contract, Event};
use web3::types::{Address, BlockNumber, Filter, FilterBuilder, H256, U64};

#[derive(Debug)]
pub struct EthLogConnector {
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
                    topics.first().cloned(),
                    topics.get(1).cloned(),
                    topics.get(2).cloned(),
                    topics.get(3).cloned(),
                )
            }
            true => builder,
        };

        builder.build()
    }

    pub fn new(config: EthLogConfig, conn_name: String) -> Self {
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

#[async_trait]
impl Connector for EthLogConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        // Return contract parsing error
        for contract in &self.config.contracts {
            serde_json::from_str(&contract.abi)?;
        }
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let event_schema_names = helper::get_contract_event_schemas(&self.contracts)
            .into_iter()
            .map(|(name, _)| TableIdentifier::from_table_name(name));
        let mut result = vec![TableIdentifier::from_table_name(ETH_LOGS_TABLE.to_string())];
        result.extend(event_schema_names);
        Ok(result)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let existing_tables = self.list_tables().await?;
        for table in tables {
            if !existing_tables.contains(table) || table.schema.is_some() {
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let event_schemas = helper::get_contract_event_schemas(&self.contracts);
        let mut result = vec![];
        for table in tables {
            let column_names = if table.name == ETH_LOGS_TABLE && table.schema.is_none() {
                helper::get_eth_schema()
                    .fields
                    .into_iter()
                    .map(|field| field.name)
                    .collect()
            } else if let Some((_, schema)) = event_schemas
                .iter()
                .find(|(name, _)| name == &table.name && table.schema.is_none())
            {
                schema
                    .schema
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect()
            } else {
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
            };
            result.push(TableInfo {
                schema: table.schema,
                name: table.name,
                column_names,
            })
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let mut schemas = vec![(
            ETH_LOGS_TABLE.to_string(),
            SourceSchema::new(helper::get_eth_schema(), CdcType::Nothing),
        )];

        let event_schemas = helper::get_contract_event_schemas(&self.contracts);
        schemas.extend(event_schemas);

        let mut result = vec![];
        for table in table_infos {
            if let Some((_, schema)) = schemas
                .iter()
                .find(|(name, _)| name == &table.name && table.schema.is_none())
            {
                warn!("TODO: filter columns");
                result.push(Ok(schema.clone()));
            } else {
                result.push(Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into()));
            }
        }

        Ok(result)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), BoxedError> {
        // Start a new thread that interfaces with ETH node
        let wss_url = self.config.wss_url.to_owned();
        let filter = self.config.filter.to_owned().unwrap_or_default();

        let details = Arc::new(EthDetails::new(
            wss_url,
            filter,
            ingestor,
            self.contracts.to_owned(),
            tables,
            self.schema_map.to_owned(),
            self.conn_name.clone(),
        ));
        run(details).await
    }
}
