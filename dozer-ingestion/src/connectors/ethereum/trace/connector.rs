use super::super::helper as conn_helper;
use super::helper::{self, get_block_traces, map_trace_to_ops};
use crate::connectors::{
    table_name, CdcType, ConnectorMeta, ConnectorStart, SourceSchema, SourceSchemaResult,
    TableIdentifier, TableToIngest,
};
use crate::{connectors::TableInfo, errors::ConnectorError, ingestion::Ingestor};
use dozer_types::ingestion_types::{EthTraceConfig, IngestionMessage};
use dozer_types::log::{error, info, warn};

use tonic::async_trait;

#[derive(Debug)]
pub struct EthTraceConnector {
    pub config: EthTraceConfig,
    pub conn_name: String,
}

pub const ETH_TRACE_TABLE: &str = "eth_traces";
pub const RETRIES: u16 = 10;
impl EthTraceConnector {
    pub fn new(config: EthTraceConfig, conn_name: String) -> Self {
        Self { config, conn_name }
    }
}

#[async_trait]
impl ConnectorMeta for EthTraceConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        validate(&self.config).await
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        Ok(vec![TableIdentifier::from_table_name(
            ETH_TRACE_TABLE.to_string(),
        )])
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        for table in tables {
            if table.name != ETH_TRACE_TABLE || table.schema.is_some() {
                return Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                )));
            }
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let mut result = Vec::new();
        for table in tables {
            if table.name != ETH_TRACE_TABLE || table.schema.is_some() {
                return Err(ConnectorError::TableNotFound(table_name(
                    table.schema.as_deref(),
                    &table.name,
                )));
            }
            let column_names = helper::get_trace_schema()
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect();
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
        _table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        warn!("TODO: respect table_infos");
        Ok(vec![Ok(SourceSchema::new(
            helper::get_trace_schema(),
            CdcType::Nothing,
        ))])
    }
}

#[async_trait(?Send)]
impl ConnectorStart for EthTraceConnector {
    async fn start(
        &self,
        ingestor: &Ingestor,
        _tables: Vec<TableToIngest>,
    ) -> Result<(), ConnectorError> {
        let config = self.config.clone();
        let conn_name = self.conn_name.clone();
        run(ingestor, config, conn_name).await
    }
}

pub async fn validate(config: &EthTraceConfig) -> Result<(), ConnectorError> {
    // Check if transport can be initialized
    let tuple = conn_helper::get_batch_http_client(&config.https_url)
        .await
        .map_err(ConnectorError::EthError)?;

    // Check if debug API is available
    get_block_traces(tuple, (1000000, 1000005)).await?;

    Ok(())
}

pub async fn run(
    ingestor: &Ingestor,
    config: EthTraceConfig,
    conn_name: String,
) -> Result<(), ConnectorError> {
    let client_tuple = conn_helper::get_batch_http_client(&config.https_url)
        .await
        .map_err(ConnectorError::EthError)?;

    info!(
        "Starting Eth Trace connector: {} from block {}",
        conn_name, config.from_block
    );
    let batch_iter = BatchIterator::new(config.from_block, config.to_block, config.batch_size);

    let mut errors: Vec<ConnectorError> = vec![];
    for batch in batch_iter {
        for retry in 0..RETRIES {
            if retry >= RETRIES - 1 {
                error!("Eth Trace connector failed more than {RETRIES} times");
                return Err(errors.pop().unwrap());
            }

            let res = get_block_traces(client_tuple.clone(), batch).await;
            match res {
                Ok(arr) => {
                    for result in arr {
                        let ops = map_trace_to_ops(&result.result);

                        for op in ops {
                            ingestor
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index: 0, // We have only one table
                                    op,
                                    id: None,
                                })
                                .await
                                .map_err(|_| ConnectorError::IngestorError)?;
                        }
                    }

                    break;
                }
                Err(e) => {
                    errors.push(e);
                    error!(
                        "Failed to get traces for block {}.. Attempt {}",
                        batch.0,
                        retry + 1
                    );
                }
            }
        }
    }

    Ok(())
}

pub struct BatchIterator {
    current_block: u64,
    to_block: Option<u64>,
    batch_size: u64,
}

impl BatchIterator {
    pub fn new(current_block: u64, to_block: Option<u64>, batch_size: u64) -> Self {
        Self {
            current_block,
            to_block,
            batch_size,
        }
    }
}
impl Iterator for BatchIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let mut end_block = self.current_block + self.batch_size;
        if let Some(to_block) = self.to_block {
            if self.current_block > to_block {
                return None;
            }

            if end_block > to_block + 1 {
                end_block = to_block + 1;
            }
        }
        let current_batch = (self.current_block, end_block);
        self.current_block = end_block;
        Some(current_batch)
    }
}
