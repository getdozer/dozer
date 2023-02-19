use std::collections::HashMap;

use super::super::helper as conn_helper;
use super::helper::{self, get_block_traces, map_trace_to_ops};
use crate::connectors::ValidationResults;
use crate::{
    connectors::{Connector, TableInfo},
    errors::ConnectorError,
    ingestion::Ingestor,
};
use dozer_types::ingestion_types::{EthTraceConfig, IngestionMessage};
use dozer_types::log::{error, info};
use dozer_types::types::{ReplicationChangesTrackingType, SourceSchema};

use tokio::runtime::Runtime;

pub struct EthTraceConnector {
    pub id: u64,
    pub config: EthTraceConfig,
    pub conn_name: String,
}

pub const ETH_TRACE_TABLE: &str = "eth_traces";
pub const RETRIES: u16 = 10;
impl EthTraceConnector {
    pub fn new(id: u64, config: EthTraceConfig, conn_name: String) -> Self {
        Self {
            id,
            config,
            conn_name,
        }
    }
}

impl Connector for EthTraceConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        Ok(vec![SourceSchema::new(
            ETH_TRACE_TABLE.to_string(),
            helper::get_trace_schema(),
            ReplicationChangesTrackingType::Nothing,
        )])
    }

    fn start(
        &self,
        _from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        _tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        let config = self.config.clone();
        let conn_name = self.conn_name.clone();
        Runtime::new()
            .unwrap()
            .block_on(async { run(ingestor, config, conn_name).await })
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        let config = self.config.clone();
        Runtime::new()
            .unwrap()
            .block_on(async { validate(config).await })
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }
}

pub async fn validate(config: EthTraceConfig) -> Result<(), ConnectorError> {
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
    let mut batch_iter = BatchIterator::new(config.from_block, config.to_block, config.batch_size);

    ingestor
        .handle_message(((config.from_block, 0), IngestionMessage::Begin()))
        .map_err(ConnectorError::IngestorError)?;

    let mut errors: Vec<ConnectorError> = vec![];
    while let Some(batch) = batch_iter.next() {
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
                            let message = IngestionMessage::OperationEvent(op);
                            ingestor
                                .handle_message(((batch.0, 0), message))
                                .map_err(ConnectorError::IngestorError)?;
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
