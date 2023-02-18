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
use dozer_types::types::ReplicationChangesTrackingType;

use tokio::runtime::Runtime;

pub struct EthTraceConnector {
    pub id: u64,
    pub wss_url: String,
    pub config: EthTraceConfig,
    pub conn_name: String,
}

pub const ETH_TRACE_TABLE: &str = "eth_traces";
pub const RETRIES: u16 = 10;
impl EthTraceConnector {
    pub fn new(id: u64, wss_url: String, config: EthTraceConfig, conn_name: String) -> Self {
        Self {
            id,
            wss_url,
            config,
            conn_name,
        }
    }
}

impl Connector for EthTraceConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> Result<
        Vec<(
            String,
            dozer_types::types::Schema,
            ReplicationChangesTrackingType,
        )>,
        ConnectorError,
    > {
        Ok(vec![(
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
        let from_block = self.config.from_block;
        let to_block = self.config.to_block;
        let wss_url = self.wss_url.clone();
        let conn_name = self.conn_name.clone();
        Runtime::new()
            .unwrap()
            .block_on(async { run(wss_url, ingestor, from_block, to_block, conn_name).await })
    }

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        Ok(HashMap::new())
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }
}

pub async fn run(
    wss_url: String,
    ingestor: &Ingestor,
    from_block: u64,
    to_block: Option<u64>,
    conn_name: String,
) -> Result<(), ConnectorError> {
    let client = conn_helper::get_wss_client(&wss_url)
        .await
        .map_err(ConnectorError::EthError)?;

    let mut current_block = from_block;
    let mut error_count = 0;

    info!(
        "Starting Eth Trace connector from block {} {}",
        from_block, conn_name
    );
    loop {
        debug_assert!(
            error_count < RETRIES,
            "Eth Trace connector failed more than {} times",
            RETRIES
        );
        if let Some(to_block) = to_block {
            if current_block > to_block {
                break;
            }
        }
        let results = get_block_traces(client.clone(), current_block).await;

        ingestor
            .handle_message(((current_block, 0), IngestionMessage::Begin()))
            .map_err(ConnectorError::IngestorError)?;

        if let Ok(results) = results {
            for result in results {
                let ops = map_trace_to_ops(&result.result);

                for op in ops {
                    let message = IngestionMessage::OperationEvent(op);
                    ingestor
                        .handle_message(((current_block, 0), message))
                        .map_err(ConnectorError::IngestorError)?;
                }
            }
            error_count = 0;
            current_block += 1;
        } else {
            error_count += 1;
            error!("Failed to get traces for block {}", current_block);
            if error_count < RETRIES {
                error!("Retrying block .. {}", current_block);
            }
        }
    }
    Ok(())
}
