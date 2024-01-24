use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        log::{error, info, warn},
        models::ingestion_types::{default_batch_size, EthTraceConfig, IngestionMessage},
        node::RestartableState,
        types::FieldType,
    },
    utils::TableNotFound,
    CdcType, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
};

use super::super::helper as conn_helper;
use super::helper::{self, get_block_traces, map_trace_to_ops};

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
impl Connector for EthTraceConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        validate(&self.config).await
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(vec![TableIdentifier::from_table_name(
            ETH_TRACE_TABLE.to_string(),
        )])
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        for table in tables {
            if table.name != ETH_TRACE_TABLE || table.schema.is_some() {
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
        let mut result = Vec::new();
        for table in tables {
            if table.name != ETH_TRACE_TABLE || table.schema.is_some() {
                return Err(TableNotFound {
                    schema: table.schema.clone(),
                    name: table.name.clone(),
                }
                .into());
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
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        warn!("TODO: respect table_infos");
        Ok(vec![Ok(SourceSchema::new(
            helper::get_trace_schema(),
            CdcType::Nothing,
        ))])
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        _tables: Vec<TableInfo>,
        _last_checkpoint: Option<RestartableState>,
    ) -> Result<(), BoxedError> {
        let config = self.config.clone();
        let conn_name = self.conn_name.clone();
        run(ingestor, config, conn_name).await
    }
}

pub async fn validate(config: &EthTraceConfig) -> Result<(), BoxedError> {
    // Check if transport can be initialized
    let tuple = conn_helper::get_batch_http_client(&config.https_url).await?;

    // Check if debug API is available
    get_block_traces(tuple, (1000000, 1000005)).await?;

    Ok(())
}

pub async fn run(
    ingestor: &Ingestor,
    config: EthTraceConfig,
    conn_name: String,
) -> Result<(), BoxedError> {
    let client_tuple = conn_helper::get_batch_http_client(&config.https_url).await?;

    info!(
        "Starting Eth Trace connector: {} from block {}",
        conn_name, config.from_block
    );
    let batch_iter = BatchIterator::new(
        config.from_block,
        config.to_block,
        config.batch_size.unwrap_or_else(default_batch_size),
    );

    let mut errors: Vec<BoxedError> = vec![];
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
                            if ingestor
                                .handle_message(IngestionMessage::OperationEvent {
                                    table_index: 0, // We have only one table
                                    op,
                                    state: None,
                                })
                                .await
                                .is_err()
                            {
                                // If receiving end is closed, exit
                                return Ok(());
                            }
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
