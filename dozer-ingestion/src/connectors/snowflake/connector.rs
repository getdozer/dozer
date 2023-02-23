#[cfg(feature = "snowflake")]
use std::time::Duration;

#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::{Connector, ValidationResults};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::SnowflakeConfig;

#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::stream_consumer::StreamConsumer;

#[cfg(feature = "snowflake")]
use dozer_types::log::{debug, info};

use crate::connectors::snowflake::schema_helper::SchemaHelper;
use dozer_types::types::SourceSchema;
use tokio::runtime::Runtime;
#[cfg(feature = "snowflake")]
use tokio::time;

#[cfg(feature = "snowflake")]
use crate::errors::{SnowflakeError, SnowflakeStreamError};

#[derive(Debug)]
pub struct SnowflakeConnector {
    name: String,
    config: SnowflakeConfig,
}

impl SnowflakeConnector {
    pub fn new(name: String, config: SnowflakeConfig) -> Self {
        Self { name, config }
    }
}

impl Connector for SnowflakeConnector {
    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        SchemaHelper::validate_schemas(&self.config, tables)
    }

    #[cfg(feature = "snowflake")]
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        SchemaHelper::get_schema(&self.config, table_names)
    }

    #[cfg(not(feature = "snowflake"))]
    fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, ConnectorError> {
        todo!()
    }

    fn start(
        &self,
        from_seq: Option<(u64, u64)>,
        ingestor: &Ingestor,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        Runtime::new().unwrap().block_on(async {
            run(
                self.name.clone(),
                self.config.clone(),
                tables,
                ingestor,
                from_seq,
            )
            .await
        })
    }

    fn get_tables(&self, tables: Option<&[TableInfo]>) -> Result<Vec<TableInfo>, ConnectorError> {
        self.get_tables_default(tables)
    }
}

#[cfg(feature = "snowflake")]
async fn run(
    name: String,
    config: SnowflakeConfig,
    tables: Option<Vec<TableInfo>>,
    ingestor: &Ingestor,
    from_seq: Option<(u64, u64)>,
) -> Result<(), ConnectorError> {
    let client = Client::new(&config);

    // SNAPSHOT part - run it when stream table doesnt exist
    match tables {
        None => {}
        Some(tables) => {
            let stream_client = Client::new(&config);
            let mut interval = time::interval(Duration::from_secs(5));

            let mut consumer = StreamConsumer::new();
            let mut iteration = 0;
            loop {
                for (idx, table) in tables.iter().enumerate() {
                    // We only check stream status on first iteration
                    if iteration == 0 {
                        match from_seq {
                            None | Some((0, _)) => {
                                info!("[{}][{}] Creating new stream", name, table.table_name);
                                StreamConsumer::drop_stream(&client, &table.table_name)?;
                                StreamConsumer::create_stream(&client, &table.table_name)?;
                            }
                            Some((lsn, seq)) => {
                                info!(
                                    "[{}][{}] Continuing ingestion from {}/{}",
                                    name, table.table_name, lsn, seq
                                );
                                iteration = lsn;
                                if let Ok(false) =
                                    StreamConsumer::is_stream_created(&client, &table.table_name)
                                {
                                    return Err(ConnectorError::SnowflakeError(
                                        SnowflakeError::SnowflakeStreamError(
                                            SnowflakeStreamError::StreamNotFound,
                                        ),
                                    ));
                                }
                            }
                        }
                    }

                    debug!(
                        "[{}][{}] Reading from changes stream",
                        name, table.table_name
                    );

                    consumer.consume_stream(
                        &stream_client,
                        &table.table_name,
                        ingestor,
                        idx,
                        iteration,
                    )?;

                    interval.tick().await;
                }

                iteration += 1;
            }
        }
    };

    Ok(())
}

#[cfg(not(feature = "snowflake"))]
async fn run(
    _name: String,
    _config: SnowflakeConfig,
    _tables: Option<Vec<TableInfo>>,
    _ingestor: &Ingestor,
    _from_seq: Option<(u64, u64)>,
) -> Result<(), ConnectorError> {
    Ok(())
}
