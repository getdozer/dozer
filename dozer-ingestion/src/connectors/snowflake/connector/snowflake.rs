use std::time::Duration;

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::{
    ConnectorMeta, ConnectorStart, SourceSchemaResult, TableIdentifier, TableInfo, TableToIngest,
};
use crate::errors::ConnectorError;
use crate::ingestion::Ingestor;
use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::node::OpIdentifier;
use tonic::async_trait;

use crate::connectors::snowflake::stream_consumer::StreamConsumer;

use dozer_types::log::{info, warn};

use crate::connectors::snowflake::schema_helper::SchemaHelper;

use tokio::time;

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

#[async_trait]
impl ConnectorMeta for SnowflakeConnector {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        SchemaHelper::get_schema(&self.config, None).map(|_| ())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        let schemas = SchemaHelper::get_schema(&self.config, None)?;
        let mut tables = vec![];
        for schema in schemas {
            tables.push(TableIdentifier::from_table_name(schema?.0));
        }
        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        let schemas = SchemaHelper::get_schema(&self.config, Some(&table_names))?;
        for schema in schemas {
            schema?;
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        let schemas = SchemaHelper::get_schema(&self.config, Some(&table_names))?;
        let mut result = vec![];
        for schema in schemas {
            let (name, schema) = schema?;
            let column_names = schema
                .schema
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect();
            result.push(TableInfo {
                schema: None,
                name,
                column_names,
            });
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
        warn!("TODO: respect `column_names` in `table_infos`");
        let table_names = table_infos
            .iter()
            .map(|table_info| table_info.name.clone())
            .collect::<Vec<_>>();
        Ok(SchemaHelper::get_schema(&self.config, Some(&table_names))?
            .into_iter()
            .map(|schema_result| schema_result.map(|(_, schema)| schema))
            .collect())
    }
}

#[async_trait(?Send)]
impl ConnectorStart for SnowflakeConnector {
    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), ConnectorError> {
        run(self.name.clone(), self.config.clone(), tables, ingestor).await
    }
}

async fn run(
    name: String,
    config: SnowflakeConfig,
    tables: Vec<TableToIngest>,
    ingestor: &Ingestor,
) -> Result<(), ConnectorError> {
    // SNAPSHOT part - run it when stream table doesn't exist
    let stream_client = Client::new(&config);
    let mut interval = time::interval(Duration::from_secs(5));

    let mut consumer = StreamConsumer::new();
    let mut iteration = 0;
    loop {
        for (idx, table) in tables.iter().enumerate() {
            // We only check stream status on first iteration
            if iteration == 0 {
                match table.checkpoint {
                    None | Some(OpIdentifier { txid: 0, .. }) => {
                        info!("[{}][{}] Creating new stream", name, table.name);
                        StreamConsumer::drop_stream(&stream_client, &table.name)?;
                        StreamConsumer::create_stream(&stream_client, &table.name)?;
                    }
                    Some(OpIdentifier { txid, seq_in_tx }) => {
                        info!(
                            "[{}][{}] Continuing ingestion from {}/{}",
                            name, table.name, txid, seq_in_tx
                        );
                        if let Ok(false) =
                            StreamConsumer::is_stream_created(&stream_client, &table.name)
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

            info!("[{}][{}] Reading from changes stream", name, table.name);

            consumer
                .consume_stream(&stream_client, &table.name, ingestor, idx, iteration)
                .await?;

            interval.tick().await;
        }

        iteration += 1;
    }
}
