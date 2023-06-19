use dozer_types::ingestion_types::IngestionMessage;
use tokio::sync::mpsc::channel;
use tonic::async_trait;

use crate::connectors::object_store::adapters::DozerObjectStore;
use crate::connectors::object_store::schema_mapper;
use crate::connectors::{
    Connector, ListOrFilterColumns, SourceSchemaResult, TableIdentifier, TableInfo,
};
use crate::errors::{ConnectorError, ObjectStoreConnectorError};
use crate::ingestion::Ingestor;

use super::connection::validator::validate_connection;
use super::csv::csv_table::CsvTable;
use super::delta::delta_table::DeltaTable;
use super::parquet::parquet_table::ParquetTable;
use super::table_watcher::TableWatcher;

use crate::errors::ObjectStoreConnectorError::RecvError;

type ConnectorResult<T> = Result<T, ConnectorError>;

#[derive(Debug)]
pub struct ObjectStoreConnector<T: Clone> {
    pub id: u64,
    config: T,
}

impl<T: DozerObjectStore> ObjectStoreConnector<T> {
    pub fn new(id: u64, config: T) -> Self {
        Self { id, config }
    }
}

#[async_trait]
impl<T: DozerObjectStore> Connector for ObjectStoreConnector<T> {
    fn types_mapping() -> Vec<(String, Option<dozer_types::types::FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), ConnectorError> {
        validate_connection("object_store", None, self.config.clone())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, ConnectorError> {
        Ok(self
            .config
            .tables()
            .iter()
            .map(|table| TableIdentifier::from_table_name(table.name.clone()))
            .collect())
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), ConnectorError> {
        validate_connection("object_store", Some(tables), self.config.clone())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let schemas = get_schema_from_tables(&self.config, &tables).await?;
        let mut result = vec![];
        for (table, schema) in tables.into_iter().zip(schemas) {
            let schema = schema?;
            let column_names = schema
                .schema
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect();
            let table_info = TableInfo {
                schema: table.schema,
                name: table.name,
                column_names,
            };
            result.push(table_info);
        }
        Ok(result)
    }

    async fn get_schemas(
        &self,
        table_infos: &[TableInfo],
    ) -> ConnectorResult<Vec<SourceSchemaResult>> {
        let list_or_filter_columns = table_infos
            .iter()
            .map(|table_info| ListOrFilterColumns {
                schema: table_info.schema.clone(),
                name: table_info.name.clone(),
                columns: Some(table_info.column_names.clone()),
            })
            .collect::<Vec<_>>();
        schema_mapper::get_schema(&self.config, &list_or_filter_columns).await
    }

    async fn start(&self, ingestor: &Ingestor, tables: Vec<TableInfo>) -> ConnectorResult<()> {
        let (sender, mut receiver) = channel(16);

        ingestor
            .handle_message(IngestionMessage::new_snapshotting_started(0_u64, 0))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        for (id, table_info) in tables.iter().enumerate() {
            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    if let Some(config) = &table_config.config {
                        match config {
                            dozer_types::ingestion_types::TableConfig::CSV(config) => {
                                let table = CsvTable::new(config.clone(), self.config.clone());
                                table
                                    .snapshot(id, table_info, sender.clone())
                                    .await
                                    .unwrap();
                            }
                            dozer_types::ingestion_types::TableConfig::Delta(config) => {
                                let table =
                                    DeltaTable::new(id, config.clone(), self.config.clone());
                                table.snapshot(id, table_info, sender.clone()).await?;
                            }
                            dozer_types::ingestion_types::TableConfig::Parquet(config) => {
                                let table = ParquetTable::new(config.clone(), self.config.clone());
                                table.snapshot(id, table_info, sender.clone()).await?;
                            }
                        }
                    }
                }
            }
        }

        ingestor
            .handle_message(IngestionMessage::new_snapshotting_done(0_u64, 1))
            .map_err(ObjectStoreConnectorError::IngestorError)?;

        for (id, table_info) in tables.iter().enumerate() {
            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    if let Some(config) = &table_config.config {
                        match config {
                            dozer_types::ingestion_types::TableConfig::CSV(config) => {
                                let table = CsvTable::new(config.clone(), self.config.clone());
                                table.watch(id, table_info, sender.clone()).await.unwrap();
                            }
                            dozer_types::ingestion_types::TableConfig::Delta(config) => {
                                let table =
                                    DeltaTable::new(id, config.clone(), self.config.clone());
                                table.watch(id, table_info, sender.clone()).await?;
                            }
                            dozer_types::ingestion_types::TableConfig::Parquet(config) => {
                                let table = ParquetTable::new(config.clone(), self.config.clone());
                                table.watch(id, table_info, sender.clone()).await?;
                            }
                        }
                    }
                }
            }
        }

        let mut seq_no = 2;
        loop {
            let message = receiver
                .recv()
                .await
                .ok_or(ConnectorError::ObjectStoreConnectorError(RecvError))??;
            match message {
                None => {
                    break;
                }
                Some(evt) => {
                    ingestor
                        .handle_message(IngestionMessage::new_op(0, seq_no, evt))
                        .map_err(ConnectorError::IngestorError)?;
                    seq_no += 1;
                }
            }
        }
        Ok(())
    }
}

async fn get_schema_from_tables(
    config: &impl DozerObjectStore,
    tables: &[TableIdentifier],
) -> Result<Vec<SourceSchemaResult>, ConnectorError> {
    let table_infos = tables
        .iter()
        .map(|table| ListOrFilterColumns {
            schema: table.schema.clone(),
            name: table.name.clone(),
            columns: None,
        })
        .collect::<Vec<_>>();
    schema_mapper::get_schema(config, &table_infos).await
}
