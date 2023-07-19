use dozer_types::ingestion_types::{IngestionMessage, IngestionMessageKind};
use futures::future::join_all;
use std::collections::HashMap;
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
        let (sender, mut receiver) =
            channel::<Result<Option<IngestionMessageKind>, ObjectStoreConnectorError>>(100); // todo: increase buffer size
        let ingestor_clone = ingestor.clone();

        // Ingestor loop - generating operation message out
        tokio::spawn(async move {
            let mut seq_no = 0;
            loop {
                let message = receiver
                    .recv()
                    .await
                    .ok_or(ConnectorError::ObjectStoreConnectorError(RecvError))
                    .unwrap()
                    .unwrap();
                match message {
                    None => {
                        break;
                    }
                    Some(evt) => {
                        match evt {
                            IngestionMessageKind::SnapshottingStarted => {
                                ingestor_clone
                                    .handle_message(IngestionMessage::new_snapshotting_started(
                                        0, seq_no,
                                    ))
                                    .map_err(ConnectorError::IngestorError)
                                    .unwrap();
                            }
                            IngestionMessageKind::SnapshotBatch(batch) => {
                                ingestor_clone
                                    .handle_message(IngestionMessage::new_snapshot_batch(
                                        0, seq_no, batch,
                                    ))
                                    .map_err(ConnectorError::IngestorError)
                                    .unwrap();
                            }
                            IngestionMessageKind::SnapshottingDone => {
                                ingestor_clone
                                    .handle_message(IngestionMessage::new_snapshotting_done(
                                        0, seq_no,
                                    ))
                                    .map_err(ConnectorError::IngestorError)
                                    .unwrap();
                            }
                            IngestionMessageKind::OperationEvent(op) => {
                                ingestor_clone
                                    .handle_message(IngestionMessage::new_op(0, seq_no, op))
                                    .map_err(ConnectorError::IngestorError)
                                    .unwrap();
                            }
                        }
                        seq_no += 1;
                    }
                }
            }
        });

        // sender sending out message for pipeline
        sender
            .send(Ok(Some(IngestionMessageKind::SnapshottingStarted)))
            .await
            .unwrap();

        let mut handles = vec![];
        // let mut csv_tables: HashMap<usize, HashMap<Path, DateTime<Utc>>> = vec![];

        for (id, table_info) in tables.iter().enumerate() {
            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    if let Some(config) = &table_config.config {
                        match config {
                            dozer_types::ingestion_types::TableConfig::CSV(config) => {
                                let table = CsvTable::new(config.clone(), self.config.clone());
                                handles.push(
                                    table
                                        .snapshot(id, table_info, sender.clone())
                                        .await
                                        .unwrap(),
                                );
                            }
                            dozer_types::ingestion_types::TableConfig::Delta(config) => {
                                let table =
                                    DeltaTable::new(id, config.clone(), self.config.clone());
                                handles.push(table.snapshot(id, table_info, sender.clone()).await?);
                            }
                            dozer_types::ingestion_types::TableConfig::Parquet(config) => {
                                let table = ParquetTable::new(config.clone(), self.config.clone());
                                handles.push(table.snapshot(id, table_info, sender.clone()).await?);
                            }
                        }
                    }
                }
            }
        }

        let updated_state = join_all(handles).await;
        let mut state_hash = HashMap::new();
        for (id, state) in updated_state.into_iter().flatten() {
            state_hash.insert(id, state);
        }

        sender
            .send(Ok(Some(IngestionMessageKind::SnapshottingDone)))
            .await
            .unwrap();

        for (id, table_info) in tables.iter().enumerate() {
            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    if let Some(config) = &table_config.config {
                        match config {
                            dozer_types::ingestion_types::TableConfig::CSV(config) => {
                                let mut table = CsvTable::new(config.clone(), self.config.clone());
                                table.update_state = state_hash.get(&id).unwrap().clone();
                                table.watch(id, table_info, sender.clone()).await.unwrap();
                            }
                            dozer_types::ingestion_types::TableConfig::Delta(config) => {
                                let table =
                                    DeltaTable::new(id, config.clone(), self.config.clone());
                                table.watch(id, table_info, sender.clone()).await?;
                            }
                            dozer_types::ingestion_types::TableConfig::Parquet(config) => {
                                let mut table =
                                    ParquetTable::new(config.clone(), self.config.clone());
                                table.update_state = state_hash.get(&id).unwrap().clone();
                                table.watch(id, table_info, sender.clone()).await?;
                            }
                        }
                    }
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
