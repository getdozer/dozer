use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::{
    IngestionMessage, TableConfig,
};
use dozer_ingestion_connector::dozer_types::types::FieldType;
use dozer_ingestion_connector::futures::future::join_all;
use dozer_ingestion_connector::tokio::sync::mpsc::channel;
use dozer_ingestion_connector::tokio::task::JoinSet;
use dozer_ingestion_connector::utils::ListOrFilterColumns;
use dozer_ingestion_connector::{
    async_trait, tokio, Connector, Ingestor, SourceSchemaResult, TableIdentifier, TableInfo,
    TableToIngest,
};

use crate::adapters::DozerObjectStore;
use crate::{schema_mapper, ObjectStoreConnectorError};

use super::connection::validator::validate_connection;
use super::csv::csv_table::CsvTable;
use super::delta::delta_table::DeltaTable;
use super::parquet::parquet_table::ParquetTable;
use super::table_watcher::TableWatcher;

#[derive(Debug)]
pub struct ObjectStoreConnector<T: Clone> {
    config: T,
}

impl<T: DozerObjectStore + 'static> ObjectStoreConnector<T> {
    pub fn new(config: T) -> Self {
        Self { config }
    }
}

#[async_trait]
impl<T: DozerObjectStore> Connector for ObjectStoreConnector<T> {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        validate_connection("object_store", None, self.config.clone()).map_err(Into::into)
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        Ok(self
            .config
            .tables()
            .iter()
            .map(|table| TableIdentifier::from_table_name(table.name.clone()))
            .collect())
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        validate_connection("object_store", Some(tables), self.config.clone()).map_err(Into::into)
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let schemas = get_schema_from_tables(&self.config, &tables).await;
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
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        let list_or_filter_columns = table_infos
            .iter()
            .map(|table_info| ListOrFilterColumns {
                schema: table_info.schema.clone(),
                name: table_info.name.clone(),
                columns: Some(table_info.column_names.clone()),
            })
            .collect::<Vec<_>>();
        Ok(schema_mapper::get_schema(&self.config, &list_or_filter_columns).await)
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), BoxedError> {
        let (sender, mut receiver) =
            channel::<Result<Option<IngestionMessage>, ObjectStoreConnectorError>>(100); // todo: increase buffer siz
        let ingestor_clone = ingestor.clone();

        // Ingestor loop - generating operation message out
        tokio::spawn(async move {
            loop {
                let message = receiver
                    .recv()
                    .await
                    .ok_or(ObjectStoreConnectorError::RecvError)??;
                match message {
                    None => {
                        break;
                    }
                    Some(evt) => ingestor_clone.handle_message(evt).await?,
                }
            }
            Ok::<_, BoxedError>(())
        });

        // sender sending out message for pipeline
        sender
            .send(Ok(Some(IngestionMessage::SnapshottingStarted)))
            .await
            .unwrap();

        let mut handles = vec![];
        // let mut csv_tables: HashMap<usize, HashMap<Path, DateTime<Utc>>> = vec![];

        for (table_index, table_info) in tables.iter().enumerate() {
            assert!(table_info.checkpoint.is_none());
            let table_info = TableInfo {
                schema: table_info.schema.clone(),
                name: table_info.name.clone(),
                column_names: table_info.column_names.clone(),
            };

            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    match &table_config.config {
                        TableConfig::CSV(config) => {
                            let table = CsvTable::new(config.clone(), self.config.clone());
                            handles.push(
                                table
                                    .snapshot(table_index, &table_info, sender.clone())
                                    .await
                                    .unwrap(),
                            );
                        }
                        TableConfig::Delta(config) => {
                            let table = DeltaTable::new(config.clone(), self.config.clone());
                            handles.push(
                                table
                                    .snapshot(table_index, &table_info, sender.clone())
                                    .await?,
                            );
                        }
                        TableConfig::Parquet(config) => {
                            let table = ParquetTable::new(config.clone(), self.config.clone());
                            handles.push(
                                table
                                    .snapshot(table_index, &table_info, sender.clone())
                                    .await?,
                            );
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
            .send(Ok(Some(IngestionMessage::SnapshottingDone)))
            .await
            .unwrap();

        let mut joinset = JoinSet::new();
        for (table_index, table_info) in tables.into_iter().enumerate() {
            let table_info = TableInfo {
                schema: table_info.schema,
                name: table_info.name,
                column_names: table_info.column_names,
            };

            for table in self.config.tables() {
                if table_info.name == table.name {
                    let config = self.config.clone();
                    let table_info = table_info.clone();
                    let sender = sender.clone();
                    match table.config.clone() {
                        TableConfig::CSV(csv_config) => {
                            let state = state_hash.get(&table_index).unwrap().clone();

                            joinset.spawn(async move {
                                let mut csv_table = CsvTable::new(csv_config, config);
                                csv_table.update_state = state;
                                csv_table.watch(table_index, &table_info, sender).await?;
                                Ok::<_, ObjectStoreConnectorError>(())
                            });
                        }
                        TableConfig::Delta(config) => {
                            let table = DeltaTable::new(config.clone(), self.config.clone());
                            joinset.spawn(async move {
                                table
                                    .watch(table_index, &table_info, sender.clone())
                                    .await?;
                                Ok::<_, ObjectStoreConnectorError>(())
                            });
                        }
                        TableConfig::Parquet(parquet_config) => {
                            let state = state_hash.get(&table_index).unwrap().clone();
                            joinset.spawn(async move {
                                let mut table = ParquetTable::new(parquet_config, config);
                                table.update_state = state;
                                table
                                    .watch(table_index, &table_info, sender.clone())
                                    .await?;
                                Ok::<_, ObjectStoreConnectorError>(())
                            });
                        }
                    }
                }
            }
        }
        while let Some(result) = joinset.join_next().await {
            // Unwrap to propagate a panic in a task, then return
            // short-circuit on any errors in connectors. The JoinSet
            // will abort all other tasks when it is dropped
            result.unwrap()?;
        }
        Ok(())
    }
}

async fn get_schema_from_tables(
    config: &impl DozerObjectStore,
    tables: &[TableIdentifier],
) -> Vec<SourceSchemaResult> {
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
