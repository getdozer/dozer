use dozer_ingestion_connector::dozer_types::errors::internal::BoxedError;
use dozer_ingestion_connector::dozer_types::models::ingestion_types::IngestionMessage;
use dozer_ingestion_connector::dozer_types::types::FieldType;
use dozer_ingestion_connector::futures::future::try_join_all;
use dozer_ingestion_connector::tokio::sync::mpsc::channel;
use dozer_ingestion_connector::tokio::task::JoinSet;
use dozer_ingestion_connector::utils::{ListOrFilterColumns, TableNotFound};
use dozer_ingestion_connector::{
    async_trait, tokio, Connector, Ingestor, SourceSchemaResult, TableIdentifier, TableInfo,
    TableToIngest,
};

use crate::adapters::DozerObjectStore;
use crate::table::ObjectStoreTable;
use crate::{schema_mapper, ObjectStoreConnectorError};

use super::connection::validator::validate_connection;

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

        for (table_index, table_info) in tables.iter().enumerate() {
            assert!(table_info.checkpoint.is_none());
            let table_info = TableInfo {
                schema: table_info.schema.clone(),
                name: table_info.name.clone(),
                column_names: table_info.column_names.clone(),
            };

            let mut found = false;
            for table_config in self.config.tables() {
                if table_info.name == table_config.name {
                    let table = ObjectStoreTable::new(
                        table_config.config.clone(),
                        self.config.clone(),
                        Default::default(),
                    );
                    let table_info = table_info.clone();
                    let sender = sender.clone();
                    handles.push(tokio::spawn(async move {
                        table
                            .snapshot(table_index, &table_info, sender)
                            .await
                            .unwrap()
                    }));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(TableNotFound {
                    schema: table_info.schema,
                    name: table_info.name,
                }
                .into());
            }
        }

        let updated_state = try_join_all(handles).await.unwrap();

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
                    let table = ObjectStoreTable::new(
                        table.config.clone(),
                        self.config.clone(),
                        updated_state[table_index].clone(),
                    );
                    let sender = sender.clone();
                    joinset
                        .spawn(async move { table.watch(table_index, &table_info, sender).await });
                    break;
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
