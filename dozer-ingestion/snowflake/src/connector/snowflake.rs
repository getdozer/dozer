use dozer_ingestion_connector::{
    async_trait,
    dozer_types::{
        errors::internal::BoxedError,
        log::{info, warn},
        models::ingestion_types::{default_snowflake_poll_interval, SnowflakeConfig},
        node::OpIdentifier,
        types::FieldType,
    },
    tokio, Connector, Ingestor, SourceSchema, SourceSchemaResult, TableIdentifier, TableInfo,
    TableToIngest,
};
use odbc::create_environment_v3;

use crate::{
    connection::client::Client, schema_helper::SchemaHelper, stream_consumer::StreamConsumer,
    SnowflakeError, SnowflakeStreamError,
};

#[derive(Debug)]
pub struct SnowflakeConnector {
    name: String,
    config: SnowflakeConfig,
}

impl SnowflakeConnector {
    pub fn new(name: String, config: SnowflakeConfig) -> Self {
        Self { name, config }
    }

    async fn get_schemas_async(
        &self,
        table_names: Option<Vec<String>>,
    ) -> Result<Vec<Result<(String, SourceSchema), SnowflakeError>>, SnowflakeError> {
        let config = self.config.clone();
        spawn_blocking(move || SchemaHelper::get_schema(config, table_names.as_deref())).await
    }
}

#[async_trait]
impl Connector for SnowflakeConnector {
    fn types_mapping() -> Vec<(String, Option<FieldType>)>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn validate_connection(&self) -> Result<(), BoxedError> {
        self.get_schemas_async(None).await?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableIdentifier>, BoxedError> {
        let schemas = self.get_schemas_async(None).await?;
        let mut tables = vec![];
        for schema in schemas {
            tables.push(TableIdentifier::from_table_name(schema?.0));
        }
        Ok(tables)
    }

    async fn validate_tables(&self, tables: &[TableIdentifier]) -> Result<(), BoxedError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        let schemas = self.get_schemas_async(Some(table_names)).await?;
        for schema in schemas {
            schema?;
        }
        Ok(())
    }

    async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, BoxedError> {
        let table_names = tables
            .iter()
            .map(|table| table.name.clone())
            .collect::<Vec<_>>();
        let schemas = self.get_schemas_async(Some(table_names)).await?;
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
    ) -> Result<Vec<SourceSchemaResult>, BoxedError> {
        warn!("TODO: respect `column_names` in `table_infos`");
        let table_names = table_infos
            .iter()
            .map(|table_info| table_info.name.clone())
            .collect::<Vec<_>>();
        Ok(self
            .get_schemas_async(Some(table_names))
            .await?
            .into_iter()
            .map(|schema_result| schema_result.map(|(_, schema)| schema).map_err(Into::into))
            .collect())
    }

    async fn start(
        &self,
        ingestor: &Ingestor,
        tables: Vec<TableToIngest>,
    ) -> Result<(), BoxedError> {
        spawn_blocking({
            let name = self.name.clone();
            let config = self.config.clone();
            let ingestor = ingestor.clone();
            move || run(name, config, tables, ingestor)
        })
        .await
        .map_err(Into::into)
    }
}

fn run(
    name: String,
    config: SnowflakeConfig,
    tables: Vec<TableToIngest>,
    ingestor: Ingestor,
) -> Result<(), SnowflakeError> {
    // SNAPSHOT part - run it when stream table doesn't exist
    let env = create_environment_v3().unwrap();
    let interval = config
        .poll_interval_seconds
        .unwrap_or_else(default_snowflake_poll_interval);
    let stream_client = Client::new(config, &env);

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
                            return Err(SnowflakeError::SnowflakeStreamError(
                                SnowflakeStreamError::StreamNotFound,
                            ));
                        }
                    }
                }
            }

            info!("[{}][{}] Reading from changes stream", name, table.name);

            consumer.consume_stream(&stream_client, &table.name, &ingestor, idx, iteration)?;

            std::thread::sleep(interval);
        }

        iteration += 1;
    }
}

async fn spawn_blocking<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|join_err| {
            let msg = format!("{join_err}");
            if join_err.is_panic() {
                panic!("{msg}; panic: {:?}", join_err.into_panic())
            } else {
                panic!("{msg}")
            }
        })
}
