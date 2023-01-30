#[cfg(feature = "snowflake")]
use odbc::create_environment_v3;
use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "snowflake")]
use std::time::Duration;

#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::{Connector, ValidationResults};
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::parking_lot::RwLock;

#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::snapshotter::Snapshotter;
#[cfg(feature = "snowflake")]
use crate::connectors::snowflake::stream_consumer::StreamConsumer;
#[cfg(feature = "snowflake")]
use crate::errors::SnowflakeError::ConnectionError;


use dozer_types::types::SchemaWithChangesType;
use tokio::runtime::Runtime;
#[cfg(feature = "snowflake")]
use tokio::time;

pub struct SnowflakeConnector {
    name: String,
    config: SnowflakeConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl SnowflakeConnector {
    pub fn new(name: String, config: SnowflakeConfig) -> Self {
        Self {
            name,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for SnowflakeConnector {
    #[cfg(feature = "snowflake")]
    fn get_schemas(
        &self,
        table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        let client = Client::new(&self.config);
        let env = create_environment_v3().map_err(|e| e.unwrap()).unwrap();
        let conn = env
            .connect_with_connection_string(&client.get_conn_string())
            .map_err(|e| ConnectionError(Box::new(e)))?;

        let keys = client
            .fetch_keys(&conn)
            .map_err(ConnectorError::SnowflakeError)?;

        let tables_indexes = table_names.clone().map_or(HashMap::new(), |tables| {
            let mut result = HashMap::new();
            for (idx, table) in tables.iter().enumerate() {
                result.insert(table.table_name.clone(), idx);
            }

            result
        });

        client
            .fetch_tables(table_names, tables_indexes, keys, &conn)
            .map_err(ConnectorError::SnowflakeError)
    }

    #[cfg(not(feature = "snowflake"))]
    fn get_schemas(
        &self,
        _table_names: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SchemaWithChangesType>, ConnectorError> {
        todo!()
    }

    fn get_tables(&self) -> Result<Vec<TableInfo>, ConnectorError> {
        todo!()
    }

    fn test_connection(&self) -> Result<(), ConnectorError> {
        todo!()
    }

    fn initialize(
        &mut self,
        ingestor: Arc<RwLock<Ingestor>>,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<(), ConnectorError> {
        self.ingestor = Some(ingestor);
        self.tables = tables;
        Ok(())
    }

    fn start(&self, from_seq: Option<(u64, u64)>) -> Result<(), ConnectorError> {
        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();

        Runtime::new().unwrap().block_on(async {
            run(
                self.name.clone(),
                self.config.clone(),
                self.tables.clone(),
                ingestor,
                from_seq,
            )
            .await
        })
    }

    fn stop(&self) {}

    fn validate(&self, _tables: Option<Vec<TableInfo>>) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn validate_schemas(&self, _tables: &[TableInfo]) -> Result<ValidationResults, ConnectorError> {
        Ok(HashMap::new())
    }
}

#[cfg(feature = "snowflake")]
async fn run(
    name: String,
    config: SnowflakeConfig,
    tables: Option<Vec<TableInfo>>,
    ingestor: Arc<RwLock<Ingestor>>,
    from_seq: Option<(u64, u64)>,
) -> Result<(), ConnectorError> {
    let client = Client::new(&config);

    // SNAPSHOT part - run it when stream table doesnt exist
    match tables {
        None => {}
        Some(tables) => {
            for (idx, table) in tables.iter().enumerate() {
                let is_stream_created =
                    StreamConsumer::is_stream_created(&client, table.table_name.clone())?;
                if !is_stream_created {
                    debug!(
                        "[{}][{}] Table stream not found, starting from snapshot",
                        name, table.table_name
                    );
                    let ingestor_snapshot = Arc::clone(&ingestor);
                    Snapshotter::run(
                        &client,
                        &ingestor_snapshot,
                        table.table_name.clone(),
                        idx,
                        from_seq.map_or(0, |(_, offset)| offset as usize),
                    )?;
                    debug!("[{}][{}] Snapshot fetch completed", name, table.table_name);
                    StreamConsumer::create_stream(&client, &table.table_name)?;

                    debug!(
                        "[{}][{}] Changes table stream creation completed",
                        name, table.table_name
                    );
                } else {
                    debug!(
                        "[{}][{}] Table stream exist, skipping snapshot",
                        name, table.table_name
                    );
                }
            }

            let stream_client = Client::new(&config);
            let ingestor_stream = Arc::clone(&ingestor);
            let mut interval = time::interval(Duration::from_secs(5));

            let mut consumer = StreamConsumer::new();
            loop {
                for (idx, table) in tables.iter().enumerate() {
                    debug!(
                        "[{}][{}] Reading from changes stream",
                        name, table.table_name
                    );
                    consumer.consume_stream(
                        &stream_client,
                        &table.table_name,
                        &ingestor_stream,
                        idx,
                    )?;

                    interval.tick().await;
                }
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
    _ingestor: Arc<RwLock<Ingestor>>,
    _from_seq: Option<(u64, u64)>,
) -> Result<(), ConnectorError> {
    Ok(())
}
