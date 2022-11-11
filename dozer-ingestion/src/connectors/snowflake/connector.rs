use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::connectors::snowflake::connection::client::Client;
use crate::connectors::Connector;
use crate::ingestion::Ingestor;
use crate::{connectors::TableInfo, errors::ConnectorError};
use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::parking_lot::RwLock;

use crate::connectors::snowflake::snapshotter::Snapshotter;
use crate::connectors::snowflake::stream_consumer::StreamConsumer;

use tokio::runtime::Runtime;
use tokio::{task, time};

pub struct SnowflakeConnector {
    pub id: u64,
    config: SnowflakeConfig,
    ingestor: Option<Arc<RwLock<Ingestor>>>,
    tables: Option<Vec<TableInfo>>,
}

impl SnowflakeConnector {
    pub fn new(id: u64, config: SnowflakeConfig) -> Self {
        Self {
            id,
            config,
            ingestor: None,
            tables: None,
        }
    }
}

impl Connector for SnowflakeConnector {
    fn get_schemas(
        &self,
        _table_names: Option<Vec<String>>,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ConnectorError> {
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

    fn start(&self, _running: Arc<AtomicBool>) -> Result<(), ConnectorError> {
        let connector_id = self.id;
        let ingestor = self
            .ingestor
            .as_ref()
            .map_or(Err(ConnectorError::InitializationError), Ok)?
            .clone();

        Runtime::new().unwrap().block_on(async {
            run(
                self.config.clone(),
                self.tables.clone(),
                ingestor,
                connector_id,
            )
            .await
        })
    }

    fn stop(&self) {}
}

async fn run(
    config: SnowflakeConfig,
    tables: Option<Vec<TableInfo>>,
    ingestor: Arc<RwLock<Ingestor>>,
    connector_id: u64,
) -> Result<(), ConnectorError> {
    let client = Client::new(&config);

    match tables {
        None => {}
        Some(tables) => {
            for table in tables.iter() {
                let is_stream_created =
                    StreamConsumer::is_stream_created(&client, table.name.clone())?;
                if !is_stream_created {
                    let ingestor_snapshot = Arc::clone(&ingestor);
                    Snapshotter::run(
                        &client,
                        &ingestor_snapshot,
                        connector_id,
                        table.name.clone(),
                    )?;
                    StreamConsumer::create_stream(&client, &table.name)?;
                } else {
                    let stream_client = Client::new(&config);
                    let table_name = table.clone();
                    let ingestor_stream = Arc::clone(&ingestor);
                    let forever = task::spawn(async move {
                        let mut interval = time::interval(Duration::from_secs(5));

                        loop {
                            interval.tick().await;

                            StreamConsumer::consume_stream(
                                &stream_client,
                                connector_id,
                                &table_name.name,
                                &ingestor_stream,
                            )
                            .unwrap();
                        }
                    });

                    forever.await.unwrap();
                }
            }
        }
    };

    Ok(())
}
