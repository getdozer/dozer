use std::sync::Arc;
use std::thread;

use crate::get_schema;

use super::super::models::source::Source as SourceModel;
use crate::models::connection::DBType;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{Source, SourceFactory};
use dozer_core::state::StateStore;
use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::storage::{RocksConfig, Storage};
use dozer_schema::registry::{SchemaRegistryClient, _serve};
use dozer_types::types::{Field, Operation, OperationEvent, Record, Schema};
use tokio::runtime::Runtime;

pub struct IngestionSourceFactory {
    id: i32,
    schema_client: Arc<SchemaRegistryClient>,
    sources: Vec<SourceModel>,
    output_schemas: Vec<Schema>,
    output_ports: Vec<PortHandle>,
}

impl IngestionSourceFactory {
    pub fn new(
        id: i32,
        output_ports: Vec<PortHandle>,
        sources: Vec<SourceModel>,
        schema_client: Arc<SchemaRegistryClient>,
    ) -> Self {
        let output_schemas = Self::_get_schemas(&sources).unwrap();
        Self {
            id,
            output_ports,
            sources,
            output_schemas,
            schema_client,
        }
    }

    fn _get_schemas(sources: &Vec<SourceModel>) -> anyhow::Result<Vec<Schema>> {
        let mut output_schemas: Vec<Schema> = vec![];
        for source in sources.iter() {
            let schema_tuples = get_schema(source.connection.to_owned())?;
            let st = schema_tuples.iter().find(|t| t.0 == source.table_name);

            match st {
                Some(st) => {
                    let schema = st.to_owned().1;
                    output_schemas.push(schema);
                }
                None => panic!("Schema not found for {}", source.table_name),
            }
        }
        Ok(output_schemas)
    }
}

impl SourceFactory for IngestionSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn get_output_schema(&self, port: PortHandle) -> anyhow::Result<Schema> {
        Ok(self.output_schemas[0].to_owned())
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(IngestionSource {
            id: self.id,
            schema_client: self.schema_client.clone(),
            sources: self.sources.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct IngestionSource {
    id: i32,
    schema_client: Arc<SchemaRegistryClient>,
    sources: Vec<SourceModel>,
}

impl Source for IngestionSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        state: &mut dyn StateStore,
        from_seq: Option<u64>,
    ) -> anyhow::Result<()> {
        let connectors = _get_connectors(self.schema_client, self.sources);
        for n in 0..10_000_000 {
            fw.send(
                OperationEvent::new(
                    n,
                    Operation::Insert {
                        new: Record::new(
                            None,
                            vec![
                                Field::Int(0),
                                Field::String("Italy".to_string()),
                                Field::Int(2000),
                            ],
                        ),
                    },
                ),
                DefaultPortHandle,
            )
            .unwrap();
        }
        cm.terminate().unwrap();
        Ok(())
    }
}

fn _run_schema_registry() -> std::thread::JoinHandle<()> {
    thread::spawn(|| {
        Runtime::new().unwrap().block_on(async move {
            _serve(None).await;
        });
    })
}

fn _get_connectors(
    schema_client: Arc<SchemaRegistryClient>,
    sources: Vec<SourceModel>,
) -> Vec<Box<dyn Connector>> {
    let connectors: Vec<Box<dyn Connector>> = sources
        .iter()
        .map(|s| match s.connection.db_type {
            DBType::Postgres => {
                let storage_config = RocksConfig::default();
                let storage_client = Arc::new(Storage::new(storage_config));
                let postgres_config = PostgresConfig {
                    name: "test_c".to_string(),
                    // tables: Some(vec!["actor".to_string()]),
                    tables: None,
                    conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=pagila".to_string(),
                    // conn_str: "host=127.0.0.1 port=5432 user=postgres dbname=large_film".to_string(),
                };
                let mut connector = PostgresConnector::new(postgres_config);

                connector.initialize(storage_client, schema_client).unwrap();

                connector.drop_replication_slot_if_exists();
                Box::new(connector) as Box<dyn Connector>
            }
            DBType::Databricks => {
                panic!("Databricks not implemented");
            }
            DBType::Snowflake => {
                panic!("Snowflake not implemented");
            }
        })
        .collect();
    connectors
}
