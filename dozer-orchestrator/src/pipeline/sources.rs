use crate::models::source;
use crate::simple::SimpleOrchestrator;
use crate::Orchestrator;
use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{Source, SourceFactory};
use dozer_core::state::StateStore;
use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::storage::RocksConfig;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, OperationEvent, Record, Schema, SchemaIdentifier,
};

use super::super::models::{
    api_endpoint::ApiEndpoint as EndpointModel, source::Source as SourceModel,
};

pub struct OSourceFactory {
    id: i32,
    sources: Vec<SourceModel>,
    output_schemas: Vec<Schema>,
    output_ports: Vec<PortHandle>,
    storage_config: RocksConfig,
    pg_config: PostgresConfig,
}

impl OSourceFactory {
    pub fn new(
        id: i32,
        output_ports: Vec<PortHandle>,
        sources: Vec<SourceModel>,
        storage_config: RocksConfig,
        pg_config: PostgresConfig,
    ) -> Self {
        let output_schemas = Self::_get_schemas(&sources).unwrap();
        Self {
            id,
            output_ports,
            sources,
            output_schemas,
            storage_config,
            pg_config,
        }
    }

    fn _get_schemas(sources: &Vec<SourceModel>) -> anyhow::Result<Vec<Schema>> {
        let mut output_schemas: Vec<Schema> = vec![];
        for source in sources.iter() {
            let table_infos = SimpleOrchestrator::get_schema(source.connection.to_owned())?;
            let ti = table_infos.iter().find(|t| t.0 == source.dest_table_name);

            match ti {
                Some(ti) => {
                    let schema = ti.to_owned().1;
                    output_schemas.push(schema);
                }
                None => panic!("Schema not found for {}", source.dest_table_name),
            }
        }
        Ok(output_schemas)
    }
}

impl SourceFactory for OSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }

    fn get_output_schema(&self, port: PortHandle) -> anyhow::Result<Schema> {
        Ok(self.output_schemas[0].to_owned())
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(OSource {
            id: self.id,
            storage_config: self.storage_config.to_owned(),
            pg_config: self.pg_config.to_owned(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct OSource {
    id: i32,
    storage_config: RocksConfig,
    pg_config: PostgresConfig,
}

impl Source for OSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        state: &mut dyn StateStore,
        from_seq: Option<u64>,
    ) -> anyhow::Result<()> {
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
