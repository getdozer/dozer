use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use dozer_core::dag::dag::PortHandle;
use dozer_core::dag::forwarder::{ChannelManager, SourceChannelForwarder};
use dozer_core::dag::mt_executor::DefaultPortHandle;
use dozer_core::dag::node::{Source, SourceFactory};
use dozer_core::state::StateStore;
use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres::connector::{PostgresConfig, PostgresConnector};
use dozer_ingestion::connectors::storage::{RocksConfig, Storage};
use dozer_schema::registry::SchemaRegistryClient;
use dozer_types::types::{Field, Operation, OperationEvent, Record, Schema, SchemaIdentifier};

use crate::get_schema;
use crate::models::connection::{Connection, DBType};

pub struct ConnectorSourceFactory {
    connections: Vec<Connection>,
    table_names: Vec<String>,
    port_map: HashMap<u16, Schema>,
    pub table_map: HashMap<String, u16>,
}

impl ConnectorSourceFactory {
    pub fn new(connections: Vec<Connection>, table_names: Vec<String>) -> Self {
        let (port_map, table_map) = Self::_get_schemas(&connections, &table_names).unwrap();
        Self {
            connections,
            port_map,
            table_map,
            table_names,
        }
    }

    fn _get_schemas(
        connections: &Vec<Connection>,
        table_names: &Vec<String>,
    ) -> anyhow::Result<(HashMap<u16, Schema>, HashMap<String, u16>)> {
        let mut port_handles: Vec<u16> = vec![];

        let mut port_map: HashMap<u16, Schema> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();
        // Generate schema_ids and port_handles for the time being
        // TODO: this should be moved into a block where schema evolution is taken care of.

        connections.iter().enumerate().for_each(|(x, connection)| {
            let schema_tuples = get_schema(connection.to_owned()).unwrap();
            schema_tuples
                .iter()
                .filter(|t| table_names.iter().find(|n| *n.to_owned() == t.0).is_some())
                .enumerate()
                .for_each(|(y, (s, schema))| {
                    let mut schema = schema.clone();
                    let id = x * 1000 + y + 1;
                    port_handles.push(id as u16);
                    schema.identifier = Some(SchemaIdentifier {
                        id: id as u32,
                        version: 1,
                    });
                    port_map.insert(id as u16, schema);
                    table_map.insert(s.to_owned(), id as u16);
                });
        });
        Ok((port_map, table_map))
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        let keys = self.port_map.to_owned().into_keys().collect();
        println!("{:?}", keys);
        keys
    }

    fn get_output_schema(&self, port: PortHandle) -> anyhow::Result<Schema> {
        let val = self.port_map.get(&port).unwrap();
        Ok(val.to_owned())
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            table_names: self.table_names.to_owned(),
            port_map: self.port_map.to_owned(),
            table_map: self.table_map.to_owned(),
        })
    }
}

pub struct ConnectorSource {
    port_map: HashMap<u16, Schema>,
    table_map: HashMap<String, u16>,
    connections: Vec<Connection>,
    table_names: Vec<String>,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        state: &mut dyn StateStore,
        from_seq: Option<u64>,
    ) -> anyhow::Result<()> {
        let connection = &self.connections[0];
        let mut connector = _get_connector(connection.to_owned());

        let mut iterator = connector.iterator();
        loop {
            let msg = iterator.next().unwrap();
            let schema_id = match msg.operation.clone() {
                Operation::Delete { old } => old.schema_id,
                Operation::Insert { new } => new.schema_id,
                Operation::Update { old: _, new } => new.schema_id,
                Operation::Terminate => panic!("this shouldnt be here"),
            }
                .unwrap();
            // println!("msg: {:?}", msg);
            fw.send(msg, schema_id.id as u16).unwrap();
        }

        // cm.terminate().unwrap();
        Ok(())
    }
}

fn _get_connector(connection: Connection) -> PostgresConnector {
    match connection.db_type {
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

            connector.initialize(storage_client).unwrap();

            connector.drop_replication_slot_if_exists();
            connector
            // Box::new(connector)
        }
        DBType::Databricks => {
            panic!("Databricks not implemented");
        }
        DBType::Snowflake => {
            panic!("Snowflake not implemented");
        }
    }
}
