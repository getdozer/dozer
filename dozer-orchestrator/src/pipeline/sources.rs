use crate::services::connection::ConnectionService;
use dozer_ingestion::connectors::TableInfo;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::core::channels::{ChannelManager, SourceChannelForwarder};
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::{Source, SourceFactory};
use dozer_types::core::state::{StateStore, StateStoreOptions};
use dozer_types::errors::execution::ExecutionError;
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::connection::Connection;
use dozer_types::types::Schema;
use log::debug;
use std::collections::HashMap;
use std::thread;

pub struct ConnectorSourceFactory {
    connections: Vec<Connection>,
    table_names: Vec<String>,
    port_map: HashMap<u16, Schema>,
    pub table_map: HashMap<String, u16>,
}

impl ConnectorSourceFactory {
    pub fn new(
        connections: Vec<Connection>,
        table_names: Vec<String>,
        source_schemas: Vec<Schema>,
    ) -> Self {
        let (port_map, table_map) = Self::_get_maps(&source_schemas, &table_names);
        Self {
            connections,
            port_map,
            table_map,
            table_names,
        }
    }

    fn _get_maps(
        source_schemas: &[Schema],
        table_names: &[String],
    ) -> (HashMap<u16, Schema>, HashMap<String, u16>) {
        let mut port_map: HashMap<u16, Schema> = HashMap::new();
        let mut table_map: HashMap<String, u16> = HashMap::new();
        let mut idx = 0;
        source_schemas.iter().for_each(|schema| {
            let id = schema.identifier.clone().unwrap().id;
            let table_name = &table_names[idx];
            port_map.insert(id as u16, schema.clone());
            table_map.insert(table_name.to_owned(), id as u16);
            idx += 1;
        });

        (port_map, table_map)
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        None
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        let keys = self.port_map.to_owned().into_keys().collect();
        debug!("{:?}", keys);
        keys
    }

    fn build(&self) -> Box<dyn Source> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            table_names: self.table_names.to_owned(),
            port_map: self.port_map.to_owned(),
            _table_map: self.table_map.to_owned(),
        })
    }
}

pub struct ConnectorSource {
    port_map: HashMap<u16, Schema>,
    _table_map: HashMap<String, u16>,
    connections: Vec<Connection>,
    table_names: Vec<String>,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        _cm: &dyn ChannelManager,
        _state: &mut dyn StateStore,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let mut threads = vec![];
        for connection in &self.connections {
            let table_names = self.table_names.clone();
            let connection = connection.clone();
            let ingestor = ingestor.clone();
            let t = thread::spawn(move || {
                let mut connector = ConnectionService::get_connector(connection);

                let tables = connector.get_tables().unwrap();
                let tables: Vec<TableInfo> = tables
                    .iter()
                    .filter(|t| {
                        let v = table_names.iter().find(|n| (*n).clone() == t.name.clone());
                        v.is_some()
                    })
                    .cloned()
                    .collect();

                connector.initialize(ingestor, Some(tables)).unwrap();
            });
            threads.push(t);
        }
        loop {
            let msg = iterator.next();

            if let Some(msg) = msg {
                let port = 0;
                match msg {
                    (_, IngestionOperation::OperationEvent(op)) => {
                        fw.send(op.seq_no, op.operation, port)?
                    }
                    (_, IngestionOperation::SchemaUpdate(schema)) => {
                        fw.update_schema(schema, port)?
                    }
                }
            } else {
                break;
            }
        }

        for t in threads {
            t.join().unwrap();
        }
        Ok(())
    }

    fn get_output_schema(&self, port: PortHandle) -> Schema {
        let val = self.port_map.get(&port).unwrap();
        val.to_owned()
    }
}
