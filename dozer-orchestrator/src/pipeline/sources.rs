use crate::services::connection::ConnectionService;
use dozer_core::dag::channels::{ChannelManager, SourceChannelForwarder};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{PortHandle, Source, SourceFactory};
use dozer_core::storage::lmdb_sys::Transaction;
use dozer_ingestion::connectors::TableInfo;
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::connection::Connection;
use dozer_types::types::{Operation, Schema, SchemaIdentifier};
use log::debug;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

pub struct ConnectorSourceFactory {
    connections: Vec<Connection>,
    table_names: Vec<String>,
    port_map: HashMap<u16, Schema>,
    pub table_map: HashMap<String, u16>,
    running: Arc<AtomicBool>,
}

impl ConnectorSourceFactory {
    pub fn new(
        connections: Vec<Connection>,
        table_names: Vec<String>,
        source_schemas: Vec<Schema>,
        running: Arc<AtomicBool>,
    ) -> Self {
        let (port_map, table_map) = Self::_get_maps(&source_schemas, &table_names);
        Self {
            connections,
            port_map,
            table_map,
            table_names,
            running,
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
    fn is_stateful(&self) -> bool {
        false
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.port_map.to_owned().into_keys().collect()
    }

    fn build(&self) -> Box<dyn Source> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            table_names: self.table_names.to_owned(),
            port_map: self.port_map.to_owned(),
            _table_map: self.table_map.to_owned(),
            running: self.running.to_owned(),
        })
    }
}

pub struct ConnectorSource {
    port_map: HashMap<u16, Schema>,
    _table_map: HashMap<String, u16>,
    connections: Vec<Connection>,
    table_names: Vec<String>,
    running: Arc<AtomicBool>,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &dyn SourceChannelForwarder,
        cm: &dyn ChannelManager,
        _state: Option<&mut Transaction>,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let mut threads = vec![];
        for connection in &self.connections {
            let table_names = self.table_names.clone();
            let connection = connection.clone();
            let ingestor = ingestor.clone();
            let t = thread::spawn(move || -> Result<(), ConnectorError> {
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

                connector.initialize(ingestor, Some(tables))?;
                connector.start()?;
                Ok(())
            });
            threads.push(t);
        }
        loop {
            // shutdown signal
            if !self.running.load(Ordering::SeqCst) {
                debug!("Exiting Executor on Ctrl-C");
                cm.terminate().unwrap();
                return Ok(());
            }
            let msg = iterator.next();
            if let Some(msg) = msg {
                match msg {
                    (_, IngestionOperation::OperationEvent(op)) => {
                        let identifier = match &op.operation {
                            Operation::Delete { old } => old.schema_id.to_owned(),
                            Operation::Insert { new } => new.schema_id.to_owned(),
                            Operation::Update { old: _, new } => new.schema_id.to_owned(),
                        };
                        let schema_id = get_schema_id(identifier.as_ref())?;
                        fw.send(op.seq_no, op.operation.to_owned(), schema_id as u16)?
                    }
                    (_, IngestionOperation::SchemaUpdate(schema)) => {
                        let schema_id = get_schema_id(schema.identifier.as_ref())?;
                        fw.update_schema(schema, schema_id as u16)?
                    }
                }
            } else {
                break;
            }
        }

        for t in threads {
            t.join()
                .unwrap()
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }
        Ok(())
    }

    fn get_output_schema(&self, port: PortHandle) -> Schema {
        let val = self.port_map.get(&port).unwrap();
        val.to_owned()
    }
}

fn get_schema_id(op_schema_id: Option<&SchemaIdentifier>) -> Result<u32, ExecutionError> {
    Ok(op_schema_id
        .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
        .id)
}
