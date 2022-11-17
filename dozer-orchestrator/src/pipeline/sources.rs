use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{PortHandle, StatelessSource, StatelessSourceFactory};
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema, SchemaIdentifier};
use log::debug;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

pub struct ConnectorSourceFactory {
    connections: Vec<Connection>,
    connection_map: HashMap<String, Vec<String>>,
    table_map: HashMap<String, u16>,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
}

impl ConnectorSourceFactory {
    pub fn new(
        connections: Vec<Connection>,
        connection_map: HashMap<String, Vec<String>>,
        table_map: HashMap<String, u16>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> Self {
        Self {
            connections,
            connection_map,
            table_map,
            ingestor,
            iterator,
        }
    }
}

impl StatelessSourceFactory for ConnectorSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.table_map
            .values()
            .cloned()
            .collect::<Vec<PortHandle>>()
    }

    fn build(&self) -> Box<dyn StatelessSource> {
        Box::new(ConnectorSource {
            connections: self.connections.to_owned(),
            connection_map: self.connection_map.to_owned(),
            ingestor: self.ingestor.to_owned(),
            iterator: self.iterator.clone(),
            table_map: self.table_map.clone(),
        })
    }
}

pub struct ConnectorSource {
    // Multiple tables per connection
    connections: Vec<Connection>,
    // Connection Index in the array to List of Tables
    connection_map: HashMap<String, Vec<String>>,
    table_map: HashMap<String, u16>,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
}

impl StatelessSource for ConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        let mut threads = vec![];
        for (idx, connection) in self.connections.iter().cloned().enumerate() {
            let connection = connection.clone();
            let ingestor = self.ingestor.clone();
            let connection_map = self.connection_map.clone();
            let t = thread::spawn(move || -> Result<(), ConnectorError> {
                let mut connector = get_connector(connection.to_owned())?;

                let id = match connection.id {
                    Some(idy) => idy,
                    None => idx.to_string(),
                };
                let tables = connection_map
                    .get(&id)
                    .map_or(Err(ConnectorError::TableNotFound(idx.to_string())), Ok)?;

                // TODO: Let users choose columns
                let tables = tables
                    .iter()
                    .enumerate()
                    .map(|(y, t)| TableInfo {
                        name: t.to_owned(),
                        id: (idx + y) as u32,
                        columns: None,
                    })
                    .collect();

                connector.initialize(ingestor, Some(tables))?;
                connector.start()?;
                Ok(())
            });
            threads.push(t);
        }

        let mut schema_map: HashMap<u32, u16> = HashMap::new();
        loop {
            // Keep a reference of schema to table mapping
            let msg = self.iterator.write().next();
            if let Some(msg) = msg {
                match msg {
                    (_, IngestionOperation::OperationEvent(op)) => {
                        let identifier = match &op.operation {
                            Operation::Delete { old } => old.schema_id.to_owned(),
                            Operation::Insert { new } => new.schema_id.to_owned(),
                            Operation::Update { old: _, new } => new.schema_id.to_owned(),
                        };
                        let schema_id = get_schema_id(identifier.as_ref())?;
                        let port = schema_map
                            .get(&schema_id)
                            .map_or(Err(ExecutionError::PortNotFound(schema_id.to_string())), Ok)
                            .unwrap();
                        fw.send(op.seq_no, op.operation.to_owned(), port.to_owned())?
                    }
                    (_, IngestionOperation::SchemaUpdate(table_name, schema)) => {
                        let schema_id = get_schema_id(schema.identifier.as_ref())?;

                        let port = self
                            .table_map
                            .get(&table_name)
                            .map_or(Err(ExecutionError::PortNotFound(table_name.clone())), Ok)
                            .unwrap();
                        schema_map.insert(schema_id, port.to_owned());
                        fw.update_schema(schema.clone(), port.to_owned())?
                    }
                }
            } else {
                break;
            }
        }

        for t in threads {
            t.join()
                .unwrap()
                .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))?;
        }
        Ok(())
    }

    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        None
    }
}

fn get_schema_id(op_schema_id: Option<&SchemaIdentifier>) -> Result<u32, ExecutionError> {
    Ok(op_schema_id
        .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
        .id)
}
