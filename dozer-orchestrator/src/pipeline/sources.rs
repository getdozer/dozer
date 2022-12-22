use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Source, SourceFactory,
};
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema, SchemaIdentifier};
use log::info;

use std::collections::HashMap;

use std::sync::Arc;
use std::thread;

pub struct ConnectorSourceFactory {
    connection_map: HashMap<Connection, Vec<TableInfo>>,
    table_map: HashMap<String, u16>,
    schema_map: HashMap<u16, Schema>,
    port_map: HashMap<u32, u16>,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
}

impl ConnectorSourceFactory {
    pub fn new(
        connection_map: HashMap<Connection, Vec<TableInfo>>,
        table_map: HashMap<String, u16>,
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
    ) -> Self {
        let (schema_map, port_map) =
            Self::get_schema_map(&connection_map, &table_map).expect("Cannot initialize schemas");
        Self {
            connection_map,
            table_map,
            schema_map,
            port_map,
            ingestor,
            iterator,
        }
    }

    #[allow(clippy::type_complexity)]
    pub fn get_schema_map(
        connection_map: &HashMap<Connection, Vec<TableInfo>>,
        table_map: &HashMap<String, u16>,
    ) -> Result<(HashMap<u16, Schema>, HashMap<u32, u16>), ConnectorError> {
        let mut schema_map = HashMap::new();
        let mut port_map: HashMap<u32, u16> = HashMap::new();
        for (connection, tables) in connection_map.iter() {
            let connection = connection.clone();
            let connector = get_connector(connection.to_owned())?;
            let schema_tuples = connector.get_schemas(Some(tables.clone()))?;

            for (table_name, schema) in schema_tuples {
                let port = table_map
                    .get(&table_name)
                    .map_or(Err(ExecutionError::PortNotFound(table_name.clone())), Ok)
                    .unwrap();
                let schema_id = get_schema_id(schema.identifier.as_ref())?;
                schema_map.insert(port.to_owned(), schema);
                port_map.insert(schema_id, *port);
            }
        }

        Ok((schema_map, port_map))
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError> {
        let schema = self.schema_map.get(port).expect("Schema not found");
        Ok(schema.to_owned())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.table_map
            .values()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }

    fn build(
        &self,
        _schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(ConnectorSource {
            connection_map: self.connection_map.to_owned(),
            port_map: self.port_map.clone(),
            ingestor: self.ingestor.to_owned(),
            iterator: self.iterator.clone(),
        }))
    }
}

pub struct ConnectorSource {
    // Multiple tables per connection
    // Connection Index in the array to List of Tables
    connection_map: HashMap<Connection, Vec<TableInfo>>,
    port_map: HashMap<u32, u16>,
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let mut threads = vec![];
        for (connection, tables) in self.connection_map.clone().into_iter() {
            let connection = connection.clone();
            let ingestor = self.ingestor.clone();
            let t = thread::spawn(move || -> Result<(), ConnectorError> {
                let mut connector = get_connector(connection.to_owned())?;

                connector.initialize(ingestor, Some(tables))?;
                connector.start()?;
                Ok(())
            });
            threads.push(t);
        }

        loop {
            // Keep a reference of schema to table mapping
            let msg = self.iterator.write().next();
            if let Some(msg) = msg {
                if let (_, IngestionOperation::OperationEvent(op)) = msg {
                    let identifier = match &op.operation {
                        Operation::Delete { old } => old.schema_id.to_owned(),
                        Operation::Insert { new } => new.schema_id.to_owned(),
                        Operation::Update { old: _, new } => new.schema_id.to_owned(),
                    };
                    let schema_id = get_schema_id(identifier.as_ref())
                        .expect("schema_id not found in process_message");
                    let port = self
                        .port_map
                        .get(&schema_id)
                        .map_or(Err(ExecutionError::PortNotFound(schema_id.to_string())), Ok)
                        .unwrap();
                    info!("{:?}", op);
                    fw.send(op.seq_no, 0, op.operation.to_owned(), port.to_owned())?
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
}

fn get_schema_id(op_schema_id: Option<&SchemaIdentifier>) -> Result<u32, ConnectorError> {
    Ok(op_schema_id
        .map_or(Err(ConnectorError::SchemaIdentifierNotFound), Ok)?
        .id)
}
