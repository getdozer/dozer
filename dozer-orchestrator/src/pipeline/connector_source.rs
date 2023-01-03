use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Source, SourceFactory,
};
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionIterator, Ingestor};
use dozer_types::ingestion_types::IngestionOperation;
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema, SchemaIdentifier};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

#[derive(Debug)]
pub struct ConnectorSourceFactory {
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub iterator: Arc<RwLock<IngestionIterator>>,
    pub ports: HashMap<String, u16>,
    pub schema_port_map: HashMap<u32, u16>,
    pub schema_map: HashMap<u16, Schema>,
    pub tables: Vec<TableInfo>,
    pub connection: Connection,
}

// TODO: Move this to sources.rs when everything is connected proeprly

impl ConnectorSourceFactory {
    pub fn new(
        ingestor: Arc<RwLock<Ingestor>>,
        iterator: Arc<RwLock<IngestionIterator>>,
        ports: HashMap<String, u16>,
        tables: Vec<TableInfo>,
        connection: Connection,
    ) -> Self {
        let (schema_map, schema_port_map) =
            Self::get_schema_map(connection.clone(), tables.clone(), ports.clone());
        Self {
            ingestor,
            iterator,
            ports,
            schema_port_map,
            schema_map,
            tables,
            connection,
        }
    }

    fn get_schema_map(
        connection: Connection,
        tables: Vec<TableInfo>,
        ports: HashMap<String, u16>,
    ) -> (HashMap<u16, Schema>, HashMap<u32, u16>) {
        let connector = get_connector(connection).unwrap();
        let schema_tuples = connector.get_schemas(Some(tables)).unwrap();

        let mut schema_map = HashMap::new();
        let mut schema_port_map: HashMap<u32, u16> = HashMap::new();

        for (table_name, schema) in schema_tuples {
            let port: u16 = *ports
                .get(&table_name)
                .map_or(Err(ExecutionError::PortNotFound(table_name.clone())), Ok)
                .unwrap();
            let schema_id = get_schema_id(schema.identifier.as_ref()).unwrap();

            schema_port_map.insert(schema_id, port);
            schema_map.insert(port, schema);
        }

        (schema_map, schema_port_map)
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError> {
        self.schema_map.get(port).map_or(
            Err(ExecutionError::PortNotFoundInSource(*port)),
            |schema_name| Ok(schema_name.clone()),
        )
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.ports
            .values()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }

    fn prepare(&self, output_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        for (port, schema) in output_schemas {
            let (name, _) = self
                .ports
                .iter()
                .find(|(_, p)| **p == port)
                .map_or(Err(ExecutionError::PortNotFound(port.to_string())), Ok)?;
            info!("SINK: Initializing input schema: {}", name);
            schema.print().printstd();
        }
        Ok(())
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(ConnectorSource {
            ingestor: self.ingestor.clone(),
            iterator: self.iterator.clone(),
            schema_port_map: self.schema_port_map.clone(),
            tables: self.tables.clone(),
            connection: self.connection.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct ConnectorSource {
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    schema_port_map: HashMap<u32, u16>,
    tables: Vec<TableInfo>,
    connection: Connection,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let mut connector = get_connector(self.connection.to_owned())
            .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))?;

        let ingestor = self.ingestor.clone();
        let tables = self.tables.clone();
        let t = thread::spawn(move || -> Result<(), ConnectorError> {
            connector.initialize(ingestor, Some(tables))?;
            connector.start()?;
            Ok(())
        });

        loop {
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
                        let port = self
                            .schema_port_map
                            .get(&schema_id)
                            .map_or(Err(ExecutionError::PortNotFound(schema_id.to_string())), Ok)?;
                        fw.send(op.seq_no, 0, op.operation.to_owned(), port.to_owned())?
                    }
                }
            } else {
                break;
            }
        }

        t.join()
            .unwrap()
            .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))?;

        Ok(())
    }
}

fn get_schema_id(op_schema_id: Option<&SchemaIdentifier>) -> Result<u32, ExecutionError> {
    Ok(op_schema_id
        .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
        .id)
}
