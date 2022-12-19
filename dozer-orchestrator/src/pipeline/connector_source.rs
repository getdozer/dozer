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
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

pub struct NewConnectorSourceFactory {
    pub ingestor: Arc<RwLock<Ingestor>>,
    pub iterator: Arc<RwLock<IngestionIterator>>,
    pub ports: HashMap<String, u16>,
    pub tables: Vec<TableInfo>,
    pub connection: Connection,
}

// TODO: Move this to sources.rs when everything is connected proeprly

impl SourceFactory for NewConnectorSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError> {
        todo!()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.ports
            .values()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }

    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(NewConnectorSource {
            ingestor: self.ingestor.clone(),
            iterator: self.iterator.clone(),
            ports: self.ports.clone(),
            tables: self.tables.clone(),
            connection: self.connection.clone(),
        }))
    }
}

pub struct NewConnectorSource {
    ingestor: Arc<RwLock<Ingestor>>,
    iterator: Arc<RwLock<IngestionIterator>>,
    ports: HashMap<String, u16>,
    tables: Vec<TableInfo>,
    connection: Connection,
}

impl Source for NewConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<(u64, u64)>,
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

        let mut schema_map: HashMap<u32, u16> = HashMap::new();
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
                        let port = schema_map
                            .get(&schema_id)
                            .map_or(Err(ExecutionError::PortNotFound(schema_id.to_string())), Ok)
                            .unwrap();
                        fw.send(op.seq_no, op.operation.to_owned(), port.to_owned())?
                    }
                    (_, IngestionOperation::SchemaUpdate(table_name, schema)) => {
                        let schema_id = get_schema_id(schema.identifier.as_ref())?;

                        let port = self
                            .ports
                            .get(&table_name)
                            .map_or(Err(ExecutionError::PortNotFound(table_name.clone())), Ok)
                            .unwrap();
                        schema_map.insert(schema_id, port.to_owned());
                        //  fw.update_schema(schema.clone(), port.to_owned())?
                    }
                    _ => {}
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
