use dozer_core::channels::SourceChannelForwarder;
use dozer_core::errors::ExecutionError::ReplicationTypeNotFound;
use dozer_core::errors::{ExecutionError, SourceError};
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_ingestion::connectors::{get_connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::ingestion_types::IngestorError;
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::Mutex;
use dozer_types::types::{
    Operation, ReplicationChangesTrackingType, Schema, SchemaIdentifier, SourceDefinition,
};
use std::collections::HashMap;
use std::thread;

#[derive(Debug)]
pub struct ConnectorSourceFactory {
    pub ports: HashMap<String, u16>,
    pub schema_port_map: HashMap<u32, u16>,
    pub schema_map: HashMap<u16, Schema>,
    pub replication_changes_type_map: HashMap<u16, ReplicationChangesTrackingType>,
    pub tables: Vec<TableInfo>,
    pub connection: Connection,
}

fn map_replication_type_to_output_port_type(
    typ: &ReplicationChangesTrackingType,
) -> OutputPortType {
    match typ {
        ReplicationChangesTrackingType::FullChanges => {
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_deletes: false,
                retr_old_records_for_updates: false,
            }
        }
        ReplicationChangesTrackingType::OnlyPK => OutputPortType::StatefulWithPrimaryKeyLookup {
            retr_old_records_for_deletes: true,
            retr_old_records_for_updates: true,
        },
        ReplicationChangesTrackingType::Nothing => OutputPortType::AutogenRowKeyLookup,
    }
}

impl ConnectorSourceFactory {
    pub fn new(
        ports: HashMap<String, u16>,
        tables: Vec<TableInfo>,
        connection: Connection,
    ) -> Self {
        let (schema_map, schema_port_map, replication_changes_type_map) =
            Self::get_schema_map(connection.clone(), tables.clone(), ports.clone());
        Self {
            ports,
            schema_port_map,
            schema_map,
            replication_changes_type_map,
            tables,
            connection,
        }
    }

    fn get_schema_map(
        connection: Connection,
        tables: Vec<TableInfo>,
        ports: HashMap<String, u16>,
    ) -> (
        HashMap<u16, Schema>,
        HashMap<u32, u16>,
        HashMap<u16, ReplicationChangesTrackingType>,
    ) {
        let mut tables_map = HashMap::new();
        for t in &tables {
            tables_map.insert(t.table_name.clone(), t.name.clone());
        }

        let connector = get_connector(connection).unwrap();
        let schema_tuples = connector.get_schemas(Some(tables)).unwrap();

        let mut schema_map = HashMap::new();
        let mut schema_port_map: HashMap<u32, u16> = HashMap::new();
        let mut replication_changes_type_map: HashMap<u16, ReplicationChangesTrackingType> =
            HashMap::new();

        for (table_name, schema, replication_changes_type) in schema_tuples {
            let source_name = tables_map.get(&table_name).unwrap();
            let port: u16 = *ports
                .get(source_name)
                .map_or(Err(ExecutionError::PortNotFound(table_name.clone())), Ok)
                .unwrap();
            let schema_id = get_schema_id(schema.identifier.as_ref()).unwrap();

            schema_port_map.insert(schema_id, port);
            schema_map.insert(port, schema);
            replication_changes_type_map.insert(port, replication_changes_type);
        }

        (schema_map, schema_port_map, replication_changes_type_map)
    }
}

impl SourceFactory<SchemaSQLContext> for ConnectorSourceFactory {
    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let mut schema = self
            .schema_map
            .get(port)
            .map_or(Err(ExecutionError::PortNotFoundInSource(*port)), |s| {
                Ok(s.clone())
            })?;

        let table_name = self
            .ports
            .iter()
            .find(|(_, p)| **p == *port)
            .unwrap()
            .0
            .clone();
        // Add source information to the schema.
        let mut fields = vec![];
        for field in schema.fields {
            let mut f = field.clone();
            f.source = SourceDefinition::Table {
                connection: self.connection.name.clone(),
                name: table_name.clone(),
            };
            fields.push(f);
        }
        schema.fields = fields;

        Ok((schema, SchemaSQLContext::default()))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        self.ports
            .values()
            .map(|e| {
                self.replication_changes_type_map.get(e).map_or(
                    Err(ReplicationTypeNotFound),
                    |typ| {
                        Ok(OutputPortDef::new(
                            *e,
                            map_replication_type_to_output_port_type(typ),
                        ))
                    },
                )
            })
            .collect()
    }

    fn prepare(
        &self,
        output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        use std::println as info;
        for (port, schema) in output_schemas {
            let (name, _) = self
                .ports
                .iter()
                .find(|(_, p)| **p == port)
                .map_or(Err(ExecutionError::PortNotFound(port.to_string())), Ok)?;
            info!("Source: Initializing input schema: {name}");
            schema.0.print().printstd();
        }
        Ok(())
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        Ok(Box::new(ConnectorSource {
            ingestor,
            iterator: Mutex::new(iterator),
            schema_port_map: self.schema_port_map.clone(),
            tables: self.tables.clone(),
            connection: self.connection.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct ConnectorSource {
    ingestor: Ingestor,
    iterator: Mutex<IngestionIterator>,
    schema_port_map: HashMap<u32, u16>,
    tables: Vec<TableInfo>,
    connection: Connection,
}

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let connector = get_connector(self.connection.to_owned())
            .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))?;

        thread::scope(|scope| {
            let t = scope.spawn(|| {
                match connector.start(from_seq, &self.ingestor, Some(self.tables.clone())) {
                    Ok(_) => {}
                    // If we get a channel error, it means the source sender thread has quit.
                    // Any error handling is done in that thread.
                    Err(ConnectorError::IngestorError(IngestorError::ChannelError(_))) => (),
                    Err(e) => std::panic::panic_any(e),
                }
            });

            let mut iterator = self.iterator.lock();
            for ((lsn, seq_no), op) in iterator.by_ref() {
                let identifier = match &op {
                    Operation::Delete { old } => old.schema_id.to_owned(),
                    Operation::Insert { new } => new.schema_id.to_owned(),
                    Operation::Update { old: _, new } => new.schema_id.to_owned(),
                };
                let schema_id = get_schema_id(identifier.as_ref())?;
                let port = self.schema_port_map.get(&schema_id).map_or(
                    Err(ExecutionError::SourceError(SourceError::PortError(
                        schema_id.to_string(),
                    ))),
                    Ok,
                )?;
                fw.send(lsn, seq_no, op, *port)?
            }

            // If we reach here, it means the connector thread has quit and the `ingestor` has been dropped.
            // `join` will not block.
            if let Err(e) = t.join() {
                std::panic::panic_any(e);
            }

            Ok(())
        })
    }
}

fn get_schema_id(op_schema_id: Option<&SchemaIdentifier>) -> Result<u32, ExecutionError> {
    Ok(op_schema_id
        .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
        .id)
}
