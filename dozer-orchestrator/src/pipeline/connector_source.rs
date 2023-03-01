use dozer_core::channels::SourceChannelForwarder;
use dozer_core::errors::ExecutionError::{InternalError, ReplicationTypeNotFound};
use dozer_core::errors::{ExecutionError, SourceError};
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_ingestion::connectors::{get_connector, Connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::ingestion_types::IngestorError;
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::Mutex;
use dozer_types::types::{
    Operation, ReplicationChangesTrackingType, Schema, SchemaIdentifier, SourceDefinition,
    SourceSchema,
};
use std::collections::HashMap;
use std::thread;

fn attach_progress(multi_pb: Option<MultiProgress>) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    multi_pb.as_ref().map(|m| m.add(pb.clone()));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.red} {msg}: {pos}: {per_sec}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}

#[derive(Debug)]
pub struct ConnectorSourceFactory {
    pub ports: HashMap<String, u16>,
    pub schema_port_map: HashMap<u32, u16>,
    pub schema_map: HashMap<u16, Schema>,
    pub replication_changes_type_map: HashMap<u16, ReplicationChangesTrackingType>,
    pub tables: Vec<TableInfo>,
    pub connection: Connection,
    pub progress: Option<MultiProgress>,
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
        progress: Option<MultiProgress>,
    ) -> Result<Self, ExecutionError> {
        let (schema_map, schema_port_map, replication_changes_type_map) =
            Self::get_schema_map(connection.clone(), tables.clone(), ports.clone())?;
        Ok(Self {
            ports,
            schema_port_map,
            schema_map,
            replication_changes_type_map,
            tables,
            connection,
            progress,
        })
    }

    #[allow(clippy::type_complexity)]
    fn get_schema_map(
        connection: Connection,
        tables: Vec<TableInfo>,
        ports: HashMap<String, u16>,
    ) -> Result<
        (
            HashMap<u16, Schema>,
            HashMap<u32, u16>,
            HashMap<u16, ReplicationChangesTrackingType>,
        ),
        ExecutionError,
    > {
        let tables_map = tables
            .iter()
            .map(|table| (table.table_name.clone(), table.name.clone()))
            .collect::<HashMap<_, _>>();

        let connector = get_connector(connection).map_err(|e| InternalError(Box::new(e)))?;
        let schema_tuples = connector
            .get_schemas(Some(tables))
            .map_err(|e| InternalError(Box::new(e)))?;

        let mut schema_map = HashMap::new();
        let mut schema_port_map: HashMap<u32, u16> = HashMap::new();
        let mut replication_changes_type_map: HashMap<u16, ReplicationChangesTrackingType> =
            HashMap::new();

        for SourceSchema {
            name,
            schema,
            replication_type,
        } in schema_tuples
        {
            let source_name = tables_map.get(&name).unwrap();
            let port: u16 = *ports
                .get(source_name)
                .ok_or(ExecutionError::PortNotFound(name))?;
            let schema_id = get_schema_id(schema.identifier)?;

            schema_port_map.insert(schema_id, port);
            schema_map.insert(port, schema);
            replication_changes_type_map.insert(port, replication_type);
        }

        Ok((schema_map, schema_port_map, replication_changes_type_map))
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

        let table_name = self.ports.iter().find(|(_, p)| **p == *port).unwrap().0;
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

        use std::println as info;
        info!("Source: Initializing input schema: {table_name}");
        schema.print().printstd();

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

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());
        let connector = get_connector(self.connection.clone())
            .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))?;

        let mut bars = HashMap::new();
        for (name, port) in self.ports.iter() {
            let pb = attach_progress(self.progress.clone());
            pb.set_message(name.clone());
            bars.insert(*port, pb);
        }

        Ok(Box::new(ConnectorSource {
            ingestor,
            iterator: Mutex::new(iterator),
            schema_port_map: self.schema_port_map.clone(),
            tables: self.tables.clone(),
            connector,
            bars,
        }))
    }
}

#[derive(Debug)]
pub struct ConnectorSource {
    ingestor: Ingestor,
    iterator: Mutex<IngestionIterator>,
    schema_port_map: HashMap<u32, u16>,
    tables: Vec<TableInfo>,
    connector: Box<dyn Connector>,
    bars: HashMap<u16, ProgressBar>,
}

impl Source for ConnectorSource {
    fn can_start_from(&self, last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError> {
        self.connector
            .as_ref()
            .can_start_from(last_checkpoint)
            .map_err(|e| ExecutionError::ConnectorError(Box::new(e)))
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        thread::scope(|scope| {
            let mut counter = HashMap::new();
            let t = scope.spawn(|| {
                match self
                    .connector
                    .start(last_checkpoint, &self.ingestor, self.tables.clone())
                {
                    Ok(_) => {}
                    // If we get a channel error, it means the source sender thread has quit.
                    // Any error handling is done in that thread.
                    Err(ConnectorError::IngestorError(IngestorError::ChannelError(_))) => (),
                    Err(e) => std::panic::panic_any(e),
                }
            });

            let mut iterator = self.iterator.lock();

            for ((lsn, seq_no), op) in iterator.by_ref() {
                let schema_id = match &op {
                    Operation::Delete { old } => Some(get_schema_id(old.schema_id)?),
                    Operation::Insert { new } => Some(get_schema_id(new.schema_id)?),
                    Operation::Update { old: _, new } => Some(get_schema_id(new.schema_id)?),
                    Operation::SnapshottingDone {} => None,
                };
                if let Some(schema_id) = schema_id {
                    let port =
                        self.schema_port_map
                            .get(&schema_id)
                            .ok_or(ExecutionError::SourceError(SourceError::PortError(
                                schema_id,
                            )))?;

                    counter
                        .entry(schema_id)
                        .and_modify(|e| *e += 1)
                        .or_insert(1);

                    let schema_counter = counter.get(&schema_id).unwrap();
                    if *schema_counter % 1000 == 0 {
                        let pb = self.bars.get(port);
                        if let Some(pb) = pb {
                            pb.set_position(*schema_counter);
                        }
                    }
                    fw.send(lsn, seq_no, op, *port)?
                } else {
                    for port in self.schema_port_map.values() {
                        fw.send(lsn, seq_no, op.clone(), *port)?
                    }
                }
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

fn get_schema_id(op_schema_id: Option<SchemaIdentifier>) -> Result<u32, ExecutionError> {
    Ok(op_schema_id
        .map_or(Err(ExecutionError::SchemaNotInitialized), Ok)?
        .id)
}
