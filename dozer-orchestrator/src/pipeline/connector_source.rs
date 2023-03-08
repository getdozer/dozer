use dozer_core::channels::SourceChannelForwarder;
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::errors::{ExecutionError, SourceError};
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_ingestion::connectors::{get_connector, ColumnInfo, Connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_sql::pipeline::builder::SchemaSQLContext;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::ingestion_types::{IngestionMessage, IngestionMessageKind, IngestorError};
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::Mutex;
use dozer_types::types::{
    Operation, ReplicationChangesTrackingType, Schema, SchemaIdentifier, SourceDefinition,
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
struct Table {
    name: String,
    columns: Option<Vec<ColumnInfo>>,
    schema: Schema,
    replication_type: ReplicationChangesTrackingType,
    port: PortHandle,
}

#[derive(Debug)]
pub struct ConnectorSourceFactory {
    connection_name: String,
    tables: Vec<Table>,
    /// Will be moved to `ConnectorSource` in `build`.
    connector: Mutex<Option<Box<dyn Connector>>>,
    progress: Option<MultiProgress>,
}

fn map_replication_type_to_output_port_type(
    typ: &ReplicationChangesTrackingType,
) -> OutputPortType {
    match typ {
        ReplicationChangesTrackingType::FullChanges => OutputPortType::StatefulWithPrimaryKeyLookup,
        ReplicationChangesTrackingType::OnlyPK => OutputPortType::StatefulWithPrimaryKeyLookup,
        ReplicationChangesTrackingType::Nothing => OutputPortType::Stateless,
    }
}

impl ConnectorSourceFactory {
    pub fn new(
        table_and_ports: Vec<(TableInfo, PortHandle)>,
        connection: Connection,
        progress: Option<MultiProgress>,
    ) -> Result<Self, ExecutionError> {
        let connection_name = connection.name.clone();

        let connector = get_connector(connection).map_err(|e| InternalError(Box::new(e)))?;
        let source_schemas = connector
            .get_schemas(Some(
                table_and_ports
                    .iter()
                    .map(|(table, _)| table.clone())
                    .collect(),
            ))
            .map_err(|e| InternalError(Box::new(e)))?;

        let mut tables = vec![];
        for ((table, port), source_schema) in
            table_and_ports.into_iter().zip(source_schemas.into_iter())
        {
            let name = table.name;
            let columns = table.columns;
            let schema = source_schema.schema;
            let replication_type = source_schema.replication_type;

            let table = Table {
                name,
                columns,
                schema,
                replication_type,
                port,
            };

            tables.push(table);
        }

        Ok(Self {
            connection_name,
            tables,
            connector: Mutex::new(Some(connector)),
            progress,
        })
    }
}

impl SourceFactory<SchemaSQLContext> for ConnectorSourceFactory {
    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let table = self
            .tables
            .iter()
            .find(|table| table.port == *port)
            .ok_or(ExecutionError::PortNotFoundInSource(*port))?;
        let mut schema = table.schema.clone();
        let table_name = &table.name;

        // Add source information to the schema.
        for field in &mut schema.fields {
            field.source = SourceDefinition::Table {
                connection: self.connection_name.clone(),
                name: table_name.clone(),
            };
        }

        use std::println as info;
        info!("Source: Initializing input schema: {table_name}");
        schema.print().printstd();

        Ok((schema, SchemaSQLContext::default()))
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.tables
            .iter()
            .map(|table| {
                let typ = map_replication_type_to_output_port_type(&table.replication_type);
                OutputPortDef::new(table.port, typ)
            })
            .collect()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let mut schema_port_map = HashMap::new();
        for table in &self.tables {
            let schema_id = get_schema_id(table.schema.identifier)?;
            schema_port_map.insert(schema_id, table.port);
        }

        let tables = self
            .tables
            .iter()
            .map(|table| TableInfo {
                name: table.name.clone(),
                columns: table.columns.clone(),
            })
            .collect();

        let connector = self
            .connector
            .lock()
            .take()
            .expect("ConnectorSource was already built");

        let mut bars = HashMap::new();
        for table in &self.tables {
            let pb = attach_progress(self.progress.clone());
            pb.set_message(table.name.clone());
            bars.insert(table.port, pb);
        }

        Ok(Box::new(ConnectorSource {
            ingestor,
            iterator: Mutex::new(iterator),
            schema_port_map,
            tables,
            connector,
            bars,
        }))
    }
}

#[derive(Debug)]
pub struct ConnectorSource {
    ingestor: Ingestor,
    iterator: Mutex<IngestionIterator>,
    schema_port_map: HashMap<u32, PortHandle>,
    tables: Vec<TableInfo>,
    connector: Box<dyn Connector>,
    bars: HashMap<PortHandle, ProgressBar>,
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

            for IngestionMessage { identifier, kind } in iterator.by_ref() {
                let schema_id = match &kind {
                    IngestionMessageKind::OperationEvent(Operation::Delete { old }) => {
                        Some(get_schema_id(old.schema_id)?)
                    }
                    IngestionMessageKind::OperationEvent(Operation::Insert { new }) => {
                        Some(get_schema_id(new.schema_id)?)
                    }
                    IngestionMessageKind::OperationEvent(Operation::Update { old: _, new }) => {
                        Some(get_schema_id(new.schema_id)?)
                    }
                    IngestionMessageKind::SnapshottingDone => None,
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
                    fw.send(IngestionMessage { identifier, kind }, *port)?
                } else {
                    for port in self.schema_port_map.values() {
                        fw.send(
                            IngestionMessage::new_snapshotting_done(
                                identifier.txid,
                                identifier.seq_in_tx,
                            ),
                            *port,
                        )?
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
