use dozer_core::channels::SourceChannelForwarder;
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_ingestion::connectors::{get_connector, CdcType, Connector, TableInfo};
use dozer_ingestion::errors::ConnectorError;
use dozer_ingestion::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_sql::pipeline::builder::SchemaSQLContext;

use dozer_types::errors::internal::BoxedError;
use dozer_types::indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use dozer_types::ingestion_types::{IngestionMessage, IngestionMessageKind, IngestorError};
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::parking_lot::Mutex;
use dozer_types::thiserror::{self, Error};
use dozer_types::tracing::{span, Level};
use dozer_types::types::{Operation, Schema, SchemaIdentifier, SourceDefinition};
use metrics::{describe_counter, increment_counter};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

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
    schema_name: Option<String>,
    name: String,
    columns: Vec<String>,
    schema: Schema,
    cdc_type: CdcType,
    port: PortHandle,
}

#[derive(Debug, Error)]
pub enum ConnectorSourceFactoryError {
    #[error("Connector error: {0}")]
    Connector(#[from] ConnectorError),
    #[error("Port not found for source: {0}")]
    PortNotFoundInSource(PortHandle),
    #[error("Schema not initialized")]
    SchemaNotInitialized,
}

#[derive(Debug)]
pub struct ConnectorSourceFactory {
    connection_name: String,
    tables: Vec<Table>,
    /// Will be moved to `ConnectorSource` in `build`.
    connector: Mutex<Option<Box<dyn Connector>>>,
    runtime: Arc<Runtime>,
    progress: Option<MultiProgress>,
}

fn map_replication_type_to_output_port_type(typ: &CdcType) -> OutputPortType {
    match typ {
        CdcType::FullChanges => OutputPortType::StatefulWithPrimaryKeyLookup,
        CdcType::OnlyPK => OutputPortType::StatefulWithPrimaryKeyLookup,
        CdcType::Nothing => OutputPortType::Stateless,
    }
}

impl ConnectorSourceFactory {
    pub async fn new(
        table_and_ports: Vec<(TableInfo, PortHandle)>,
        connection: Connection,
        runtime: Arc<Runtime>,
        progress: Option<MultiProgress>,
    ) -> Result<Self, ConnectorSourceFactoryError> {
        let connection_name = connection.name.clone();

        let connector = get_connector(connection)?;
        let tables: Vec<TableInfo> = table_and_ports
            .iter()
            .map(|(table, _)| table.clone())
            .collect();
        let source_schemas = connector.get_schemas(&tables).await?;

        let mut tables = vec![];
        for ((table, port), source_schema) in table_and_ports.into_iter().zip(source_schemas) {
            let name = table.name;
            let columns = table.column_names;
            let source_schema = source_schema?;
            let schema = source_schema.schema;
            let cdc_type = source_schema.cdc_type;

            let table = Table {
                name,
                schema_name: table.schema.clone(),
                columns,
                schema,
                cdc_type,
                port,
            };

            tables.push(table);
        }

        Ok(Self {
            connection_name,
            tables,
            connector: Mutex::new(Some(connector)),
            runtime,
            progress,
        })
    }
}

impl SourceFactory<SchemaSQLContext> for ConnectorSourceFactory {
    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        let table = self
            .tables
            .iter()
            .find(|table| table.port == *port)
            .ok_or(ConnectorSourceFactoryError::PortNotFoundInSource(*port))?;
        let mut schema = table.schema.clone();
        let table_name = &table.name;

        // Add source information to the schema.
        for field in &mut schema.fields {
            field.source = SourceDefinition::Table {
                connection: self.connection_name.clone(),
                name: table_name.clone(),
            };
        }

        info!(
            "Source: Initializing input schema: {}\n{}",
            table_name,
            schema.print()
        );

        Ok((schema, SchemaSQLContext::default()))
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.tables
            .iter()
            .map(|table| {
                let typ = map_replication_type_to_output_port_type(&table.cdc_type);
                OutputPortDef::new(table.port, typ)
            })
            .collect()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

        let mut schema_port_map = HashMap::new();
        for table in &self.tables {
            let schema_id = table
                .schema
                .identifier
                .ok_or(ConnectorSourceFactoryError::SchemaNotInitialized)?
                .id;
            schema_port_map.insert(schema_id, (table.port, table.name.clone()));
        }

        let tables = self
            .tables
            .iter()
            .map(|table| TableInfo {
                schema: table.schema_name.clone(),
                name: table.name.clone(),
                column_names: table.columns.clone(),
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
            runtime: self.runtime.clone(),
            connection_name: self.connection_name.clone(),
            bars,
        }))
    }
}

#[derive(Error, Debug)]
enum ConnectorSourceError {
    #[error("Schema not initialized")]
    SchemaNotInitialized,
    #[error("Failed to find table in Source: {0}")]
    PortError(u32),
}

#[derive(Debug)]
pub struct ConnectorSource {
    ingestor: Ingestor,
    iterator: Mutex<IngestionIterator>,
    schema_port_map: HashMap<u32, (PortHandle, String)>,
    tables: Vec<TableInfo>,
    connector: Box<dyn Connector>,
    runtime: Arc<Runtime>,
    connection_name: String,
    bars: HashMap<PortHandle, ProgressBar>,
}

const SOURCE_OPERATION_COUNTER_NAME: &str = "source_operation";

impl Source for ConnectorSource {
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, BoxedError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), BoxedError> {
        thread::scope(|scope| {
            describe_counter!(
                SOURCE_OPERATION_COUNTER_NAME,
                "Number of operation processed by source"
            );

            let mut counter = HashMap::new();
            let t = scope.spawn(|| {
                match self
                    .runtime
                    .block_on(self.connector.start(&self.ingestor, self.tables.clone()))
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
                let span = span!(
                    Level::TRACE,
                    "pipeline_source_start",
                    self.connection_name,
                    identifier.txid,
                    identifier.seq_in_tx
                );
                let _enter = span.enter();

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
                    IngestionMessageKind::SnapshottingDone
                    | IngestionMessageKind::SnapshottingStarted => {
                        for (port, _) in self.schema_port_map.values() {
                            fw.send(
                                IngestionMessage {
                                    identifier,
                                    kind: kind.clone(),
                                },
                                *port,
                            )?;
                        }
                        None
                    }
                    IngestionMessageKind::SnapshotBatch(batch) => {
                        for (port, _) in self.schema_port_map.values() {
                            fw.send(
                                IngestionMessage {
                                    identifier,
                                    kind: kind.clone(),
                                },
                                *port,
                            )?;
                        }
                        None
                    }
                };
                if let Some(schema_id) = schema_id {
                    let (port, table_name) = self
                        .schema_port_map
                        .get(&schema_id)
                        .ok_or(ConnectorSourceError::PortError(schema_id))?;

                    let mut labels = vec![
                        ("connection", self.connection_name.clone()),
                        ("table", table_name.to_string()),
                    ];
                    match &kind {
                        IngestionMessageKind::OperationEvent(Operation::Delete { .. }) => {
                            labels.push(("operation_type", "delete".to_string()));
                            increment_counter!(SOURCE_OPERATION_COUNTER_NAME, &labels);
                        }
                        IngestionMessageKind::OperationEvent(Operation::Insert { .. }) => {
                            labels.push(("operation_type", "insert".to_string()));
                            increment_counter!(SOURCE_OPERATION_COUNTER_NAME, &labels);
                        }
                        IngestionMessageKind::OperationEvent(Operation::Update { .. }) => {
                            labels.push(("operation_type", "update".to_string()));
                            increment_counter!(SOURCE_OPERATION_COUNTER_NAME, &labels);
                        }
                        _ => {}
                    }
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

    fn snapshot(&self, fw: &mut dyn SourceChannelForwarder) -> Result<(), BoxedError> {
        Ok(())
    }
}

fn get_schema_id(op_schema_id: Option<SchemaIdentifier>) -> Result<u32, ConnectorSourceError> {
    Ok(op_schema_id
        .ok_or(ConnectorSourceError::SchemaNotInitialized)?
        .id)
}
