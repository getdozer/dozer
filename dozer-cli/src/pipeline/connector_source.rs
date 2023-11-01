use dozer_api::shutdown::ShutdownReceiver;
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory, SourceState,
};
use dozer_ingestion::{
    get_connector, CdcType, Connector, TableIdentifier, TableInfo, TableToIngest,
};
use dozer_ingestion::{IngestionConfig, Ingestor};

use dozer_tracing::LabelsAndProgress;
use dozer_types::errors::internal::BoxedError;
use dozer_types::indicatif::ProgressBar;
use dozer_types::log::info;
use dozer_types::models::connection::Connection;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::parking_lot::Mutex;
use dozer_types::thiserror::{self, Error};
use dozer_types::tracing::{span, Level};
use dozer_types::types::{Operation, Schema, SourceDefinition};
use futures::stream::{AbortHandle, Abortable, Aborted};
use metrics::{describe_counter, increment_counter};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

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
    Connector(#[source] BoxedError),
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
    labels: LabelsAndProgress,
    shutdown: ShutdownReceiver,
}

fn map_replication_type_to_output_port_type(typ: &CdcType) -> OutputPortType {
    match typ {
        CdcType::FullChanges => OutputPortType::Stateless,
        CdcType::OnlyPK => OutputPortType::StatefulWithPrimaryKeyLookup,
        CdcType::Nothing => OutputPortType::Stateless,
    }
}

impl ConnectorSourceFactory {
    pub async fn new(
        mut table_and_ports: Vec<(TableInfo, PortHandle)>,
        connection: Connection,
        runtime: Arc<Runtime>,
        labels: LabelsAndProgress,
        shutdown: ShutdownReceiver,
    ) -> Result<Self, ConnectorSourceFactoryError> {
        let connection_name = connection.name.clone();

        let connector = get_connector(runtime.clone(), connection)
            .map_err(|e| ConnectorSourceFactoryError::Connector(e.into()))?;

        // Fill column names if not provided.
        let table_identifiers = table_and_ports
            .iter()
            .map(|(table, _)| TableIdentifier::new(table.schema.clone(), table.name.clone()))
            .collect();
        let all_columns = connector
            .list_columns(table_identifiers)
            .await
            .map_err(ConnectorSourceFactoryError::Connector)?;
        for ((table, _), columns) in table_and_ports.iter_mut().zip(all_columns) {
            if table.column_names.is_empty() {
                table.column_names = columns.column_names;
            }
        }

        let tables: Vec<TableInfo> = table_and_ports
            .iter()
            .map(|(table, _)| table.clone())
            .collect();
        let source_schemas = connector
            .get_schemas(&tables)
            .await
            .map_err(ConnectorSourceFactoryError::Connector)?;

        let mut tables = vec![];
        for ((table, port), source_schema) in table_and_ports.into_iter().zip(source_schemas) {
            let name = table.name;
            let columns = table.column_names;
            let source_schema = source_schema.map_err(ConnectorSourceFactoryError::Connector)?;
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
            labels,
            shutdown,
        })
    }
}

impl SourceFactory for ConnectorSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
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

        Ok(schema)
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        let table = self
            .tables
            .iter()
            .find(|table| table.port == *port)
            .unwrap_or_else(|| panic!("Port {} not found", port));
        table.name.clone()
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
        let tables = self
            .tables
            .iter()
            .map(|table| TableInfo {
                schema: table.schema_name.clone(),
                name: table.name.clone(),
                column_names: table.columns.clone(),
            })
            .collect();
        let ports = self.tables.iter().map(|table| table.port).collect();

        let connector = self
            .connector
            .lock()
            .take()
            .expect("ConnectorSource was already built");

        let mut bars = vec![];
        for table in &self.tables {
            let pb = self.labels.create_progress_bar(table.name.clone());
            bars.push(pb);
        }

        Ok(Box::new(ConnectorSource {
            tables,
            ports,
            connector,
            runtime: self.runtime.clone(),
            connection_name: self.connection_name.clone(),
            labels: self.labels.clone(),
            bars,
            shutdown: self.shutdown.clone(),
            ingestion_config: IngestionConfig::default(),
        }))
    }
}

#[derive(Debug)]
pub struct ConnectorSource {
    tables: Vec<TableInfo>,
    ports: Vec<PortHandle>,
    connector: Box<dyn Connector>,
    runtime: Arc<Runtime>,
    connection_name: String,
    labels: LabelsAndProgress,
    bars: Vec<ProgressBar>,
    shutdown: ShutdownReceiver,
    ingestion_config: IngestionConfig,
}

const SOURCE_OPERATION_COUNTER_NAME: &str = "source_operation";

impl Source for ConnectorSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        thread::scope(|scope| {
            describe_counter!(
                SOURCE_OPERATION_COUNTER_NAME,
                "Number of operation processed by source"
            );

            let mut counter = vec![0; self.tables.len()];

            let (ingestor, mut iterator) =
                Ingestor::initialize_channel(self.ingestion_config.clone());
            let t = scope.spawn(|| {
                self.runtime.block_on(async move {
                    let ingestor = ingestor;
                    let shutdown_future = self.shutdown.create_shutdown_future();
                    let (abort_handle, abort_registration) = AbortHandle::new_pair();

                    // Construct the tables to ingest.
                    let tables = self
                        .tables
                        .iter()
                        .zip(&self.ports)
                        .map(|(table, port)| {
                            let checkpoint = last_checkpoint.get(port).copied().flatten();
                            TableToIngest {
                                schema: table.schema.clone(),
                                name: table.name.clone(),
                                column_names: table.column_names.clone(),
                                checkpoint,
                            }
                        })
                        .collect::<Vec<_>>();

                    // Abort the connector when we shut down
                    // TODO: pass a `CancellationToken` to the connector to allow
                    // it to gracefully shut down.
                    let name = self.connection_name.clone();
                    tokio::spawn(async move {
                        shutdown_future.await;
                        abort_handle.abort();
                        eprintln!("Aborted connector {}", name);
                    });
                    let result =
                        Abortable::new(self.connector.start(&ingestor, tables), abort_registration)
                            .await;
                    match result {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => std::panic::panic_any(e),
                        // Aborted means we are shutting down
                        Err(Aborted) => (),
                    }
                })
            });

            for message in iterator.by_ref() {
                let span = span!(Level::TRACE, "pipeline_source_start", self.connection_name,);
                let _enter = span.enter();

                match &message {
                    IngestionMessage::OperationEvent {
                        table_index, op, ..
                    } => {
                        let port = self.ports[*table_index];
                        let table_name = &self.tables[*table_index].name;

                        // Update metrics
                        let mut labels = self.labels.labels().clone();
                        labels.push("connection", self.connection_name.clone());
                        labels.push("table", table_name.clone());
                        const OPERATION_TYPE_LABEL: &str = "operation_type";
                        match op {
                            Operation::Delete { .. } => {
                                labels.push(OPERATION_TYPE_LABEL, "delete");
                            }
                            Operation::Insert { .. } => {
                                labels.push(OPERATION_TYPE_LABEL, "insert");
                            }
                            Operation::Update { .. } => {
                                labels.push(OPERATION_TYPE_LABEL, "update");
                            }
                        }
                        increment_counter!(SOURCE_OPERATION_COUNTER_NAME, labels);

                        // Update counter
                        let counter = &mut counter[*table_index];
                        *counter += 1;
                        if *counter % 1000 == 0 {
                            self.bars[*table_index].set_position(*counter);
                        }

                        // Send message to the pipeline
                        fw.send(message, port)?;
                    }
                    IngestionMessage::SnapshottingDone | IngestionMessage::SnapshottingStarted => {
                        for port in &self.ports {
                            fw.send(message.clone(), *port)?;
                        }
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
