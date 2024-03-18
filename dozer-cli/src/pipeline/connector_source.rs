use dozer_core::event::EventHub;
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Source, SourceFactory};
use dozer_core::shutdown::ShutdownReceiver;
use dozer_ingestion::{
    get_connector, CdcType, Connector, IngestionIterator, TableIdentifier, TableInfo,
};
use dozer_ingestion::{IngestionConfig, Ingestor};
use dozer_tracing::metrics::{
    CONNECTION_LABEL, DOZER_METER_NAME, OPERATION_TYPE_LABEL, SOURCE_OPERATION_COUNTER_NAME,
    TABLE_LABEL,
};
use dozer_tracing::LabelsAndProgress;
use dozer_types::errors::internal::BoxedError;
use dozer_types::log::{error, info};
use dozer_types::models::connection::Connection;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::OpIdentifier;
use dozer_types::thiserror::{self, Error};
use dozer_types::tracing::{span, Level};
use dozer_types::types::{Operation, Schema, SourceDefinition};
use futures::stream::{AbortHandle, Abortable, Aborted};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tonic::async_trait;

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
    connection: Connection,
    runtime: Arc<Runtime>,
    tables: Vec<Table>,
    labels: LabelsAndProgress,
    shutdown: ShutdownReceiver,
}

fn map_replication_type_to_output_port_type(_typ: &CdcType) -> OutputPortType {
    OutputPortType::Stateless
}

impl ConnectorSourceFactory {
    pub async fn new(
        mut table_and_ports: Vec<(TableInfo, PortHandle)>,
        connection: Connection,
        runtime: Arc<Runtime>,
        labels: LabelsAndProgress,
        shutdown: ShutdownReceiver,
    ) -> Result<Self, ConnectorSourceFactoryError> {
        let mut connector =
            get_connector(runtime.clone(), EventHub::new(1), connection.clone(), None)
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
            connection,
            runtime,
            tables,
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
                connection: self.connection.name.clone(),
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
        event_hub: EventHub,
        state: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        // Construct table info.
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

        let connector = get_connector(
            self.runtime.clone(),
            event_hub,
            self.connection.clone(),
            state,
        )?;

        Ok(Box::new(ConnectorSource {
            tables,
            ports,
            connector,
            connection_name: self.connection.name.clone(),
            labels: self.labels.clone(),
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
    connection_name: String,
    labels: LabelsAndProgress,
    shutdown: ShutdownReceiver,
    ingestion_config: IngestionConfig,
}

#[async_trait]
impl Source for ConnectorSource {
    async fn serialize_state(&self) -> Result<Vec<u8>, BoxedError> {
        self.connector.serialize_state().await
    }

    async fn start(
        &mut self,
        sender: Sender<(PortHandle, IngestionMessage)>,
        last_checkpoint: Option<OpIdentifier>,
    ) -> Result<(), BoxedError> {
        let (ingestor, iterator) = Ingestor::initialize_channel(self.ingestion_config.clone());
        let connection_name = self.connection_name.clone();
        let tables = self.tables.clone();
        let ports = self.ports.clone();
        let labels = self.labels.clone();
        let handle = tokio::spawn(forward_message_to_pipeline(
            iterator,
            sender,
            connection_name,
            tables,
            ports,
            labels,
        ));

        let shutdown_future = self.shutdown.create_shutdown_future();
        let (abort_handle, abort_registration) = AbortHandle::new_pair();

        // Abort the connector when we shut down
        // TODO: pass a `CancellationToken` to the connector to allow
        // it to gracefully shut down.
        let name = self.connection_name.clone();
        tokio::spawn(async move {
            shutdown_future.await;
            abort_handle.abort();
            eprintln!("Aborted connector {}", name);
        });
        let result = Abortable::new(
            self.connector
                .start(&ingestor, self.tables.clone(), last_checkpoint),
            abort_registration,
        )
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            // Aborted means we are shutting down
            Err(Aborted) => return Ok(()),
        }
        drop(ingestor);

        // If we reach here, it means the connector has finished ingesting, so we wait for the forwarding task to finish.
        if let Err(e) = handle.await {
            std::panic::panic_any(e);
        }

        Ok(())
    }
}

async fn forward_message_to_pipeline(
    mut iterator: IngestionIterator,
    sender: Sender<(PortHandle, IngestionMessage)>,
    connection_name: String,
    tables: Vec<TableInfo>,
    ports: Vec<PortHandle>,
    labels: LabelsAndProgress,
) {
    let mut bars = vec![];
    for table in &tables {
        let pb = labels.create_progress_bar(table.name.clone());
        bars.push(pb);
    }

    let meter = dozer_tracing::global::meter(DOZER_METER_NAME);

    let source_counter = meter
        .u64_counter(SOURCE_OPERATION_COUNTER_NAME)
        .with_description("Number of operation processed by source")
        .init();

    let mut counter = vec![(0u64, 0u64); tables.len()];
    while let Some(message) = iterator.receiver.recv().await {
        let span = span!(Level::TRACE, "pipeline_source_start", connection_name);
        let _enter = span.enter();

        match &message {
            IngestionMessage::OperationEvent {
                table_index, op, ..
            } => {
                let port = ports[*table_index];
                let table_name = &tables[*table_index].name;

                // Update metrics

                let mut labels = labels.attrs();
                labels.push(dozer_tracing::KeyValue::new(
                    CONNECTION_LABEL,
                    connection_name.clone(),
                ));

                labels.push(dozer_tracing::KeyValue::new(
                    TABLE_LABEL,
                    table_name.clone(),
                ));

                let op_str = match op {
                    Operation::Insert { .. } => "insert",
                    Operation::Delete { .. } => "delete",
                    Operation::Update { .. } => "update",
                    Operation::BatchInsert { .. } => "insert",
                };
                labels.push(dozer_tracing::KeyValue::new(OPERATION_TYPE_LABEL, op_str));

                let counter_number: u64 = match op {
                    Operation::BatchInsert { new } => new.to_owned().len().try_into().unwrap_or(1),
                    _ => 1,
                };

                source_counter.add(counter_number, &labels);

                // Update counter
                let counter = &mut counter[*table_index];
                if let Operation::BatchInsert { new } = &op {
                    counter.0 += new.len() as u64;
                } else {
                    counter.0 += 1;
                }
                if counter.0 >> 10 > counter.1 {
                    counter.1 = counter.0 >> 10;
                    bars[*table_index].set_position(counter.0);
                }

                // Send message to the pipeline
                if sender.send((port, message)).await.is_err() {
                    break;
                }
            }
            IngestionMessage::TransactionInfo(_) => {
                // For transaction level messages, we can send to any port.
                if sender.send((ports[0], message)).await.is_err() {
                    break;
                }
            }
        }
    }
}
