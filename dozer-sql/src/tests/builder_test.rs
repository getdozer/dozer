use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::checkpoint::create_checkpoint_for_test;
use dozer_core::dozer_log::storage::Queue;
use dozer_core::epoch::Epoch;
use dozer_core::executor::DagExecutor;
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
    SourceState,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_recordstore::ProcessorRecordStore;
use dozer_types::chrono::DateTime;
use dozer_types::errors::internal::BoxedError;
use dozer_types::log::debug;
use dozer_types::models::ingestion_types::IngestionMessage;
use dozer_types::node::OpIdentifier;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::builder::statement_to_pipeline;
use crate::tests::utils::create_test_runtime;

/// Test Source
#[derive(Debug)]
pub struct TestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl TestSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect()
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("CustomerID"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Country"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("Spending"),
                    FieldType::Float,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    String::from("timestamp"),
                    FieldType::Timestamp,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone())
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        format!("port_{}", port)
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(TestSource {}))
    }
}

#[derive(Debug)]
pub struct TestSource {}

impl Source for TestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: SourceState,
    ) -> Result<(), BoxedError> {
        for n in 0..10 {
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op: Operation::Insert {
                        new: Record::new(vec![
                            Field::Int(0),
                            Field::String("Italy".to_string()),
                            Field::Float(OrderedFloat(5.5)),
                            Field::Timestamp(
                                DateTime::parse_from_rfc3339("2020-01-01T00:13:00Z").unwrap(),
                            ),
                        ]),
                    },
                    id: Some(OpIdentifier::new(n, 0)),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
}

impl TestSinkFactory {
    pub fn new(input_ports: Vec<PortHandle>) -> Self {
        Self { input_ports }
    }
}

impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(TestSink {}))
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSink {}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        println!("Sink: {:?}", op.load(record_store).unwrap());
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn persist(&mut self, _epoch: &Epoch, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[test]
fn test_pipeline_builder() {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let runtime = create_test_runtime();
    let context = statement_to_pipeline(
        "SELECT t.Spending  \
        FROM TTL(TUMBLE(users, timestamp, '5 MINUTES'), timestamp, '1 MINUTE') t JOIN users u on t.CustomerID=u.CustomerID \
         WHERE t.Spending >= 1",
        &mut pipeline,
        Some("results".to_string()),
        vec![],
        runtime.clone()
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE])),
        AppSourceMappings::new(
            "mem".to_string(),
            vec![("users".to_string(), DEFAULT_PORT_HANDLE)]
                .into_iter()
                .collect(),
        ),
    )
    .unwrap();

    pipeline.add_sink(
        Box::new(TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE])),
        "sink",
        None,
    );
    pipeline.connect_nodes(
        &table_info.node,
        table_info.port,
        "sink",
        DEFAULT_PORT_HANDLE,
    );

    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.into_dag().unwrap();

    let now = std::time::Instant::now();

    runtime.block_on(async move {
        let (_temp_dir, checkpoint) = create_checkpoint_for_test().await;
        DagExecutor::new(dag, checkpoint, Default::default())
            .await
            .unwrap()
            .start(Arc::new(AtomicBool::new(true)), Default::default())
            .await
            .unwrap()
            .join()
            .unwrap();
    });

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
