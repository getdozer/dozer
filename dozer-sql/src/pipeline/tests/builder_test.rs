use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::checkpoint::create_checkpoint_factory_for_test;
use dozer_core::dozer_log::storage::Queue;
use dozer_core::epoch::Epoch;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
    SourceState,
};
use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::debug;
use dozer_types::node::OpIdentifier;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::pipeline::builder::statement_to_pipeline;

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
        for n in 0..10000 {
            fw.send(
                IngestionMessage::OperationEvent {
                    table_index: 0,
                    op: Operation::Insert {
                        new: Record::new(vec![
                            Field::Int(0),
                            Field::String("Italy".to_string()),
                            Field::Float(OrderedFloat(5.5)),
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
        _record_store: &ProcessorRecordStore,
        _op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn persist(&mut self, _queue: &Queue) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[tokio::test]
async fn test_pipeline_builder() {
    let mut pipeline = AppPipeline::new_with_default_flags();
    let context = statement_to_pipeline(
        "SELECT COUNT(Spending), users.Country \
        FROM users \
         WHERE Spending >= 1",
        &mut pipeline,
        Some("results".to_string()),
        vec![],
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

    let (_temp_dir, checkpoint_factory, _) = create_checkpoint_factory_for_test(&[]).await;
    DagExecutor::new(
        dag,
        checkpoint_factory,
        Default::default(),
        ExecutorOptions::default(),
    )
    .await
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)), Default::default())
    .unwrap()
    .join()
    .unwrap();

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
