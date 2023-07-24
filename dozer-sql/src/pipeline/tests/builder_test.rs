use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::epoch::Epoch;
use dozer_types::errors::internal::BoxedError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::log::debug;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};

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

impl SourceFactory<SchemaSQLContext> for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect()
    }

    fn get_output_schema(
        &self,
        _port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        Ok((
            Schema::default()
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
                .clone(),
            SchemaSQLContext::default(),
        ))
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
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, BoxedError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), BoxedError> {
        for n in 0..10000 {
            fw.send(
                IngestionMessage::new_op(
                    n,
                    0,
                    0,
                    Operation::Insert {
                        new: Record::new(vec![
                            Field::Int(0),
                            Field::String("Italy".to_string()),
                            Field::Float(OrderedFloat(5.5)),
                        ]),
                    },
                ),
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

impl SinkFactory<SchemaSQLContext> for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(TestSink {}))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSink {}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[test]
fn test_pipeline_builder() {
    let mut pipeline = AppPipeline::new();
    let context = statement_to_pipeline(
        "SELECT COUNT(Spending), users.Country \
        FROM users \
         WHERE Spending >= 1",
        &mut pipeline,
        Some("results".to_string()),
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "mem".to_string(),
        Arc::new(TestSourceFactory::new(vec![DEFAULT_PORT_HANDLE])),
        vec![("users".to_string(), DEFAULT_PORT_HANDLE)]
            .into_iter()
            .collect(),
    ))
    .unwrap();

    pipeline.add_sink(
        Arc::new(TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE])),
        "sink",
    );
    pipeline.connect_nodes(
        &table_info.node,
        Some(table_info.port),
        "sink",
        Some(DEFAULT_PORT_HANDLE),
        true,
    );

    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.get_dag().unwrap();

    let now = std::time::Instant::now();

    DagExecutor::new(dag, ExecutorOptions::default())
        .unwrap()
        .start(Arc::new(AtomicBool::new(true)))
        .unwrap()
        .join()
        .unwrap();

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
