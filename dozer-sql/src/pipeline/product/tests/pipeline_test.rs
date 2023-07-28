use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::processor_record::ProcessorRecord;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::tracing::debug;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use crate::pipeline::window::tests::pipeline_test::TestSink;

const USER_PORT: u16 = 0 as PortHandle;
const DEPARTMENT_PORT: u16 = 1 as PortHandle;
const COUNTRY_PORT: u16 = 2 as PortHandle;

#[derive(Debug)]
pub struct TestSourceFactory {
    running: Arc<AtomicBool>,
}

impl TestSourceFactory {
    pub fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl SourceFactory<SchemaSQLContext> for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![
            OutputPortDef::new(USER_PORT, OutputPortType::Stateless),
            OutputPortDef::new(DEPARTMENT_PORT, OutputPortType::Stateless),
            OutputPortDef::new(COUNTRY_PORT, OutputPortType::Stateless),
        ]
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        if port == &USER_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "user".to_string(),
            };
            Ok((
                Schema::default()
                    .field(
                        FieldDefinition::new(
                            String::from("id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("name"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("department_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("country_id"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("salary"),
                            FieldType::Float,
                            false,
                            source_id,
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else if port == &DEPARTMENT_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "department".to_string(),
            };
            Ok((
                Schema::default()
                    .field(
                        FieldDefinition::new(
                            String::from("did"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("dname"),
                            FieldType::String,
                            false,
                            source_id,
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else if port == &COUNTRY_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "country".to_string(),
            };
            Ok((
                Schema::default()
                    .field(
                        FieldDefinition::new(
                            String::from("cid"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("cname"),
                            FieldType::String,
                            false,
                            source_id,
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else {
            panic!("Invalid Port Handle {port}");
        }
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(TestSource {
            running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct TestSource {
    running: Arc<AtomicBool>,
}

impl Source for TestSource {
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, BoxedError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), BoxedError> {
        let mut new_rec = ProcessorRecord::new();
        new_rec.extend_direct_field(Field::Int(0));
        new_rec.extend_direct_field(Field::String("IT".to_string()));
        let operations = vec![
            (
                Operation::Insert {
                    new: Record::new(vec![]),
                },
                DEPARTMENT_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![Field::Int(1), Field::String("HR".to_string())]),
                },
                DEPARTMENT_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10000),
                        Field::String("Alice".to_string()),
                        Field::Int(0),
                        Field::String("UK".to_string()),
                        Field::Float(OrderedFloat(1.1)),
                    ]),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10001),
                        Field::String("Bob".to_string()),
                        Field::Int(0),
                        Field::String("UK".to_string()),
                        Field::Float(OrderedFloat(1.1)),
                    ]),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::String("UK".to_string()),
                        Field::String("United Kingdom".to_string()),
                    ]),
                },
                COUNTRY_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::String("SG".to_string()),
                        Field::String("Singapore".to_string()),
                    ]),
                },
                COUNTRY_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10002),
                        Field::String("Craig".to_string()),
                        Field::Int(1),
                        Field::String("SG".to_string()),
                        Field::Float(OrderedFloat(1.1)),
                    ]),
                },
                USER_PORT,
            ),
            // (
            //     Operation::Delete {
            //         old: Record::new(
            //             None,
            //             vec![Field::Int(1), Field::String("HR".to_string())],
            //         ),
            //     },
            //     DEPARTMENT_PORT,
            // ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10003),
                        Field::String("Dan".to_string()),
                        Field::Int(0),
                        Field::String("UK".to_string()),
                        Field::Float(OrderedFloat(1.1)),
                    ]),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10004),
                        Field::String("Eve".to_string()),
                        Field::Int(1),
                        Field::String("SG".to_string()),
                        Field::Float(OrderedFloat(1.1)),
                    ]),
                },
                USER_PORT,
            ),
            // (
            //     Operation::Delete {
            //         old: Record::new(
            //             None,
            //             vec![
            //                 Field::Int(10002),
            //                 Field::String("Craig".to_string()),
            //                 Field::Int(1),
            //                 Field::Float(OrderedFloat(1.1)),
            //             ],
            //         ),
            //     },
            //     USER_PORT,
            // ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::Int(10005),
                        Field::String("Frank".to_string()),
                        Field::Int(1),
                        Field::String("SG".to_string()),
                        Field::Float(OrderedFloat(1.5)),
                    ]),
                },
                USER_PORT,
            ),
            (
                Operation::Update {
                    old: Record::new(vec![Field::Int(0), Field::String("IT".to_string())]),
                    new: Record::new(vec![Field::Int(0), Field::String("RD".to_string())]),
                },
                DEPARTMENT_PORT,
            ),
            // (
            //     Operation::Update {
            //         old: Record::new(
            //             None,
            //             vec![Field::Int(0), Field::String("IT".to_string())],
            //         ),
            //         new: Record::new(
            //             None,
            //             vec![Field::Int(0), Field::String("XX".to_string())],
            //         ),
            //     },
            //     DEPARTMENT_PORT,
            // ),
        ];

        for (index, (op, port)) in operations.into_iter().enumerate() {
            // match operation.1.clone().0 {
            //     Operation::Delete { old } => {
            //         info!("s{}: - {:?}", operation.1.clone().1, old.values)
            //     }
            //     Operation::Insert { new } => {
            //         info!("s{}: + {:?}", operation.1.clone().1, new.values)
            //     }
            //     Operation::Update { old, new } => {
            //         info!(
            //             "s{}: - {:?}, + {:?}",
            //             operation.1.clone().1,
            //             old.values,
            //             new.values
            //         )
            //     }
            // }
            fw.send(IngestionMessage::new_op(index as u64, 0, 0, op), port)
                .unwrap();
        }

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            // thread::sleep(Duration::from_millis(500));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TestSinkFactory {
    expected: u64,
    running: Arc<AtomicBool>,
}

impl TestSinkFactory {
    pub fn new(expected: u64, barrier: Arc<AtomicBool>) -> Self {
        Self {
            expected,
            running: barrier,
        }
    }
}

impl SinkFactory<SchemaSQLContext> for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, BoxedError> {
        Ok(Box::new(TestSink {
            expected: self.expected,
            current: 0,
            running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[test]
#[ignore]
fn test_pipeline_builder() {
    dozer_tracing::init_telemetry(None, None);

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        "SELECT  name, dname, salary \
        FROM user JOIN department ON user.department_id = department.did JOIN country ON user.country_id = country.cid ",
        &mut pipeline,
        Some("results".to_string()),
    )
        .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(TestSourceFactory::new(latch.clone())),
        AppSourceMappings::new(
            "conn".to_string(),
            vec![
                ("user".to_string(), USER_PORT),
                ("department".to_string(), DEPARTMENT_PORT),
                ("country".to_string(), COUNTRY_PORT),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();

    pipeline.add_sink(Box::new(TestSinkFactory::new(8, latch)), "sink", None);
    pipeline.connect_nodes(
        &table_info.node,
        Some(table_info.port),
        "sink",
        Some(DEFAULT_PORT_HANDLE),
    );

    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.into_dag().unwrap();

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
