use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::errors::ExecutionError;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::tracing::{debug, info};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempdir::TempDir;

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};

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
    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(vec![
            OutputPortDef::new(
                USER_PORT,
                OutputPortType::StatefulWithPrimaryKeyLookup {
                    retr_old_records_for_updates: true,
                    retr_old_records_for_deletes: true,
                },
            ),
            OutputPortDef::new(DEPARTMENT_PORT, OutputPortType::Stateless),
            OutputPortDef::new(
                COUNTRY_PORT,
                OutputPortType::StatefulWithPrimaryKeyLookup {
                    retr_old_records_for_updates: true,
                    retr_old_records_for_deletes: true,
                },
            ),
        ])
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        if port == &USER_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "user".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
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
                Schema::empty()
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
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("cid"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        true,
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
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(TestSource {
            running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSource {
    running: Arc<AtomicBool>,
}

impl Source for TestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let operations = vec![
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![Field::Int(0), Field::String("IT".to_string())],
                        Some(1),
                    ),
                },
                DEPARTMENT_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![Field::Int(1), Field::String("HR".to_string())],
                        Some(1),
                    ),
                },
                DEPARTMENT_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10000),
                            Field::String("Alice".to_string()),
                            Field::Int(0),
                            Field::String("UK".to_string()),
                            Field::Float(OrderedFloat(1.1)),
                        ],
                        Some(1),
                    ),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10001),
                            Field::String("Bob".to_string()),
                            Field::Int(0),
                            Field::String("UK".to_string()),
                            Field::Float(OrderedFloat(1.1)),
                        ],
                        Some(1),
                    ),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String("UK".to_string()),
                            Field::String("United Kingdom".to_string()),
                        ],
                        Some(1),
                    ),
                },
                COUNTRY_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::String("SG".to_string()),
                            Field::String("Singapore".to_string()),
                        ],
                        Some(1),
                    ),
                },
                COUNTRY_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10002),
                            Field::String("Craig".to_string()),
                            Field::Int(1),
                            Field::String("SG".to_string()),
                            Field::Float(OrderedFloat(1.1)),
                        ],
                        Some(1),
                    ),
                },
                USER_PORT,
            ),
            // (
            //     Operation::Delete {
            //         old: Record::new(
            //             None,
            //             vec![Field::Int(1), Field::String("HR".to_string())],
            //             Some(1),
            //         ),
            //     },
            //     DEPARTMENT_PORT,
            // ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10003),
                            Field::String("Dan".to_string()),
                            Field::Int(0),
                            Field::String("UK".to_string()),
                            Field::Float(OrderedFloat(1.1)),
                        ],
                        Some(1),
                    ),
                },
                USER_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10004),
                            Field::String("Eve".to_string()),
                            Field::Int(1),
                            Field::String("SG".to_string()),
                            Field::Float(OrderedFloat(1.1)),
                        ],
                        Some(1),
                    ),
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
            //             None,
            //         ),
            //     },
            //     USER_PORT,
            // ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(10005),
                            Field::String("Frank".to_string()),
                            Field::Int(1),
                            Field::String("SG".to_string()),
                            Field::Float(OrderedFloat(1.5)),
                        ],
                        None,
                    ),
                },
                USER_PORT,
            ),
            (
                Operation::Update {
                    old: Record::new(
                        None,
                        vec![Field::Int(0), Field::String("IT".to_string())],
                        Some(1),
                    ),
                    new: Record::new(
                        None,
                        vec![Field::Int(0), Field::String("RD".to_string())],
                        Some(2),
                    ),
                },
                DEPARTMENT_PORT,
            ),
            // (
            //     Operation::Update {
            //         old: Record::new(
            //             None,
            //             vec![Field::Int(0), Field::String("IT".to_string())],
            //             None,
            //         ),
            //         new: Record::new(
            //             None,
            //             vec![Field::Int(0), Field::String("XX".to_string())],
            //             None,
            //         ),
            //     },
            //     DEPARTMENT_PORT,
            // ),
        ];

        for operation in operations.iter().enumerate() {
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
            fw.send(
                operation.0.try_into().unwrap(),
                0,
                operation.1.clone().0,
                operation.1.clone().1,
            )
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
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(TestSink {
            expected: self.expected,
            current: 0,
            running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSink {
    expected: u64,
    current: u64,
    running: Arc<AtomicBool>,
}

impl Sink for TestSink {
    fn init(&mut self, _txn: &mut LmdbExclusiveTransaction) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
    ) -> Result<(), ExecutionError> {
        match _op {
            Operation::Delete { old } => info!("o0:-> - {:?}", old.values),
            Operation::Insert { new } => info!("o0:-> + {:?}", new.values),
            Operation::Update { old, new } => {
                info!("o0:-> - {:?}, + {:?}", old.values, new.values)
            }
        }

        self.current += 1;
        if self.current == self.expected {
            debug!(
                "Received {} messages. Notifying sender to exit!",
                self.current
            );
            self.running.store(false, Ordering::Relaxed);
        }
        Ok(())
    }

    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[test]
#[ignore]
fn test_pipeline_builder() {
    dozer_tracing::init_telemetry(false).unwrap();

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        "SELECT  name, dname, salary \
        FROM user JOIN department ON user.department_id = department.did JOIN country ON user.country_id = country.cid ",
        &mut pipeline,
        Some("results".to_string())
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "conn".to_string(),
        Arc::new(TestSourceFactory::new(latch.clone())),
        vec![
            ("user".to_string(), USER_PORT),
            ("department".to_string(), DEPARTMENT_PORT),
            ("country".to_string(), COUNTRY_PORT),
        ]
        .into_iter()
        .collect(),
    ))
    .unwrap();

    pipeline.add_sink(Arc::new(TestSinkFactory::new(8, latch)), "sink");
    pipeline
        .connect_nodes(
            &table_info.node,
            Some(table_info.port),
            "sink",
            Some(DEFAULT_PORT_HANDLE),
            true,
        )
        .unwrap();

    let mut app = App::new(asm);
    app.add_pipeline(pipeline);

    let dag = app.get_dag().unwrap();

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        std::fs::remove_dir_all(tmp_dir.path())
            .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    std::fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    use std::time::Instant;
    let now = Instant::now();

    let tmp_dir = TempDir::new("test").unwrap();

    DagExecutor::new(
        &dag,
        tmp_dir.path().to_path_buf(),
        ExecutorOptions::default(),
    )
    .unwrap()
    .start(Arc::new(AtomicBool::new(true)))
    .unwrap()
    .join()
    .unwrap();

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
