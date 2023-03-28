use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::errors::ExecutionError;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::ingestion_types::IngestionMessage;
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

const ACTOR: u16 = 0 as PortHandle;
const FILM_ACTOR: u16 = 1 as PortHandle;

#[test]
#[ignore]
fn test_pipeline_builder() {
    dozer_tracing::init_telemetry(None, None);

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        // "SELECT a.actor_id, a.first_name, a.last_name, fa.film_id from actor a
        // LEFT JOIN film_actor fa on fa.actor_id = a.actor_id;",
        "SELECT a.first_name, a.last_name
                FROM actor a
                INNER JOIN (
                SELECT actor_id
                FROM film_actor
                GROUP BY actor_id
                HAVING COUNT(*) >= 10
                ) fa ON a.actor_id = fa.actor_id",
        &mut pipeline,
        Some("results".to_string()),
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();
    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "conn".to_string(),
        Arc::new(TestSourceFactory::new(latch.clone())),
        vec![
            ("actor".to_string(), ACTOR),
            ("film_actor".to_string(), FILM_ACTOR),
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
    dag.print_dot();

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
        dag,
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
            OutputPortDef::new(ACTOR, OutputPortType::Stateless),
            OutputPortDef::new(FILM_ACTOR, OutputPortType::Stateless),
        ]
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        if port == &ACTOR {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "actor".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("actor_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("first_name"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("last_name"),
                            FieldType::String,
                            false,
                            source_id.clone(),
                        ),
                        false,
                    )
                    .clone(),
                SchemaSQLContext::default(),
            ))
        } else if port == &FILM_ACTOR {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "film_actor".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("actor_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("film_id"),
                            FieldType::Int,
                            false,
                            source_id.clone(),
                        ),
                        true,
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
}

#[derive(Debug)]
pub struct TestSource {
    running: Arc<AtomicBool>,
}

impl Source for TestSource {
    fn can_start_from(&self, _last_checkpoint: (u64, u64)) -> Result<bool, ExecutionError> {
        Ok(false)
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _last_checkpoint: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let operations = vec![
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(1),
                            Field::String("Penelope".to_string()),
                            Field::String("Guiness".to_string()),
                        ],
                        Some(1),
                    ),
                },
                ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(1),
                            Field::String("Penelope".to_string()),
                            Field::String("Guiness".to_string()),
                        ],
                        Some(1),
                    ),
                },
                ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(2),
                            Field::String("Jack".to_string()),
                            Field::String("Nicholson".to_string()),
                        ],
                        Some(1),
                    ),
                },
                ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(3),
                            Field::String("Angelina".to_string()),
                            Field::String("Jolie".to_string()),
                        ],
                        Some(1),
                    ),
                },
                ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(4),
                            Field::String("Tom".to_string()),
                            Field::String("Hanks".to_string()),
                        ],
                        Some(1),
                    ),
                },
                ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(1), Field::Int(1)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(1), Field::Int(2)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(1), Field::Int(3)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(2), Field::Int(4)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(2), Field::Int(5)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(2), Field::Int(6)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(3), Field::Int(7)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(3), Field::Int(8)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(3), Field::Int(9)], Some(1)),
                },
                FILM_ACTOR,
            ),
            (
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(4), Field::Int(10)], Some(1)),
                },
                FILM_ACTOR,
            ),
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
            fw.send(IngestionMessage::new_op(index as u64, 0, op), port)
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
    fn process(&mut self, _from_port: PortHandle, _op: Operation) -> Result<(), ExecutionError> {
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

    fn commit(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}
