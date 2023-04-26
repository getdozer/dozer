use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::errors::ExecutionError;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};

use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::tracing::{debug, info};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};

const TRIPS_PORT: u16 = 0 as PortHandle;
const EXPECTED_SINK_OP_COUNT: u64 = 12;
const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[test]
#[ignore]
fn test_pipeline_builder() {
    let _ = dozer_tracing::init_telemetry(None, None);

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        "SELECT trips.taxi_id, trips.completed_at \
        FROM TTL(taxi_trips, '32 SECONDS') trips ",
        &mut pipeline,
        Some("results".to_string()),
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "connection".to_string(),
        Arc::new(TestSourceFactory::new(latch.clone())),
        vec![("taxi_trips".to_string(), TRIPS_PORT)]
            .into_iter()
            .collect(),
    ))
    .unwrap();

    pipeline.add_sink(
        Arc::new(TestSinkFactory::new(EXPECTED_SINK_OP_COUNT, latch)),
        "sink",
    );
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
        vec![OutputPortDef::new(TRIPS_PORT, OutputPortType::Stateless)]
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        if port == &TRIPS_PORT {
            let taxi_trips_source = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "taxi_trips".to_string(),
            };
            Ok((
                Schema::empty()
                    .field(
                        FieldDefinition::new(
                            String::from("taxi_id"),
                            FieldType::UInt,
                            false,
                            taxi_trips_source.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("completed_at"),
                            FieldType::Timestamp,
                            false,
                            taxi_trips_source.clone(),
                        ),
                        false,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("pu_location_id"),
                            FieldType::UInt,
                            false,
                            taxi_trips_source,
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
                            Field::UInt(1001),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:00:00", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(1),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::UInt(1002),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:01:00", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(2),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::UInt(1003),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:02:10", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(3),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::UInt(1004),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:03:00", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(2),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::UInt(1005),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:05:00", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(1),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::UInt(1006),
                            Field::Timestamp(
                                Utc.datetime_from_str("2023-02-01 22:06:00", DATE_FORMAT)
                                    .unwrap()
                                    .into(),
                            ),
                            Field::UInt(2),
                        ],
                    ),
                },
                TRIPS_PORT,
            ),
        ];

        for (index, (op, port)) in operations.into_iter().enumerate() {
            fw.send(IngestionMessage::new_op(index as u64, 0, op), port)
                .unwrap();
        }

        loop {
            if !self.running.load(Ordering::Relaxed) {
                break;
            }
            thread::sleep(Duration::from_millis(500));
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
