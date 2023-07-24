use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};

use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::epoch::Epoch;
use dozer_types::errors::internal::BoxedError;
use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::tracing::{debug, info};
use dozer_types::types::{
    Field, FieldDefinition, FieldType, ProcessorOperation, ProcessorRecord, Record, Schema,
    SourceDefinition,
};

use dozer_types::types::ref_types::ProcessorRecordRef;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};

const TRIPS_PORT: u16 = 0 as PortHandle;
const ZONES_PORT: u16 = 1 as PortHandle;
const EXPECTED_SINK_OP_COUNT: u64 = 12;
const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[test]
#[ignore]
fn test_lifetime_pipeline() {
    dozer_tracing::init_telemetry(None, None);

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        "SELECT trips.taxi_id, puz.zone, trips.completed_at \
                FROM TTL(taxi_trips, completed_at, '3 MINUTES') trips \
                JOIN zones puz ON trips.pu_location_id = puz.location_id",
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
        vec![
            ("taxi_trips".to_string(), TRIPS_PORT),
            ("zones".to_string(), ZONES_PORT),
        ]
        .into_iter()
        .collect(),
    ))
    .unwrap();

    pipeline.add_sink(
        Arc::new(TestSinkFactory::new(EXPECTED_SINK_OP_COUNT, latch)),
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

    dag.print_dot();

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
            OutputPortDef::new(TRIPS_PORT, OutputPortType::Stateless),
            OutputPortDef::new(ZONES_PORT, OutputPortType::Stateless),
        ]
    }

    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        if port == &TRIPS_PORT {
            let taxi_trips_source = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "taxi_trips".to_string(),
            };
            Ok((
                Schema::default()
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
        } else if port == &ZONES_PORT {
            let source_id = SourceDefinition::Table {
                connection: "connection".to_string(),
                name: "zones".to_string(),
            };
            Ok((
                Schema::default()
                    .field(
                        FieldDefinition::new(
                            String::from("location_id"),
                            FieldType::UInt,
                            false,
                            source_id.clone(),
                        ),
                        true,
                    )
                    .field(
                        FieldDefinition::new(
                            String::from("zone"),
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
            _running: self.running.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct TestSource {
    _running: Arc<AtomicBool>,
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
        let operations = vec![
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1001),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:00:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(1),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1002),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:01:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1003),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:02:10", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(3),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1004),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:03:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1005),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:05:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(1),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1006),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:06:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]))),
                },
                TRIPS_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(1),
                        Field::String("Newark Airport".to_string()),
                    ]))),
                },
                ZONES_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(2),
                        Field::String("Jamaica Bay".to_string()),
                    ]))),
                },
                ZONES_PORT,
            ),
            (
                ProcessorOperation::Insert {
                    new: ProcessorRecordRef::new(ProcessorRecord::from(Record::new(vec![
                        Field::UInt(3),
                        Field::String("Allerton/Pelham Gardens".to_string()),
                    ]))),
                },
                ZONES_PORT,
            ),
        ];

        for (index, (op, port)) in operations.into_iter().enumerate() {
            fw.send(
                IngestionMessage::new_op(index as u64, 0, 0, op.clone_deref()),
                port,
            )
            .unwrap();
            //thread::sleep(Duration::from_millis(500));
        }

        thread::sleep(Duration::from_millis(500));

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
            _expected: self.expected,
            _current: 0,
            _running: self.running.clone(),
        }))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), BoxedError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct TestSink {
    _expected: u64,
    _current: u64,
    _running: Arc<AtomicBool>,
}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        match _op {
            ProcessorOperation::Delete { old } => {
                info!("o0:-> - {:?}", old.get_record().get_fields())
            }
            ProcessorOperation::Insert { new } => {
                info!("o0:-> + {:?}", new.get_record().get_fields())
            }
            ProcessorOperation::Update { old, new } => {
                info!(
                    "o0:-> - {:?}, + {:?}",
                    old.get_record().get_fields(),
                    new.get_record().get_fields()
                )
            }
        }

        Ok(())
    }

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}
