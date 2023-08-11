use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSourceManager, AppSourceMappings};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::executor_operation::ProcessorOperation;
use dozer_core::node::{OutputPortDef, OutputPortType, PortHandle, Sink, Source, SourceFactory};

use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::chrono::{TimeZone, Utc};
use dozer_types::errors::internal::BoxedError;
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

use crate::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use crate::pipeline::product::tests::pipeline_test::TestSinkFactory;

const TRIPS_PORT: u16 = 0 as PortHandle;
const ZONES_PORT: u16 = 1 as PortHandle;
const EXPECTED_SINK_OP_COUNT: u64 = 12;
const DATE_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

#[test]
#[ignore]
fn test_pipeline_builder() {
    dozer_tracing::init_telemetry(None, None);

    let mut pipeline = AppPipeline::new();

    let context = statement_to_pipeline(
        // "SELECT trips.taxi_id, puz.zone, trips.completed_at, trips.window_start, trips.window_end \
        // FROM HOP(taxi_trips, completed_at, '1 MINUTE', '2 MINUTES') trips \
        // JOIN zones puz ON trips.pu_location_id = puz.location_id",
        "SELECT trips.taxi_id, trips.completed_at, trips.window_start, trips.window_end \
        FROM HOP(taxi_trips, completed_at, '1 MINUTE', '2 MINUTES') trips ",
        &mut pipeline,
        Some("results".to_string()),
        &vec![],
    )
    .unwrap();

    let table_info = context.output_tables_map.get("results").unwrap();

    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(
        Box::new(TestSourceFactory::new(latch.clone())),
        AppSourceMappings::new(
            "connection".to_string(),
            vec![
                ("taxi_trips".to_string(), TRIPS_PORT),
                ("zones".to_string(), ZONES_PORT),
            ]
            .into_iter()
            .collect(),
        ),
    )
    .unwrap();

    pipeline.add_sink(
        Box::new(TestSinkFactory::new(EXPECTED_SINK_OP_COUNT, latch)),
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

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        match *port {
            TRIPS_PORT => "trips".to_string(),
            ZONES_PORT => "zones".to_string(),
            _ => panic!("Invalid Port Handle {port}"),
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
        let operations = vec![
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1001),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:00:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(1),
                    ]),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1002),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:01:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1003),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:02:10", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(3),
                    ]),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1004),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:03:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1005),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:05:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(1),
                    ]),
                },
                TRIPS_PORT,
            ),
            (
                Operation::Insert {
                    new: Record::new(vec![
                        Field::UInt(1006),
                        Field::Timestamp(
                            Utc.datetime_from_str("2023-02-01 22:06:00", DATE_FORMAT)
                                .unwrap()
                                .into(),
                        ),
                        Field::UInt(2),
                    ]),
                },
                TRIPS_PORT,
            ),
            // (
            //     Operation::Insert {
            //         new: Record::new(
            //             None,
            //             vec![Field::UInt(1), Field::String("Newark Airport".to_string())],
            //         ),
            //     },
            //     ZONES_PORT,
            // ),
            // (
            //     Operation::Insert {
            //         new: Record::new(
            //             None,
            //             vec![Field::UInt(2), Field::String("Jamaica Bay".to_string())],
            //         ),
            //     },
            //     ZONES_PORT,
            // ),
            // (
            //     Operation::Insert {
            //         new: Record::new(
            //             None,
            //             vec![
            //                 Field::UInt(3),
            //                 Field::String("Allerton/Pelham Gardens".to_string()),
            //             ],
            //         ),
            //     },
            //     ZONES_PORT,
            // ),
        ];

        for (index, (op, port)) in operations.into_iter().enumerate() {
            fw.send(IngestionMessage::new_op(index as u64, 0, 0, op), port)
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
pub struct TestSink {
    pub expected: u64,
    pub current: u64,
    pub running: Arc<AtomicBool>,
}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        record_store: &ProcessorRecordStore,
        _op: ProcessorOperation,
    ) -> Result<(), BoxedError> {
        match _op {
            ProcessorOperation::Delete { old } => {
                info!(
                    "o0:-> - {:?}",
                    record_store.load_record(&old).unwrap().values
                )
            }
            ProcessorOperation::Insert { new } => {
                info!(
                    "o0:-> + {:?}",
                    record_store.load_record(&new).unwrap().values
                )
            }
            ProcessorOperation::Update { old, new } => {
                info!(
                    "o0:-> - {:?}, + {:?}",
                    record_store.load_record(&old).unwrap().values,
                    record_store.load_record(&new).unwrap().values
                )
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

    fn commit(&mut self, _epoch_details: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self, _connection_name: String) -> Result<(), BoxedError> {
        Ok(())
    }
}
