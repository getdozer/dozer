use dozer_core::dag::app::App;
use dozer_core::dag::appsource::{AppSource, AppSourceManager};
use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::epoch::Epoch;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
#[cfg(not(test))]
use log::debug; // Use log crate when building application

use std::collections::HashMap;
#[cfg(test)]
use std::println as debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempdir::TempDir;

use crate::pipeline::builder::PipelineBuilder;

#[derive(Debug)]
pub struct UserTestSourceFactory {
    running: Arc<AtomicBool>,
}

impl UserTestSourceFactory {
    pub fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl SourceFactory for UserTestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_updates: true,
                retr_old_records_for_deletes: true,
            },
        )]
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new(String::from("id"), FieldType::Int, false),
                true,
            )
            .field(
                FieldDefinition::new(String::from("name"), FieldType::String, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("department_id"), FieldType::Int, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("salary"), FieldType::Float, false),
                false,
            )
            .clone())
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(UserTestSource {
            running: self.running.clone(),
        }))
    }

    fn prepare(&self, _output_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct UserTestSource {
    running: Arc<AtomicBool>,
}

impl Source for UserTestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        for n in 0..100 {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(0),
                            Field::String(format!("User {}", n)),
                            Field::Int(0),
                            Field::Float(OrderedFloat(1.0)),
                        ],
                    ),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }

        fw.send(
            10000,
            0,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(10000),
                        Field::String("Alice".to_string()),
                        Field::Int(0),
                        Field::Float(OrderedFloat(1.1)),
                    ],
                ),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

        fw.send(
            10001,
            0,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(10001),
                        Field::String("Bob".to_string()),
                        Field::Int(0),
                        Field::Float(OrderedFloat(1.1)),
                    ],
                ),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

        fw.send(
            10002,
            0,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(10002),
                        Field::String("Craig".to_string()),
                        Field::Int(1),
                        Field::Float(OrderedFloat(1.1)),
                    ],
                ),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

        fw.send(
            10003,
            0,
            Operation::Insert {
                new: Record::new(
                    None,
                    vec![
                        Field::Int(10003),
                        Field::String("Dan".to_string()),
                        Field::Int(0),
                        Field::Float(OrderedFloat(1.1)),
                    ],
                ),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

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
pub struct DepartmentTestSourceFactory {
    running: Arc<AtomicBool>,
}

impl DepartmentTestSourceFactory {
    pub fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl SourceFactory for DepartmentTestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::StatefulWithPrimaryKeyLookup {
                retr_old_records_for_updates: true,
                retr_old_records_for_deletes: true,
            },
        )]
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(DepartmentTestSource {
            running: self.running.clone(),
        }))
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new(String::from("id"), FieldType::Int, false),
                true,
            )
            .field(
                FieldDefinition::new(String::from("name"), FieldType::String, false),
                false,
            )
            .clone())
    }

    fn prepare(&self, _output_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct DepartmentTestSource {
    running: Arc<AtomicBool>,
}

impl Source for DepartmentTestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        fw.send(
            0,
            0,
            Operation::Insert {
                new: Record::new(None, vec![Field::Int(0), Field::String("IT".to_string())]),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

        fw.send(
            1,
            0,
            Operation::Insert {
                new: Record::new(None, vec![Field::Int(1), Field::String("SG".to_string())]),
            },
            DEFAULT_PORT_HANDLE,
        )
        .unwrap();

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

impl SinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn set_input_schema(
        &self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
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

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
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
    fn init(&mut self, _env: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        match _op {
            Operation::Delete { old } => debug!("<- {:?}", old.values),
            Operation::Insert { new } => debug!("-> {:?}", new.values),
            Operation::Update { old, new } => debug!("<- {:?}\n-> {:?}", old.values, new.values),
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
fn test_pipeline_builder() {
    let mut pipeline = PipelineBuilder {}
        .build_pipeline(
            // "SELECT user.name, department.name \
            //     FROM user JOIN department ON user.department_id = department.id \
            //     WHERE salary >= 1",
            "SELECT  department.name, SUM(user.salary) \
                FROM user JOIN department ON user.department_id = department.id \
                GROUP BY department.name",
        )
        .unwrap_or_else(|e| panic!("Unable to start the Executor: {}", e));

    let latch = Arc::new(AtomicBool::new(true));

    let mut asm = AppSourceManager::new();
    asm.add(AppSource::new(
        "conn1".to_string(),
        Arc::new(UserTestSourceFactory::new(latch.clone())),
        vec![("user".to_string(), DEFAULT_PORT_HANDLE)]
            .into_iter()
            .collect(),
    ));
    asm.add(AppSource::new(
        "conn2".to_string(),
        Arc::new(DepartmentTestSourceFactory::new(latch.clone())),
        vec![("department".to_string(), DEFAULT_PORT_HANDLE)]
            .into_iter()
            .collect(),
    ));
    pipeline.add_sink(Arc::new(TestSinkFactory::new(104, latch)), "sink");
    pipeline
        .connect_nodes(
            "aggregation",
            Some(DEFAULT_PORT_HANDLE),
            "sink",
            Some(DEFAULT_PORT_HANDLE),
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
    let mut executor = DagExecutor::new(
        &dag,
        tmp_dir.path(),
        ExecutorOptions::default(),
        Arc::new(AtomicBool::new(true)),
    )
    .unwrap();

    executor
        .start()
        .unwrap_or_else(|e| panic!("Unable to start the Executor: {}", e));
    assert!(executor.join().is_ok());

    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
