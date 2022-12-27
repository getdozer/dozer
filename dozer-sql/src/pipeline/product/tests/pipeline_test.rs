use crate::pipeline::builder::PipelineBuilder;
use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::{Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::node::{
    NodeHandle, OutputPortDef, OutputPortDefOptions, PortHandle, Sink, SinkFactory, Source,
    SourceFactory,
};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tempdir::TempDir;

/// Test Source
pub struct UserTestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl UserTestSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for UserTestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new(String::from("id"), FieldType::Int, false),
                false,
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
        Ok(Box::new(UserTestSource {}))
    }
}

pub struct UserTestSource {}

impl Source for UserTestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000 {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(0),
                            Field::String("Alice".to_string()),
                            Field::Int(0),
                            Field::Float(OrderedFloat(5.5)),
                        ],
                    ),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        Ok(())
    }
}

/// Test Source
pub struct DepartmentTestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl DepartmentTestSourceFactory {
    pub fn new(output_ports: Vec<PortHandle>) -> Self {
        Self { output_ports }
    }
}

impl SourceFactory for DepartmentTestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(DepartmentTestSource {}))
    }

    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(Schema::empty()
            .field(
                FieldDefinition::new(String::from("id"), FieldType::Int, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("name"), FieldType::String, false),
                false,
            )
            .clone())
    }
}

pub struct DepartmentTestSource {}

impl Source for DepartmentTestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000 {
            fw.send(
                n,
                0,
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(0), Field::String("IT".to_string())]),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        Ok(())
    }
}

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
        Ok(Box::new(TestSink {}))
    }
}

pub struct TestSink {}

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
        Ok(())
    }

    fn commit(
        &mut self,
        _source: &NodeHandle,
        _txid: u64,
        _seq_in_tx: u64,
        _tx: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[test]
fn test_single_table_pipeline() {
    let sql = "SELECT conn.Users.name, d.name FROM Users u JOIN Department d ON u.department_id = d.id WHERE u.salary>1";

    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

    let ast = Parser::parse_sql(&dialect, sql).unwrap();

    let statement: &Statement = &ast[0];

    let builder = PipelineBuilder::new(Some(1));
    let (mut dag, mut in_handle, out_handle) =
        builder.statement_to_pipeline(statement.clone()).unwrap();

    let user_source = UserTestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);

    let department_source = DepartmentTestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);

    let sink = TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE]);

    dag.add_node(
        NodeType::Source(Arc::new(user_source)),
        NodeHandle::new(Some(1), String::from("users")),
    );

    dag.add_node(
        NodeType::Source(Arc::new(department_source)),
        NodeHandle::new(Some(1), String::from("department")),
    );

    dag.add_node(
        NodeType::Sink(Arc::new(sink)),
        NodeHandle::new(Some(1), String::from("sink")),
    );

    let users_input = in_handle.remove("users").unwrap();

    let _users_to_dag = dag.connect(
        Endpoint::new(
            NodeHandle::new(Some(1), String::from("users")),
            DEFAULT_PORT_HANDLE,
        ),
        Endpoint::new(users_input.node, users_input.port),
    );

    let department_input = in_handle.remove("department").unwrap();

    let _department_to_dag = dag.connect(
        Endpoint::new(
            NodeHandle::new(Some(1), String::from("department")),
            DEFAULT_PORT_HANDLE,
        ),
        Endpoint::new(department_input.node, department_input.port),
    );

    let _output_to_sink = dag.connect(
        Endpoint::new(out_handle.node, out_handle.port),
        Endpoint::new(
            NodeHandle::new(Some(1), String::from("sink")),
            DEFAULT_PORT_HANDLE,
        ),
    );

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

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
