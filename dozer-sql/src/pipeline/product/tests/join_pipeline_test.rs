use crate::pipeline::builder::{get_select, PipelineBuilder};
use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::ExecutorOptions;
use dozer_core::dag::executor_local::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_core::dag::node::{
    OutputPortDef, OutputPortDefOptions, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_core::storage::record_reader::RecordReader;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use log::debug;
use std::collections::HashMap;
use std::fs;
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
    fn build(&self) -> Box<dyn Source> {
        Box::new(UserTestSource {})
    }
}

pub struct UserTestSource {}

impl Source for UserTestSource {
    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        Some(
            Schema::empty()
                .field(
                    FieldDefinition::new(String::from("id"), FieldType::Int, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("name"), FieldType::String, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("salary"), FieldType::Float, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("department_id"), FieldType::Int, false),
                    false,
                    false,
                )
                .clone(),
        )
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(
                        None,
                        vec![
                            Field::Int(0),
                            Field::String("Alice".to_string()),
                            Field::Float(OrderedFloat(1000.0)),
                            Field::Int(0),
                        ],
                    ),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        fw.terminate().unwrap();
        Ok(())
    }
}

/// Test Source
pub struct DepartmentTestSourceFactory {
    output_ports: Vec<PortHandle>,
}

impl SourceFactory for DepartmentTestSourceFactory {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }
    fn build(&self) -> Box<dyn Source> {
        Box::new(DepartmentTestSource {})
    }
}

pub struct DepartmentTestSource {}

impl Source for DepartmentTestSource {
    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        Some(
            Schema::empty()
                .field(
                    FieldDefinition::new(String::from("id"), FieldType::Int, false),
                    false,
                    false,
                )
                .field(
                    FieldDefinition::new(String::from("name"), FieldType::String, false),
                    false,
                    false,
                )
                .clone(),
        )
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        for n in 0..10000 {
            fw.send(
                n,
                Operation::Insert {
                    new: Record::new(None, vec![Field::Int(0), Field::String("IT".to_string())]),
                },
                DEFAULT_PORT_HANDLE,
            )
            .unwrap();
        }
        fw.terminate().unwrap();
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
    fn build(&self) -> Box<dyn Sink> {
        Box::new(TestSink {})
    }
}

pub struct TestSink {}

impl Sink for TestSink {
    fn update_schema(
        &mut self,
        _input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn init(&mut self, _env: &mut dyn Environment) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        _op: Operation,
        _state: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }
}

#[test]
fn test_single_table_pipeline() {
    let sql = "SELECT u.name, d.name, AVG(salary) \
    FROM Users u JOIN Departments d ON u.department_id = d.id \
    WHERE salary >= 100 GROUP BY department_id";
    let (mut dag, mut in_handle, out_handle) = PipelineBuilder {}
        .build(sql)
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let dag_user_input = in_handle.remove("users").unwrap();
    let user_source = UserTestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);
    dag.add_node(NodeType::Source(Box::new(user_source)), "users".to_string());
    let _users_source_to_dag = dag.connect(
        Endpoint::new("users".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(dag_user_input.node, dag_user_input.port),
    );

    let dag_department_input = in_handle.remove("departments").unwrap();
    let department_source = DepartmentTestSourceFactory::new(vec![DEFAULT_PORT_HANDLE]);
    dag.add_node(
        NodeType::Source(Box::new(department_source)),
        "departments".to_string(),
    );
    let _departments_source_to_dag = dag.connect(
        Endpoint::new("departments".to_string(), DEFAULT_PORT_HANDLE),
        Endpoint::new(dag_department_input.node, dag_department_input.port),
    );

    let sink = TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE]);
    dag.add_node(NodeType::Sink(Box::new(sink)), "sink".to_string());
    let _dag_to_sink = dag.connect(
        Endpoint::new(out_handle.node, out_handle.port),
        Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
    );

    let tmp_dir =
        TempDir::new("test_join").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    use std::time::Instant;
    let now = Instant::now();

    let exec =
        MultiThreadedDagExecutor::start(dag, &tmp_dir.into_path(), ExecutorOptions::default())
            .unwrap();

    exec.join().unwrap();
    let elapsed = now.elapsed();
    debug!("Elapsed: {:.2?}", elapsed);
}
