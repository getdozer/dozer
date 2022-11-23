use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::{Endpoint, NodeType};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::ExecutorOptions;
use dozer_core::dag::executor_local::{MultiThreadedDagExecutor, DEFAULT_PORT_HANDLE};
use dozer_core::dag::node::{
    PortHandle, StatelessSink, StatelessSinkFactory, StatelessSource, StatelessSourceFactory,
};

use dozer_sql::pipeline::builder::PipelineBuilder;

use dozer_types::crossbeam::channel::{unbounded, Sender};
use dozer_types::log::debug;
use dozer_types::types::{Operation, Schema};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use std::{fs, thread};
use tempdir::TempDir;

use super::helper::get_table_create_sql;
use super::SqlMapper;

pub struct TestSourceFactory {
    output_ports: Vec<PortHandle>,
    ops: Vec<Operation>,
    schema: Schema,
}

impl TestSourceFactory {
    pub fn new(schema: Schema, ops: Vec<Operation>) -> Self {
        Self {
            output_ports: vec![DEFAULT_PORT_HANDLE],
            schema,
            ops,
        }
    }
}

impl StatelessSourceFactory for TestSourceFactory {
    fn get_output_ports(&self) -> Vec<PortHandle> {
        self.output_ports.clone()
    }
    fn build(&self) -> Box<dyn StatelessSource> {
        Box::new(TestSource {
            schema: self.schema.to_owned(),
            ops: self.ops.to_owned(),
        })
    }
}

pub struct TestSource {
    schema: Schema,
    ops: Vec<Operation>,
}

impl StatelessSource for TestSource {
    fn get_output_schema(&self, _port: PortHandle) -> Option<Schema> {
        Some(self.schema.to_owned())
    }

    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<u64>,
    ) -> Result<(), ExecutionError> {
        let mut idx = 0;
        for op in self.ops.iter().cloned() {
            idx += 1;
            fw.send(idx, op, DEFAULT_PORT_HANDLE).unwrap();
        }
        fw.terminate().unwrap();
        Ok(())
    }
}

pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
    mapper: Arc<Mutex<SqlMapper>>,
    tx: Sender<Schema>,
}

impl TestSinkFactory {
    pub fn new(mapper: Arc<Mutex<SqlMapper>>, tx: Sender<Schema>) -> Self {
        Self {
            input_ports: vec![DEFAULT_PORT_HANDLE],
            mapper,
            tx,
        }
    }
}

impl StatelessSinkFactory for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(&self) -> Box<dyn StatelessSink> {
        Box::new(TestSink::new(self.mapper.clone(), self.tx.clone()))
    }
}

pub struct TestSink {
    mapper: Arc<Mutex<SqlMapper>>,
    input_schemas: HashMap<PortHandle, Schema>,
    tx: Sender<Schema>,
}

impl TestSink {
    pub fn new(mapper: Arc<Mutex<SqlMapper>>, tx: Sender<Schema>) -> Self {
        Self {
            mapper,
            input_schemas: HashMap::new(),
            tx,
        }
    }
}

impl StatelessSink for TestSink {
    fn update_schema(
        &mut self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        self.input_schemas = input_schemas.to_owned();

        for (_port, schema) in input_schemas.iter() {
            self.tx.send(schema.clone()).unwrap();
            self.mapper
                .lock()
                .unwrap()
                .create_tables(vec![(
                    "results",
                    &get_table_create_sql("results", schema.to_owned()),
                )])
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        }
        Ok(())
    }

    fn init(&mut self) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        op: Operation,
    ) -> Result<(), ExecutionError> {
        let sql = self
            .mapper
            .lock()
            .unwrap()
            .map_operation_to_sql(&"results".to_string(), op)?;

        self.mapper
            .lock()
            .unwrap()
            .execute_list(vec![("results", sql)])
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        Ok(())
    }
}

pub struct TestPipeline {
    sql: String,
    schema: Schema,
    ops: Vec<Operation>,
    mapper: Arc<Mutex<SqlMapper>>,
}

impl TestPipeline {
    pub fn new(
        sql: String,
        schema: Schema,
        ops: Vec<Operation>,
        mapper: Arc<Mutex<SqlMapper>>,
    ) -> Self {
        Self {
            sql,
            schema,
            ops,
            mapper,
        }
    }
    pub fn run(&mut self) -> Result<Schema, ExecutionError> {
        let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...

        let (tx, rx) = unbounded::<Schema>();

        let ast = Parser::parse_sql(&dialect, &self.sql).unwrap();

        let statement: &Statement = &ast[0];

        let builder = PipelineBuilder {};
        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let source = TestSourceFactory::new(self.schema.clone(), self.ops.to_owned());
        let sink = TestSinkFactory::new(self.mapper.clone(), tx);

        dag.add_node(
            NodeType::StatelessSource(Box::new(source)),
            "source".to_string(),
        );
        dag.add_node(NodeType::StatelessSink(Box::new(sink)), "sink".to_string());

        for (_table_name, endpoint) in in_handle.into_iter() {
            dag.connect(
                Endpoint::new("source".to_string(), DEFAULT_PORT_HANDLE),
                Endpoint::new(endpoint.node, endpoint.port),
            )
            .unwrap();
        }

        dag.connect(
            Endpoint::new(out_handle.node, out_handle.port),
            Endpoint::new("sink".to_string(), DEFAULT_PORT_HANDLE),
        )
        .unwrap();

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
        if tmp_dir.path().exists() {
            fs::remove_dir_all(tmp_dir.path())
                .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
        }
        fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let exec =
            MultiThreadedDagExecutor::start(dag, tmp_dir.into_path(), ExecutorOptions::default())?;

        match rx.recv() {
            Ok(schema) => {
                let (tx, rx) = mpsc::channel();
                let _ = thread::spawn(move || -> Result<(), ExecutionError> {
                    let result = exec.join().map_err(|errors| {
                        let str = errors
                            .iter()
                            .map(|e| e.to_string())
                            .collect::<Vec<String>>()
                            .join(",");
                        ExecutionError::InternalStringError(str)
                    });
                    match tx.send(result) {
                        Ok(()) => Ok(()),
                        Err(_) => Err(ExecutionError::InternalStringError(
                            "Disconnected".to_string(),
                        )),
                    }
                });

                match rx.recv_timeout(Duration::from_millis(2000)) {
                    Ok(_) => Ok(schema),
                    Err(RecvTimeoutError::Timeout) => Err(ExecutionError::InternalStringError(
                        "Pipeline timed out".to_string(),
                    )),
                    Err(RecvTimeoutError::Disconnected) => unreachable!(),
                }
            }
            Err(_e) => Err(ExecutionError::InternalStringError(
                "Schema not sent in the pipeline".to_string(),
            )),
        }
    }
}
