use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::{Endpoint, NodeType, DEFAULT_PORT_HANDLE};
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    NodeHandle, OutputPortDef, OutputPortDefOptions, PortHandle, Sink, SinkFactory, Source,
    SourceFactory,
};

use dozer_sql::pipeline::builder::PipelineBuilder;

use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::crossbeam::channel::{unbounded, Sender};
use dozer_types::log::debug;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use fp_rust::sync::CountDownLatch;
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
    term_latch: Arc<CountDownLatch>,
}

impl TestSourceFactory {
    pub fn new(schema: Schema, ops: Vec<Operation>, term_latch: Arc<CountDownLatch>) -> Self {
        Self {
            output_ports: vec![DEFAULT_PORT_HANDLE],
            schema,
            ops,
            term_latch,
        }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(self.schema.clone())
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortDefOptions::default()))
            .collect()
    }
    fn build(
        &self,
        output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(TestSource {
            schema: self.schema.to_owned(),
            ops: self.ops.to_owned(),
            term_latch: self.term_latch.clone(),
        }))
    }
}

pub struct TestSource {
    schema: Schema,
    ops: Vec<Operation>,
    term_latch: Arc<CountDownLatch>,
}

impl Source for TestSource {
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

        thread::sleep(Duration::from_millis(1000));
        Ok(())
    }
}

pub struct SchemaHolder {
    pub schema: Option<Schema>,
}

pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
    mapper: Arc<Mutex<SqlMapper>>,
    schema: Arc<RwLock<SchemaHolder>>,
    term_latch: Arc<CountDownLatch>,
    ops: usize,
}

impl TestSinkFactory {
    pub fn new(
        mapper: Arc<Mutex<SqlMapper>>,
        schema: Arc<RwLock<SchemaHolder>>,
        term_latch: Arc<CountDownLatch>,
        ops: usize,
    ) -> Self {
        Self {
            input_ports: vec![DEFAULT_PORT_HANDLE],
            mapper,
            schema,
            term_latch,
            ops,
        }
    }
}

impl SinkFactory for TestSinkFactory {
    fn set_input_schema(
        &self,
        output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        let schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap().clone();
        self.schema.write().schema = Some(schema.clone());
        self.mapper
            .lock()
            .unwrap()
            .create_tables(vec![(
                "results",
                &get_table_create_sql("results", schema.to_owned()),
            )])
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }
    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(TestSink::new(
            self.mapper.clone(),
            input_schemas,
            self.term_latch.clone(),
            self.ops,
        )))
    }
}

pub struct TestSink {
    mapper: Arc<Mutex<SqlMapper>>,
    input_schemas: HashMap<PortHandle, Schema>,
    term_latch: Arc<CountDownLatch>,
    ops: usize,
    curr: usize,
}

impl TestSink {
    pub fn new(
        mapper: Arc<Mutex<SqlMapper>>,
        input_schemas: HashMap<PortHandle, Schema>,
        term_latch: Arc<CountDownLatch>,
        ops: usize,
    ) -> Self {
        Self {
            mapper,
            input_schemas,
            term_latch,
            ops,
            curr: 0,
        }
    }
}

impl Sink for TestSink {
    fn init(&mut self, _env: &mut dyn Environment) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        _seq: u64,
        op: Operation,
        _state: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        let sql = self
            .mapper
            .lock()
            .unwrap()
            .map_operation_to_sql(&"results".to_string(), op)?;

        self.mapper
            .lock()
            .unwrap()
            .execute_list(vec![("results".to_string(), sql)])
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        self.curr += 1;
        if self.curr == self.ops {
            self.term_latch.countdown();
        }

        Ok(())
    }

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
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
        let latch = Arc::new(CountDownLatch::new(1));

        let builder = PipelineBuilder::new(None);
        let (mut dag, in_handle, out_handle) =
            builder.statement_to_pipeline(statement.clone()).unwrap();

        let source_handle = NodeHandle::new(None, "source".to_string());
        let sink_handle = NodeHandle::new(None, "sink".to_string());

        let schema_holder: Arc<RwLock<SchemaHolder>> =
            Arc::new(RwLock::new(SchemaHolder { schema: None }));

        let ops_count = self.ops.len();

        let source =
            TestSourceFactory::new(self.schema.clone(), self.ops.to_owned(), latch.clone());
        let sink = TestSinkFactory::new(
            self.mapper.clone(),
            schema_holder.clone(),
            latch.clone(),
            ops_count,
        );

        dag.add_node(NodeType::Source(Arc::new(source)), source_handle.clone());
        dag.add_node(NodeType::Sink(Arc::new(sink)), sink_handle.clone());

        for (_table_name, endpoint) in in_handle.into_iter() {
            dag.connect(
                Endpoint::new(source_handle.clone(), DEFAULT_PORT_HANDLE),
                Endpoint::new(endpoint.node, endpoint.port),
            )
            .unwrap();
        }

        dag.connect(
            Endpoint::new(out_handle.node, out_handle.port),
            Endpoint::new(sink_handle.clone(), DEFAULT_PORT_HANDLE),
        )
        .unwrap();

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
        // if tmp_dir.path().exists() {
        //     fs::remove_dir_all(tmp_dir.path())
        //         .unwrap_or_else(|_e| panic!("Unable to remove old dir"));
        // }
        // fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let mut exec = DagExecutor::new(&dag, tmp_dir.path(), ExecutorOptions::default())
            .unwrap_or_else(|_e| panic!("Unable to create exec"));
        exec.start()?;
        exec.join()?;

        let r_schema = schema_holder.read().schema.as_ref().unwrap().clone();

        Ok(r_schema)

        // match rx.recv() {
        //     Ok(schema) => {
        //         let (tx, rx) = mpsc::channel();
        //         let _ = thread::spawn(move || -> Result<(), ExecutionError> {
        //             let result = exec.join();
        //             match tx.send(result) {
        //                 Ok(()) => Ok(()),
        //                 Err(_) => Err(ExecutionError::InternalStringError(
        //                     "Disconnected".to_string(),
        //                 )),
        //             }
        //         });
        //
        //         match rx.recv_timeout(Duration::from_millis(2000)) {
        //             Ok(_) => Ok(schema),
        //             Err(RecvTimeoutError::Timeout) => Err(ExecutionError::InternalStringError(
        //                 "Pipeline timed out".to_string(),
        //             )),
        //             Err(RecvTimeoutError::Disconnected) => unreachable!(),
        //         }
        //     }
        //     Err(_e) => Err(ExecutionError::InternalStringError(
        //         "Schema not sent in the pipeline".to_string(),
        //     )),
        // }
    }
}
