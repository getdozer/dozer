use dozer_core::dag::app::App;
use dozer_core::dag::appsource::{AppSource, AppSourceManager};
use dozer_core::dag::channels::SourceChannelForwarder;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};

use dozer_core::dag::executor::{DagExecutor, ExecutorOptions};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction};
use dozer_sql::pipeline::builder::PipelineBuilder;
use dozer_types::crossbeam::channel::{bounded, Receiver, Sender};
use dozer_types::log::debug;
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Operation, Schema};
use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use dozer_core::dag::epoch::Epoch;

use std::time::Duration;
use tempdir::TempDir;

use super::helper::get_table_create_sql;
use super::SqlMapper;

#[derive(Debug)]
pub struct TestSourceFactory {
    output_ports: Vec<PortHandle>,
    ops: Vec<Operation>,
    schema: Schema,
    term_latch: Arc<Receiver<bool>>,
}

impl TestSourceFactory {
    pub fn new(schema: Schema, ops: Vec<Operation>, term_latch: Arc<Receiver<bool>>) -> Self {
        Self {
            output_ports: vec![DEFAULT_PORT_HANDLE],
            schema,
            ops,
            term_latch,
        }
    }
}

impl SourceFactory for TestSourceFactory {
    fn get_output_schema(&self, _port: &PortHandle) -> Result<Schema, ExecutionError> {
        Ok(self.schema.clone())
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(self.output_ports
            .iter()
            .map(|e| OutputPortDef::new(*e, OutputPortType::Stateless))
            .collect())
    }

    fn prepare(&self, _output_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(TestSource {
            ops: self.ops.to_owned(),
            term_latch: self.term_latch.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct TestSource {
    ops: Vec<Operation>,
    term_latch: Arc<Receiver<bool>>,
}

impl Source for TestSource {
    fn start(
        &self,
        fw: &mut dyn SourceChannelForwarder,
        _from_seq: Option<(u64, u64)>,
    ) -> Result<(), ExecutionError> {
        let mut idx = 0;
        for op in self.ops.iter().cloned() {
            idx += 1;
            fw.send(idx, 0, op, DEFAULT_PORT_HANDLE).unwrap();
        }
        let _res = self.term_latch.recv_timeout(Duration::from_secs(2));

        Ok(())
    }
}

#[derive(Debug)]
pub struct SchemaHolder {
    pub schema: Option<Schema>,
}

#[derive(Debug)]
pub struct TestSinkFactory {
    input_ports: Vec<PortHandle>,
    mapper: Arc<Mutex<SqlMapper>>,
    schema: Arc<RwLock<SchemaHolder>>,
    term_latch: Arc<Sender<bool>>,
    ops: usize,
}

impl TestSinkFactory {
    pub fn new(
        mapper: Arc<Mutex<SqlMapper>>,
        schema: Arc<RwLock<SchemaHolder>>,
        term_latch: Arc<Sender<bool>>,
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
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<(), ExecutionError> {
        let schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap().clone();
        self.schema.write().schema = Some(schema.clone());
        self.mapper
            .lock()
            .unwrap()
            .create_tables(vec![("results", &get_table_create_sql("results", schema))])
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(())
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn prepare(&self, _input_schemas: HashMap<PortHandle, Schema>) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        Ok(Box::new(TestSink::new(
            self.mapper.clone(),
            self.term_latch.clone(),
            self.ops,
        )))
    }
}

#[derive(Debug)]
pub struct TestSink {
    mapper: Arc<Mutex<SqlMapper>>,
    term_latch: Arc<Sender<bool>>,
    ops: usize,
    curr: usize,
}

impl TestSink {
    pub fn new(mapper: Arc<Mutex<SqlMapper>>, term_latch: Arc<Sender<bool>>, ops: usize) -> Self {
        Self {
            mapper,
            term_latch,
            ops,
            curr: 0,
        }
    }
}

impl Sink for TestSink {
    fn init(&mut self, _env: &mut LmdbEnvironmentManager) -> Result<(), ExecutionError> {
        debug!("SINK: Initialising TestSink");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        _state: &SharedTransaction,
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
            .unwrap();

        self.curr += 1;
        if self.curr == self.ops {
            let _ = self.term_latch.send(true);
        }

        Ok(())
    }

    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
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
        let mut pipeline = PipelineBuilder {}.build_pipeline(&self.sql).unwrap();

        let schema_holder: Arc<RwLock<SchemaHolder>> =
            Arc::new(RwLock::new(SchemaHolder { schema: None }));

        let (sync_sender, sync_receiver) = bounded::<bool>(0);
        let ops_count = self.ops.len();

        let mut asm = AppSourceManager::new();
        asm.add(AppSource::new(
            "mem".to_string(),
            Arc::new(TestSourceFactory::new(
                self.schema.clone(),
                self.ops.to_owned(),
                Arc::new(sync_receiver),
            )),
            vec![("actor".to_string(), DEFAULT_PORT_HANDLE)]
                .into_iter()
                .collect(),
        ))
        .unwrap();

        pipeline.add_sink(
            Arc::new(TestSinkFactory::new(
                self.mapper.clone(),
                schema_holder.clone(),
                Arc::new(sync_sender),
                ops_count,
            )),
            "sink",
        );

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

        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let mut exec = DagExecutor::new(
            &dag,
            tmp_dir.path(),
            ExecutorOptions::default(),
            Arc::new(AtomicBool::new(true)),
        )
        .unwrap_or_else(|_e| panic!("Unable to create exec"));
        exec.start()?;
        exec.join()?;

        let r_schema = schema_holder.read().schema.as_ref().unwrap().clone();
        Ok(r_schema)
    }
}
