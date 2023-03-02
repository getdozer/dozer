use dozer_core::app::{App, AppPipeline};
use dozer_core::appsource::{AppSource, AppSourceManager};
use dozer_core::channels::SourceChannelForwarder;
use dozer_core::dag_schemas::{DagHaveSchemas, DagSchemas};
use dozer_core::errors::ExecutionError;
use dozer_core::node::{
    OutputPortDef, OutputPortType, PortHandle, Sink, SinkFactory, Source, SourceFactory,
};
use dozer_core::petgraph::visit::{IntoEdgesDirected, IntoNodeReferences};
use dozer_core::petgraph::Direction;
use dozer_core::{Dag, NodeKind, DEFAULT_PORT_HANDLE};

use dozer_core::executor::{DagExecutor, ExecutorOptions};
use dozer_core::record_store::RecordReader;
use dozer_core::storage::lmdb_storage::SharedTransaction;

use dozer_sql::pipeline::builder::{statement_to_pipeline, SchemaSQLContext};
use dozer_types::crossbeam::channel::{Receiver, Sender};

use dozer_types::ingestion_types::IngestionMessage;
use dozer_types::node::SourceStates;
use dozer_types::types::{Operation, Schema, SourceDefinition};
use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;

use dozer_core::epoch::Epoch;

use std::time::Duration;
use tempdir::TempDir;

use super::helper::get_table_create_sql;
use super::SqlMapper;

#[derive(Debug)]
pub(crate) struct TestSourceFactory {
    schemas: HashMap<u16, Schema>,
    name_to_port: HashMap<String, u16>,
    running: Arc<AtomicBool>,
    receiver: Receiver<Option<(String, Operation)>>,
}

impl TestSourceFactory {
    pub fn new(
        schemas: HashMap<u16, Schema>,
        name_to_port: HashMap<String, u16>,
        running: Arc<AtomicBool>,
        receiver: Receiver<Option<(String, Operation)>>,
    ) -> Self {
        Self {
            schemas,
            name_to_port,
            running,
            receiver,
        }
    }
}

impl SourceFactory<SchemaSQLContext> for TestSourceFactory {
    fn get_output_schema(
        &self,
        port: &PortHandle,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let mut schema = self
            .schemas
            .get(port)
            .expect("schemas should have been initialized with enumerated index")
            .to_owned();

        let table_name = self
            .name_to_port
            .iter()
            .find(|(_, p)| **p == *port)
            .unwrap()
            .0
            .clone();
        // Add source information to the schema.
        let mut fields = vec![];
        for field in schema.fields {
            let mut f = field.clone();
            f.source = SourceDefinition::Table {
                connection: "test_connection".to_string(),
                name: table_name.clone(),
            };
            fields.push(f);
        }
        schema.fields = fields;

        Ok((schema, SchemaSQLContext::default()))
    }

    fn get_output_ports(&self) -> Result<Vec<OutputPortDef>, ExecutionError> {
        Ok(self
            .schemas
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                OutputPortDef::new(
                    idx as u16,
                    OutputPortType::StatefulWithPrimaryKeyLookup {
                        retr_old_records_for_deletes: true,
                        retr_old_records_for_updates: true,
                    },
                )
            })
            .collect())
    }

    fn build(
        &self,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Source>, ExecutionError> {
        Ok(Box::new(TestSource {
            name_to_port: self.name_to_port.to_owned(),
            running: self.running.clone(),
            receiver: self.receiver.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct TestSource {
    name_to_port: HashMap<String, u16>,
    running: Arc<AtomicBool>,
    receiver: Receiver<Option<(String, Operation)>>,
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
        let mut idx = 0;

        while let Ok(Some((schema_name, op))) = self.receiver.recv() {
            idx += 1;
            let port = self.name_to_port.get(&schema_name).expect("port not found");
            fw.send(IngestionMessage::new_op(idx, 0, op), *port)
                .unwrap();
        }
        thread::sleep(Duration::from_millis(500));

        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
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
}

impl TestSinkFactory {
    pub fn new(mapper: Arc<Mutex<SqlMapper>>) -> Self {
        Self {
            input_ports: vec![DEFAULT_PORT_HANDLE],
            mapper,
        }
    }
}

impl SinkFactory<SchemaSQLContext> for TestSinkFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        self.input_ports.clone()
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _source_states: &SourceStates,
    ) -> Result<Box<dyn Sink>, ExecutionError> {
        let schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap().clone();

        self.mapper
            .lock()
            .unwrap()
            .create_tables(vec![("results", &get_table_create_sql("results", schema))])
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(Box::new(TestSink::new(self.mapper.clone())))
    }
}

#[derive(Debug)]
pub struct TestSink {
    mapper: Arc<Mutex<SqlMapper>>,
    curr: usize,
}

impl TestSink {
    pub fn new(mapper: Arc<Mutex<SqlMapper>>) -> Self {
        Self { mapper, curr: 0 }
    }
}

impl Sink for TestSink {
    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        _state: &SharedTransaction,
        _reader: &HashMap<PortHandle, Box<dyn RecordReader>>,
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
            .unwrap();

        self.curr += 1;

        Ok(())
    }

    fn commit(&mut self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn on_source_snapshotting_done(&mut self) -> Result<(), ExecutionError> {
        Ok(())
    }
}

pub struct TestPipeline {
    pub schema: Schema,
    pub dag: Dag<SchemaSQLContext>,
    pub used_schemas: Vec<String>,
    pub running: Arc<AtomicBool>,
    pub sender: Sender<Option<(String, Operation)>>,
    pub ops: Vec<(&'static str, Operation)>,
}

impl TestPipeline {
    pub fn new(
        sql: String,
        schemas: HashMap<String, Schema>,
        ops: Vec<(&'static str, Operation)>,
        mapper: Arc<Mutex<SqlMapper>>,
    ) -> Self {
        Self::build_pipeline(sql, schemas, ops, mapper).unwrap()
    }

    pub fn get_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn build_pipeline(
        sql: String,
        schemas: HashMap<String, Schema>,
        ops: Vec<(&'static str, Operation)>,
        mapper: Arc<Mutex<SqlMapper>>,
    ) -> Result<TestPipeline, ExecutionError> {
        let mut pipeline = AppPipeline::new();

        let transform_response =
            statement_to_pipeline(&sql, &mut pipeline, Some("results".to_string())).unwrap();

        let output_table = transform_response.output_tables_map.get("results").unwrap();
        let (sender, receiver) =
            dozer_types::crossbeam::channel::bounded::<Option<(String, Operation)>>(10);
        let mut port_to_schemas = HashMap::new();
        let mut mappings = HashMap::new();

        let running = Arc::new(AtomicBool::new(true));

        for (idx, (name, schema)) in schemas.iter().enumerate() {
            mappings.insert(name.clone(), idx as u16);
            port_to_schemas.insert(idx as u16, schema.clone());
        }

        let mut asm = AppSourceManager::new();

        asm.add(AppSource::new(
            "mem".to_string(),
            Arc::new(TestSourceFactory::new(
                port_to_schemas,
                mappings.clone(),
                running.clone(),
                receiver,
            )),
            mappings,
        ))
        .unwrap();

        pipeline.add_sink(Arc::new(TestSinkFactory::new(mapper)), "sink");

        pipeline
            .connect_nodes(
                &output_table.node,
                Some(output_table.port),
                "sink",
                Some(DEFAULT_PORT_HANDLE),
                true,
            )
            .unwrap();
        let used_schemas = pipeline.get_entry_points_sources_names();
        let mut app = App::new(asm);
        app.add_pipeline(pipeline);

        let dag = app.get_dag().unwrap();

        let dag_schemas = DagSchemas::new(dag.clone())?;

        let sink_index = (|| {
            for (node_index, node) in dag_schemas.graph().node_references() {
                if matches!(node.kind, NodeKind::Sink(_)) {
                    return node_index;
                }
            }
            panic!("Sink is expected");
        })();

        let schema = dag_schemas
            .graph()
            .edges_directed(sink_index, Direction::Incoming)
            .next()
            .expect("Sink must have incoming edge")
            .weight()
            .schema
            .clone();
        Ok(TestPipeline {
            schema,
            dag,
            used_schemas,
            running,
            sender,
            ops,
        })
    }

    pub fn run(self) -> Result<(), ExecutionError> {
        let tmp_dir =
            TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));

        let exec = DagExecutor::new(
            self.dag,
            tmp_dir.path().to_path_buf(),
            ExecutorOptions::default(),
        )
        .unwrap_or_else(|e| panic!("Unable to create exec: {e}"));
        let join_handle = exec.start(self.running.clone())?;

        for (schema_name, op) in &self.ops {
            if self.used_schemas.contains(&schema_name.to_string()) {
                self.sender
                    .send(Some((schema_name.to_string(), op.clone())))
                    .unwrap();
            }
        }
        self.sender.send(None).unwrap();

        join_handle.join()?;

        Ok(())
    }
}
