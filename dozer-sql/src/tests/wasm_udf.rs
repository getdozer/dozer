use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use dozer_core::{
    app::{App, AppPipeline},
    appsource::{AppSourceManager, AppSourceMappings},
    channels::ProcessorChannelForwarder,
    dag_schemas::{DagHaveSchemas, DagSchemas},
    daggy::{
        petgraph::visit::{EdgeRef, Topo},
        NodeIndex, Walker,
    },
    event::EventHub,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, Source, SourceFactory},
    NodeKind, DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    errors::internal::BoxedError,
    models::{
        config::Config,
        udf_config::{UdfConfig, UdfType},
    },
    types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};
use tokio::runtime::Runtime;

use super::{builder_test::TestSinkFactory, utils::create_test_runtime};
use crate::builder::statement_to_pipeline;

#[derive(Debug)]
struct Input;
impl SourceFactory for Input {
    fn get_output_schema(&self, _: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(Schema {
            fields: vec![
                FieldDefinition::new(
                    "field1".into(),
                    FieldType::Int,
                    true,
                    SourceDefinition::Table {
                        name: "input_rows".into(),
                        connection: "fixtures".into(),
                    },
                ),
                FieldDefinition::new(
                    "field2".into(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Table {
                        name: "input_rows".into(),
                        connection: "fixtures".into(),
                    },
                ),
            ],
            ..Default::default()
        })
    }
    fn get_output_port_name(&self, _: &PortHandle) -> String {
        "input_rows".into()
    }
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }
    fn build(
        &self,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
        _: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        unreachable!("test operations are supplied directly")
    }
}
#[derive(Default)]
struct Output(Vec<TableOperation>);
impl ProcessorChannelForwarder for Output {
    fn send(&mut self, operation: TableOperation) {
        self.0.push(operation);
    }
}
type Endpoint = (NodeIndex, PortHandle);
struct Pipeline {
    processors: HashMap<NodeIndex, Box<dyn Processor>>,
    routes: HashMap<Endpoint, Vec<Endpoint>>,
    source: NodeIndex,
    sink: NodeIndex,
    schema: Schema,
    _runtime: Arc<Runtime>,
}
impl Pipeline {
    fn new(sql: &str, udfs: Vec<UdfConfig>) -> Result<Self, BoxedError> {
        let runtime = create_test_runtime();
        let mut pipeline = AppPipeline::new(Default::default());
        let context = statement_to_pipeline(
            sql,
            &mut pipeline,
            Some("result".into()),
            udfs,
            runtime.clone(),
        )?;
        let result = &context.output_tables_map["result"];
        pipeline.add_sink(
            Box::new(TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE])),
            "capture".into(),
        );
        pipeline.connect_nodes(
            result.node.clone(),
            result.port,
            "capture".into(),
            DEFAULT_PORT_HANDLE,
        );
        let mut sources = AppSourceManager::new();
        sources.add(
            Box::new(Input),
            AppSourceMappings::new(
                "fixtures".into(),
                HashMap::from([("input_rows".into(), DEFAULT_PORT_HANDLE)]),
            ),
        )?;
        let mut app = App::new(sources);
        app.add_pipeline(pipeline);
        let dag = runtime.block_on(DagSchemas::new(app.into_dag()?))?;
        let mut processors = HashMap::new();
        let mut source = None;
        let mut sink = None;
        let mut schema = None;
        for index in Topo::new(dag.graph()).iter(dag.graph()) {
            match &dag.graph()[index].kind {
                NodeKind::Source(_) => source = Some(index),
                NodeKind::Processor(factory) => {
                    processors.insert(
                        index,
                        runtime.block_on(factory.build(
                            dag.get_node_input_schemas(index),
                            dag.get_node_output_schemas(index),
                            EventHub::new(1),
                        ))?,
                    );
                }
                NodeKind::Sink(_) => {
                    sink = Some(index);
                    schema = Some(dag.get_node_input_schemas(index)[&DEFAULT_PORT_HANDLE].clone());
                }
            }
        }
        let mut routes: HashMap<Endpoint, Vec<Endpoint>> = HashMap::new();
        for edge in dag.graph().graph().edge_references() {
            routes
                .entry((edge.source(), edge.weight().output_port))
                .or_default()
                .push((edge.target(), edge.weight().input_port));
        }
        Ok(Self {
            processors,
            routes,
            source: source.unwrap(),
            sink: sink.unwrap(),
            schema: schema.unwrap(),
            _runtime: runtime,
        })
    }

    fn feed(&mut self, operation: Operation) -> Result<Vec<Operation>, BoxedError> {
        let mut queue = VecDeque::from([(
            self.source,
            TableOperation::without_id(operation, DEFAULT_PORT_HANDLE),
        )]);
        let mut emitted = Vec::new();
        while let Some((from, operation)) = queue.pop_front() {
            for &(to, port) in &self.routes[&(from, operation.port)] {
                let operation = TableOperation {
                    port,
                    op: operation.op.clone(),
                    id: operation.id,
                };
                if to == self.sink {
                    emitted.push(operation.op);
                } else {
                    let mut output = Output::default();
                    self.processors
                        .get_mut(&to)
                        .unwrap()
                        .process(operation, &mut output)?;
                    queue.extend(output.0.into_iter().map(|operation| (to, operation)));
                }
            }
        }
        Ok(emitted)
    }
}

fn example_config() -> Config {
    let project = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../examples/wasm-udf");
    let mut config: Config = dozer_types::serde_yaml::from_str(
        &std::fs::read_to_string(project.join("dozer-config.yaml")).unwrap(),
    )
    .unwrap();
    // Model launching Dozer from the example project directory without changing
    // this test process's working directory (tests run concurrently).
    for udf in &mut config.udfs {
        if let UdfType::Wasm(wasm) = &mut udf.config {
            wasm.path = project.join(&wasm.path).to_str().unwrap().into();
        }
    }
    config
}
fn row(a: i64, b: i64) -> Record {
    Record::new(vec![Field::Int(a), Field::Int(b)])
}
fn value(a: i64) -> Record {
    Record::new(vec![Field::Int(a)])
}

#[test]
fn wasm_assemblyscript_streaming_demo() {
    let config = example_config();
    let mut pipeline = Pipeline::new(config.sql.as_deref().unwrap(), config.udfs).unwrap();
    assert_eq!(pipeline.schema.fields[0].typ, FieldType::Int);
    assert!(pipeline.schema.fields[0].nullable);
    println!("AssemblyScript .wasm -> original YAML mapping -> SQL planner -> real DAG processors");
    let events = [
        (
            Operation::Insert { new: row(3, 4) },
            Operation::Insert { new: value(7) },
        ),
        (
            Operation::Update {
                old: row(3, 4),
                new: row(10, -2),
            },
            Operation::Update {
                old: value(7),
                new: value(8),
            },
        ),
        (
            Operation::Delete { old: row(10, -2) },
            Operation::Delete { old: value(8) },
        ),
        (
            Operation::Insert {
                new: Record::new(vec![Field::Null, Field::Int(1)]),
            },
            Operation::Insert {
                new: Record::new(vec![Field::Null]),
            },
        ),
    ];
    for (input, expected) in events {
        let output = pipeline.feed(input.clone()).unwrap();
        println!("{input:?} -> {output:?}");
        assert_eq!(output, vec![expected]);
    }
}

#[test]
fn wasm_assemblyscript_selection_and_case_insensitive_sql_call() {
    let mut config = example_config();
    config.udfs[0].name = "My_Function".into();
    if let UdfType::Wasm(wasm) = &mut config.udfs[0].config {
        wasm.export = Some("my_function".into());
    }
    let mut pipeline = Pipeline::new("SELECT MY_FUNCTION(field1, field2) AS total FROM input_rows WHERE my_function(field1, field2) > 5", config.udfs).unwrap();
    assert!(pipeline
        .feed(Operation::Insert { new: row(1, 2) })
        .unwrap()
        .is_empty());
    assert_eq!(
        pipeline.feed(Operation::Insert { new: row(3, 4) }).unwrap(),
        vec![Operation::Insert { new: value(7) }]
    );
}

#[test]
fn wasm_bad_configuration_and_wrong_sql_types_fail_before_streaming() {
    let mut config = example_config();
    if let UdfType::Wasm(wasm) = &mut config.udfs[0].config {
        wasm.export = Some("missing".into());
    }
    assert!(Pipeline::new("SELECT field1 FROM input_rows", config.udfs).is_err());
    assert!(Pipeline::new(
        "SELECT my_function('not a pointer', field2) FROM input_rows",
        example_config().udfs
    )
    .is_err());
    assert!(Pipeline::new(
        "SELECT my_function(field1) FROM input_rows",
        example_config().udfs
    )
    .is_err());
}

#[test]
fn wasm_assemblyscript_abort_does_not_poison_later_rows() {
    let mut config = example_config();
    if let UdfType::Wasm(wasm) = &mut config.udfs[0].config {
        wasm.export = Some("checked_divide".into());
    }
    let mut pipeline = Pipeline::new(
        "SELECT my_function(field1, field2) FROM input_rows",
        config.udfs,
    )
    .unwrap();
    assert!(pipeline
        .feed(Operation::Insert { new: row(6, 0) })
        .unwrap_err()
        .to_string()
        .contains("AssemblyScript abort"));
    assert_eq!(
        pipeline.feed(Operation::Insert { new: row(6, 2) }).unwrap(),
        vec![Operation::Insert { new: value(3) }]
    );
}

#[test]
fn wasm_scalar_calls_reject_modifiers_wildcards_and_named_arguments() {
    for call in [
        "my_function(DISTINCT field1, field2)",
        "my_function(field1, field2) OVER ()",
        "my_function(field1, field2 ORDER BY field1)",
        "my_function<INT>(field1, field2)",
        "my_function(*, field2)",
        "my_function(x => field1, y => field2)",
    ] {
        let result = Pipeline::new(
            &format!("SELECT {call} FROM input_rows"),
            example_config().udfs,
        );
        let Err(error) = result else {
            panic!("accepted unsupported scalar call: {call}");
        };
        assert!(
            error.to_string().contains("scalar call")
                || error.to_string().contains("Unsupported SQL function arg"),
            "{call}: {error}"
        );
    }
}

#[test]
fn wasm_computed_nullable_arguments_keep_nullable_output_schema() {
    let mut pipeline = Pipeline::new(
        "SELECT my_function(field1 + 1, field2) FROM input_rows",
        example_config().udfs,
    )
    .unwrap();
    assert!(pipeline.schema.fields[0].nullable);
    assert_eq!(
        pipeline
            .feed(Operation::Insert {
                new: Record::new(vec![Field::Null, Field::Int(1)])
            })
            .unwrap(),
        vec![Operation::Insert {
            new: Record::new(vec![Field::Null])
        }]
    );
    assert_eq!(
        pipeline.feed(Operation::Insert { new: row(2, 3) }).unwrap(),
        vec![Operation::Insert { new: value(6) }]
    );
}

#[test]
fn wasm_assemblyscript_mutable_globals_do_not_change_retractions() {
    let mut config = example_config();
    if let UdfType::Wasm(wasm) = &mut config.udfs[0].config {
        wasm.export = Some("isolated_counter".into());
    }
    let mut pipeline =
        Pipeline::new("SELECT my_function(field1) FROM input_rows", config.udfs).unwrap();
    assert_eq!(
        pipeline.feed(Operation::Insert { new: row(4, 0) }).unwrap(),
        vec![Operation::Insert { new: value(5) }]
    );
    assert_eq!(
        pipeline
            .feed(Operation::Update {
                old: row(4, 0),
                new: row(5, 0)
            })
            .unwrap(),
        vec![Operation::Update {
            old: value(5),
            new: value(6)
        }]
    );
    assert_eq!(
        pipeline.feed(Operation::Delete { old: row(5, 0) }).unwrap(),
        vec![Operation::Delete { old: value(6) }]
    );
}
