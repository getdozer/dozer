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
    types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};
use tokio::runtime::Runtime;

use crate::builder::statement_to_pipeline;

use super::{builder_test::TestSinkFactory, utils::create_test_runtime};

#[derive(Debug)]
struct TableSources(Vec<(String, Schema)>);

impl SourceFactory for TableSources {
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(self.0[usize::from(*port)].1.clone())
    }

    fn get_output_port_name(&self, port: &PortHandle) -> String {
        self.0[usize::from(*port)].0.clone()
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        (0..self.0.len())
            .map(|port| OutputPortDef::new(port as PortHandle, OutputPortType::Stateless))
            .collect()
    }

    fn build(
        &self,
        _schemas: HashMap<PortHandle, Schema>,
        _events: EventHub,
        _state: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        unreachable!("the test supplies source operations directly")
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
    sources: HashMap<String, Endpoint>,
    sink: NodeIndex,
    schema: Schema,
    rows: Vec<Record>,
    _runtime: Arc<Runtime>,
}

impl Pipeline {
    fn new(sql: &str, tables: Vec<(&str, Schema)>) -> Self {
        Self::try_new(sql, tables).unwrap()
    }

    fn try_new(sql: &str, tables: Vec<(&str, Schema)>) -> Result<Self, BoxedError> {
        let runtime = create_test_runtime();
        let mut pipeline = AppPipeline::new_with_default_flags();
        let context = statement_to_pipeline(
            sql,
            &mut pipeline,
            Some("result".into()),
            vec![],
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

        let mappings: HashMap<_, _> = tables
            .iter()
            .enumerate()
            .map(|(port, (name, _))| ((*name).to_owned(), port as PortHandle))
            .collect();
        let mut sources = AppSourceManager::new();
        sources
            .add(
                Box::new(TableSources(
                    tables
                        .into_iter()
                        .map(|(name, schema)| (name.to_owned(), schema))
                        .collect(),
                )),
                AppSourceMappings::new("fixtures".into(), mappings.clone()),
            )
            .unwrap();
        let mut app = App::new(sources);
        app.add_pipeline(pipeline);
        let dag = runtime.block_on(DagSchemas::new(app.into_dag()?))?;

        let mut processors = HashMap::new();
        let mut routes: HashMap<Endpoint, Vec<Endpoint>> = HashMap::new();
        let mut source_node = None;
        let mut sink = None;
        let mut schema = None;
        for index in Topo::new(dag.graph()).iter(dag.graph()) {
            match &dag.graph()[index].kind {
                NodeKind::Source(_) => source_node = Some(index),
                NodeKind::Processor(factory) => {
                    let processor = runtime.block_on(factory.build(
                        dag.get_node_input_schemas(index),
                        dag.get_node_output_schemas(index),
                        EventHub::new(1),
                    ))?;
                    processors.insert(index, processor);
                }
                NodeKind::Sink(_) => {
                    sink = Some(index);
                    schema = Some(dag.get_node_input_schemas(index)[&DEFAULT_PORT_HANDLE].clone());
                }
            }
        }
        for edge in dag.graph().graph().edge_references() {
            routes
                .entry((edge.source(), edge.weight().output_port))
                .or_default()
                .push((edge.target(), edge.weight().input_port));
        }
        let source_node = source_node.unwrap();
        Ok(Self {
            processors,
            routes,
            sources: mappings
                .into_iter()
                .map(|(name, port)| (name, (source_node, port)))
                .collect(),
            sink: sink.unwrap(),
            schema: schema.unwrap(),
            rows: vec![],
            _runtime: runtime,
        })
    }

    fn two_tables(sql: &str) -> Self {
        Self::new(
            sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        )
    }

    fn feed(&mut self, table: &str, operation: Operation) -> Vec<Operation> {
        // Every operator is built by the real SQL planner. Only the transport
        // is synchronous, so each assertion observes a drained event queue.
        let (source, port) = self.sources[table];
        let mut queue = VecDeque::from([(source, TableOperation::without_id(operation, port))]);
        let mut emitted = Vec::new();
        while let Some((from, operation)) = queue.pop_front() {
            for &(to, port) in self.routes.get(&(from, operation.port)).unwrap() {
                let operation = TableOperation {
                    id: operation.id,
                    port,
                    op: operation.op.clone(),
                };
                if to == self.sink {
                    apply(&mut self.rows, &operation.op);
                    emitted.push(operation.op);
                } else {
                    let mut output = Output::default();
                    self.processors
                        .get_mut(&to)
                        .unwrap()
                        .process(operation, &mut output)
                        .unwrap();
                    queue.extend(output.0.into_iter().map(|operation| (to, operation)));
                }
            }
        }
        emitted
    }

    fn assert_rows(&self, expected: Vec<Record>) {
        fn counts(rows: &[Record]) -> HashMap<Record, usize> {
            let mut counts = HashMap::new();
            for row in rows {
                *counts.entry(row.clone()).or_default() += 1;
            }
            counts
        }
        assert_eq!(counts(&self.rows), counts(&expected));
        assert!(self
            .rows
            .iter()
            .all(|row| row.values.len() == self.schema.fields.len()));
    }
}

fn table_schema(name: &str, with_id: bool) -> Schema {
    let source = SourceDefinition::Table {
        connection: "fixtures".into(),
        name: name.into(),
    };
    let mut fields = vec![];
    if with_id {
        fields.push(FieldDefinition::new(
            "id".into(),
            FieldType::Int,
            false,
            source.clone(),
        ));
    }
    fields.push(FieldDefinition::new(
        "value".into(),
        FieldType::Int,
        true,
        source,
    ));
    Schema {
        fields,
        primary_index: vec![],
    }
}

fn outer(id: i64, value: Field) -> Record {
    Record::new(vec![Field::Int(id), value])
}

fn inner(value: Field) -> Record {
    Record::new(vec![value])
}

fn insert(new: Record) -> Operation {
    Operation::Insert { new }
}

fn delete(old: Record) -> Operation {
    Operation::Delete { old }
}

fn apply(rows: &mut Vec<Record>, operation: &Operation) {
    match operation {
        Operation::Insert { new } => rows.push(new.clone()),
        Operation::Delete { old } => {
            let index = rows
                .iter()
                .position(|row| row == old)
                .expect("the pipeline deleted a row that was never emitted");
            rows.remove(index);
        }
        Operation::Update { old, new } => {
            apply(rows, &delete(old.clone()));
            apply(rows, &insert(new.clone()));
        }
        Operation::BatchInsert { new } => rows.extend(new.iter().cloned()),
    }
}

#[test]
fn late_inner_changes_and_outer_changes_reach_the_query_output() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT * FROM outer_rows WHERE value IN (SELECT value FROM inner_rows)",
    );
    assert_eq!(
        pipeline
            .schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "value"]
    );
    let first = outer(1, Field::Int(7));
    let second = outer(2, Field::Int(9));
    pipeline.feed("outer_rows", insert(first.clone()));
    pipeline.feed("outer_rows", insert(second.clone()));
    pipeline.assert_rows(vec![]);
    assert_eq!(
        pipeline.feed("inner_rows", insert(inner(Field::Int(7)))),
        vec![insert(first.clone())]
    );
    let changed = outer(1, Field::Int(9));
    assert_eq!(
        pipeline.feed(
            "outer_rows",
            Operation::Update {
                old: first.clone(),
                new: changed.clone()
            }
        ),
        vec![delete(first)]
    );
    pipeline.feed(
        "inner_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![changed.clone(), second.clone()]);
    pipeline.feed("outer_rows", delete(second));
    pipeline.assert_rows(vec![changed]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn duplicate_and_null_membership_transitions_preserve_sql_rows() {
    for negated in [false, true] {
        let mut pipeline = Pipeline::two_tables(&format!(
            "SELECT * FROM outer_rows WHERE value {}IN (SELECT value FROM inner_rows)",
            if negated { "NOT " } else { "" }
        ));
        let seven = outer(1, Field::Int(7));
        let nine = outer(2, Field::Int(9));
        let null = outer(3, Field::Null);
        let rows = vec![seven.clone(), seven.clone(), nine.clone(), null];
        pipeline.feed("outer_rows", Operation::BatchInsert { new: rows.clone() });
        pipeline.assert_rows(if negated { rows.clone() } else { vec![] });
        pipeline.feed(
            "inner_rows",
            Operation::BatchInsert {
                new: vec![
                    inner(Field::Int(7)),
                    inner(Field::Int(7)),
                    inner(Field::Null),
                ],
            },
        );
        pipeline.assert_rows(if negated {
            vec![]
        } else {
            vec![seven.clone(), seven.clone()]
        });
        assert!(pipeline
            .feed("inner_rows", delete(inner(Field::Int(7))))
            .is_empty());
        pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
        pipeline.assert_rows(vec![]);
        pipeline.feed("inner_rows", delete(inner(Field::Null)));
        pipeline.assert_rows(if negated { rows } else { vec![] });
        pipeline.feed("inner_rows", insert(inner(Field::Int(9))));
        pipeline.assert_rows(if negated {
            vec![seven.clone(), seven.clone()]
        } else {
            vec![nine.clone()]
        });
        pipeline.feed("outer_rows", delete(seven.clone()));
        pipeline.assert_rows(if negated { vec![seven] } else { vec![nine] });
    }
}

#[test]
fn projection_receives_only_visible_columns() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT id FROM outer_rows WHERE value IN (SELECT value FROM inner_rows)",
    );
    assert_eq!(pipeline.schema.fields.len(), 1);
    assert_eq!(pipeline.schema.fields[0].name, "id");
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.feed("outer_rows", insert(outer(4, Field::Int(7))));
    pipeline.assert_rows(vec![Record::new(vec![Field::Int(4)])]);
}

#[test]
fn qualified_operands_and_filtered_inner_rows_use_their_own_schemas() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT o.id FROM outer_rows o WHERE o.value IN \
         (SELECT i.value FROM inner_rows i WHERE i.value > 5)",
    );
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![outer(1, Field::Int(7)), outer(2, Field::Int(4))],
        },
    );
    pipeline.feed(
        "inner_rows",
        Operation::BatchInsert {
            new: vec![inner(Field::Int(7)), inner(Field::Int(4))],
        },
    );
    pipeline.assert_rows(vec![Record::new(vec![Field::Int(1)])]);
    pipeline.feed(
        "inner_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(4)),
        },
    );
    pipeline.assert_rows(vec![]);
}

#[test]
fn outer_cte_membership_uses_the_visible_relation_name_or_alias() {
    for subquery in ["SELECT c.value FROM c", "SELECT x.value FROM c AS x"] {
        let mut pipeline = Pipeline::two_tables(&format!(
            "WITH c AS (SELECT value FROM inner_rows) \
             SELECT * FROM outer_rows WHERE value IN ({subquery})"
        ));
        let seven = outer(1, Field::Int(7));
        let nine = outer(2, Field::Int(9));
        pipeline.feed(
            "outer_rows",
            Operation::BatchInsert {
                new: vec![seven.clone(), nine.clone(), outer(3, Field::Null)],
            },
        );
        pipeline.assert_rows(vec![]);
        pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
        pipeline.assert_rows(vec![seven]);
        pipeline.feed(
            "inner_rows",
            Operation::Update {
                old: inner(Field::Int(7)),
                new: inner(Field::Int(9)),
            },
        );
        pipeline.assert_rows(vec![nine]);
        pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
        pipeline.assert_rows(vec![]);
        assert_eq!(
            pipeline
                .schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "value"]
        );
    }
}

#[test]
fn cte_membership_does_not_expose_original_or_hidden_qualifiers() {
    for subquery in [
        "SELECT inner_rows.value FROM c",
        "SELECT c.value FROM c AS x",
        "SELECT inner_rows.value FROM c AS x",
    ] {
        let sql = format!(
            "WITH c AS (SELECT value FROM inner_rows) \
             SELECT * FROM outer_rows WHERE value IN ({subquery})"
        );
        let result = Pipeline::try_new(
            &sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        );
        assert!(
            result.is_err(),
            "CTE accepted an out-of-scope qualifier: {subquery}"
        );
    }
}

#[test]
fn volatile_inner_filters_are_rejected_including_cte_dependencies() {
    use dozer_sql_expression::sqlparser::{dialect::DozerDialect, parser::Parser};

    for sql in [
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT value FROM inner_rows WHERE NOW() IS NOT NULL)",
        "WITH c AS (SELECT value FROM inner_rows WHERE NOW() IS NOT NULL) \
         SELECT * FROM outer_rows WHERE value IN (SELECT c.value FROM c)",
        "WITH c AS (SELECT value FROM inner_rows WHERE NOW() IS NOT NULL), \
         d AS (SELECT c.value FROM c) \
         SELECT * FROM outer_rows WHERE value IN (SELECT d.value FROM d)",
        "WITH c AS (SELECT NOW() AS value FROM inner_rows) \
         SELECT * FROM outer_rows WHERE value IN (SELECT c.value FROM c)",
        "SELECT * FROM outer_rows WHERE \
         value IN (SELECT value FROM inner_rows) AND NOW() IS NOT NULL",
    ] {
        Parser::parse_sql(&DozerDialect {}, sql)
            .unwrap_or_else(|error| panic!("test must reach planner validation: {sql}: {error}"));
        let result = Pipeline::try_new(
            sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        );
        assert!(
            result.is_err(),
            "volatile membership input was accepted: {sql}"
        );
    }
}

#[test]
fn ordinary_queries_keep_volatile_filters_without_membership() {
    let mut pipeline = Pipeline::new(
        "SELECT * FROM outer_rows WHERE NOW() IS NOT NULL",
        vec![("outer_rows", table_schema("outer_rows", true))],
    );
    let row = outer(1, Field::Int(7));
    pipeline.feed("outer_rows", insert(row.clone()));
    pipeline.assert_rows(vec![row.clone()]);
    pipeline.feed("outer_rows", delete(row));
    pipeline.assert_rows(vec![]);
}

#[test]
fn membership_inside_a_compound_predicate_keeps_the_other_branch() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT * FROM outer_rows WHERE id = 99 OR \
         (value IN (SELECT value FROM inner_rows) AND value > 5)",
    );
    let always = outer(99, Field::Int(1));
    let member = outer(1, Field::Int(7));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![always.clone(), member.clone(), outer(2, Field::Int(3))],
        },
    );
    pipeline.assert_rows(vec![always.clone()]);
    pipeline.feed(
        "inner_rows",
        Operation::BatchInsert {
            new: vec![inner(Field::Int(7)), inner(Field::Int(3))],
        },
    );
    pipeline.assert_rows(vec![always.clone(), member]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
    pipeline.assert_rows(vec![always]);
}

#[test]
fn negated_compounds_preserve_unknown_membership() {
    for predicate in [
        "NOT (value IN (SELECT value FROM inner_rows) OR 1 = 2)",
        "NOT (value IN (SELECT value FROM inner_rows) AND 1 = 1)",
    ] {
        let mut pipeline =
            Pipeline::two_tables(&format!("SELECT * FROM outer_rows WHERE {predicate}"));
        let row = outer(1, Field::Int(7));
        pipeline.feed("outer_rows", insert(row.clone()));
        pipeline.assert_rows(vec![row.clone()]);
        pipeline.feed("inner_rows", insert(inner(Field::Null)));
        pipeline.assert_rows(vec![]);
        pipeline.feed("inner_rows", delete(inner(Field::Null)));
        pipeline.assert_rows(vec![row]);
    }
}

#[test]
fn unsupported_subqueries_fail_during_pipeline_construction() {
    for subquery in [
        "SELECT value, value FROM inner_rows",
        "SELECT COUNT(*) FROM inner_rows",
        "SELECT value + 1 FROM inner_rows",
        "SELECT DISTINCT value FROM inner_rows",
        "SELECT value FROM inner_rows GROUP BY value",
        "SELECT value FROM inner_rows UNION ALL SELECT value FROM inner_rows",
        "WITH candidate AS (SELECT value FROM inner_rows) SELECT value FROM candidate",
        "SELECT i.value FROM inner_rows i JOIN inner_rows j ON i.value = j.value",
    ] {
        let sql = format!("SELECT * FROM outer_rows WHERE value IN ({subquery})");
        let result = Pipeline::try_new(
            &sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        );
        assert!(
            result.is_err(),
            "unsupported subquery was accepted: {subquery}"
        );
    }
}

#[test]
fn parsed_modifiers_are_rejected_instead_of_ignored() {
    use crate::errors::PipelineError;
    use dozer_sql_expression::sqlparser::{dialect::DozerDialect, parser::Parser};

    for (subquery, expected) in [
        (
            "SELECT value FROM inner_rows FETCH FIRST 1 ROW ONLY",
            "FETCH",
        ),
        ("SELECT TOP (1) value FROM inner_rows", "SELECT modifiers"),
        (
            "SELECT value FROM inner_rows QUALIFY value > 5",
            "SELECT modifiers",
        ),
        ("SELECT value FROM inner_rows(1)", "table arguments"),
        ("SELECT value FROM inner_rows WITH (NOLOCK)", "hints"),
        (
            "SELECT renamed FROM inner_rows AS i(renamed)",
            "column aliases",
        ),
    ] {
        let sql = format!("SELECT * FROM outer_rows WHERE value IN ({subquery})");
        Parser::parse_sql(&DozerDialect {}, &sql)
            .unwrap_or_else(|error| panic!("test must reach planner validation: {sql}: {error}"));
        let runtime = create_test_runtime();
        let error = statement_to_pipeline(
            &sql,
            &mut AppPipeline::new_with_default_flags(),
            Some("result".into()),
            vec![],
            runtime,
        )
        .unwrap_err();
        assert!(
            matches!(error, PipelineError::InvalidQuery(_)),
            "expected explicit planner rejection for {subquery}, got {error}"
        );
        assert!(error.to_string().contains(expected), "{subquery}: {error}");
    }
}

#[test]
fn correlated_names_are_not_rebound_to_the_inner_relation() {
    for subquery in [
        "SELECT i.value FROM inner_rows i WHERE i.value = o.value",
        "SELECT o.value FROM inner_rows i",
    ] {
        let sql = format!("SELECT * FROM outer_rows o WHERE o.value IN ({subquery})");
        let result = Pipeline::try_new(
            &sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        );
        assert!(
            result.is_err(),
            "outer reference was accepted as an inner column: {subquery}"
        );
    }
}

#[test]
fn shared_source_settles_with_either_fanout_order() {
    for reverse in [false, true] {
        let mut pipeline = Pipeline::new(
            "SELECT * FROM rows WHERE value IN (SELECT value FROM rows)",
            vec![("rows", table_schema("rows", true))],
        );
        if reverse {
            for targets in pipeline.routes.values_mut() {
                targets.reverse();
            }
        }
        let first = outer(1, Field::Int(7));
        let second = outer(2, Field::Int(9));
        pipeline.feed(
            "rows",
            Operation::BatchInsert {
                new: vec![first.clone(), second.clone(), outer(3, Field::Null)],
            },
        );
        pipeline.assert_rows(vec![first.clone(), second.clone()]);
        let changed = outer(1, Field::Int(10));
        pipeline.feed(
            "rows",
            Operation::Update {
                old: first,
                new: changed.clone(),
            },
        );
        pipeline.assert_rows(vec![changed.clone(), second.clone()]);
        pipeline.feed("rows", insert(changed.clone()));
        pipeline.assert_rows(vec![changed.clone(), changed.clone(), second.clone()]);
        pipeline.feed("rows", delete(second));
        pipeline.feed("rows", delete(changed.clone()));
        pipeline.assert_rows(vec![changed]);
        assert_eq!(pipeline.schema.fields.len(), 2);
    }
}
