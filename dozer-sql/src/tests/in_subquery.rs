use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use dozer_core::{
    app::{App, AppPipeline, PipelineFlags},
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
        Self::try_new_with_flags(sql, tables, PipelineFlags::default())
    }

    fn try_new_with_flags(
        sql: &str,
        tables: Vec<(&str, Schema)>,
        flags: PipelineFlags,
    ) -> Result<Self, BoxedError> {
        let runtime = create_test_runtime();
        let mut pipeline = AppPipeline::new(flags);
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
        "SELECT DISTINCT value FROM inner_rows",
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
fn inner_with_shadows_an_outer_cte_without_changing_sibling_membership() {
    let mut pipeline = Pipeline::new(
        "WITH candidates AS (SELECT value FROM other_rows) \
         SELECT * FROM outer_rows WHERE \
         value IN (WITH candidates AS (SELECT value FROM inner_rows) \
                   SELECT candidates.value FROM candidates) \
         AND value IN (SELECT candidates.value FROM candidates)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
            ("other_rows", table_schema("other_rows", false)),
        ],
    );
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![
                seven.clone(),
                seven.clone(),
                nine.clone(),
                outer(3, Field::Null),
            ],
        },
    );
    pipeline.feed("other_rows", insert(inner(Field::Int(9))));
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![]);
    pipeline.feed("inner_rows", insert(inner(Field::Int(9))));
    pipeline.assert_rows(vec![nine]);
    pipeline.feed(
        "other_rows",
        Operation::Update {
            old: inner(Field::Int(9)),
            new: inner(Field::Int(7)),
        },
    );
    pipeline.assert_rows(vec![seven.clone(), seven]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
    pipeline.assert_rows(vec![]);
    assert_eq!(pipeline.schema.fields.len(), 2);
}

#[test]
fn a_sibling_membership_cannot_read_a_previous_subquery_private_cte() {
    let result = Pipeline::try_new(
        "SELECT * FROM outer_rows WHERE \
         value IN (WITH private_values AS (SELECT value FROM inner_rows) \
                   SELECT value FROM private_values) \
         AND value IN (SELECT value FROM private_values)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
        ],
    );
    assert!(
        result.is_err(),
        "a sibling query exposed the inner WITH binding"
    );
}

#[test]
fn nested_and_multiple_membership_queries_track_nulls_and_duplicates() {
    let mut pipeline = Pipeline::new(
        "SELECT * FROM outer_rows WHERE \
         value IN (SELECT value FROM inner_rows WHERE value IN \
                   (SELECT value FROM allowed_rows)) \
         AND value NOT IN (SELECT value FROM blocked_rows)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
            ("allowed_rows", table_schema("allowed_rows", false)),
            ("blocked_rows", table_schema("blocked_rows", false)),
        ],
    );
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![
                seven.clone(),
                seven.clone(),
                nine.clone(),
                outer(3, Field::Null),
            ],
        },
    );
    pipeline.feed(
        "inner_rows",
        Operation::BatchInsert {
            new: vec![
                inner(Field::Int(7)),
                inner(Field::Int(7)),
                inner(Field::Int(9)),
                inner(Field::Null),
            ],
        },
    );
    pipeline.assert_rows(vec![]);
    pipeline.feed("allowed_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven.clone(), seven.clone()]);
    pipeline.feed("blocked_rows", insert(inner(Field::Null)));
    pipeline.assert_rows(vec![]);
    pipeline.feed("blocked_rows", insert(inner(Field::Int(7))));
    pipeline.feed("blocked_rows", delete(inner(Field::Null)));
    pipeline.assert_rows(vec![]);
    pipeline.feed("blocked_rows", delete(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven.clone(), seven]);
    pipeline.feed(
        "allowed_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![nine]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.feed("allowed_rows", insert(inner(Field::Null)));
    pipeline.assert_rows(vec![]);
    assert_eq!(pipeline.schema.fields.len(), 2);
}

#[test]
fn computed_inner_projection_changes_membership_after_updates() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT value + 1 AS next_value FROM inner_rows)",
    );
    let eight = outer(1, Field::Int(8));
    let ten = outer(2, Field::Int(10));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![eight.clone(), ten.clone(), outer(3, Field::Null)],
        },
    );
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![eight]);
    pipeline.feed(
        "inner_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![ten.clone()]);
    pipeline.feed("inner_rows", insert(inner(Field::Null)));
    pipeline.assert_rows(vec![ten]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn union_membership_retains_matches_until_both_branches_remove_them() {
    for quantifier in ["ALL", "DISTINCT"] {
        let mut pipeline = Pipeline::new(
            &format!(
                "SELECT * FROM outer_rows WHERE value IN \
                      (SELECT value FROM inner_rows UNION {quantifier} \
                       SELECT value FROM other_rows)"
            ),
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
                ("other_rows", table_schema("other_rows", false)),
            ],
        );
        let seven = outer(1, Field::Int(7));
        let nine = outer(2, Field::Int(9));
        pipeline.feed(
            "outer_rows",
            Operation::BatchInsert {
                new: vec![
                    seven.clone(),
                    seven.clone(),
                    nine.clone(),
                    outer(3, Field::Null),
                ],
            },
        );
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
        pipeline.feed(
            "other_rows",
            Operation::BatchInsert {
                new: vec![inner(Field::Int(7)), inner(Field::Int(9))],
            },
        );
        pipeline.assert_rows(vec![seven.clone(), seven.clone(), nine.clone()]);
        pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
        pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
        pipeline.assert_rows(vec![seven.clone(), seven, nine.clone()]);
        pipeline.feed(
            "other_rows",
            Operation::Update {
                old: inner(Field::Int(7)),
                new: inner(Field::Int(9)),
            },
        );
        pipeline.assert_rows(vec![nine.clone()]);
        pipeline.feed("other_rows", delete(inner(Field::Int(9))));
        pipeline.assert_rows(vec![nine]);
        pipeline.feed("other_rows", delete(inner(Field::Int(9))));
        pipeline.assert_rows(vec![]);
    }
}

#[test]
fn grouped_having_membership_changes_when_a_group_crosses_its_threshold() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT value FROM inner_rows GROUP BY value HAVING COUNT(*) > 1)",
    );
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![seven.clone(), seven.clone(), nine.clone()],
        },
    );
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![]);
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven.clone(), seven.clone()]);
    pipeline.feed(
        "inner_rows",
        Operation::BatchInsert {
            new: vec![inner(Field::Int(9)), inner(Field::Int(9))],
        },
    );
    pipeline.assert_rows(vec![seven.clone(), seven, nine.clone()]);
    pipeline.feed(
        "inner_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![nine.clone()]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![nine]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn joined_inner_rows_keep_membership_until_the_last_match_disappears() {
    let mut pipeline = Pipeline::new(
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT i.value FROM inner_rows i JOIN other_rows j ON i.value = j.value)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
            ("other_rows", table_schema("other_rows", false)),
        ],
    );
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![seven.clone(), seven.clone(), nine.clone()],
        },
    );
    pipeline.feed(
        "inner_rows",
        Operation::BatchInsert {
            new: vec![
                inner(Field::Int(7)),
                inner(Field::Int(7)),
                inner(Field::Int(9)),
            ],
        },
    );
    pipeline.assert_rows(vec![]);
    pipeline.feed("other_rows", insert(inner(Field::Int(7))));
    pipeline.feed("other_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven.clone(), seven.clone()]);
    pipeline.feed("other_rows", delete(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven.clone(), seven]);
    pipeline.feed(
        "other_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![nine]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn expanded_inner_query_dependencies_reject_volatile_evaluation() {
    use dozer_sql_expression::sqlparser::{dialect::DozerDialect, parser::Parser};

    for subquery in [
        "WITH candidate AS (SELECT value FROM inner_rows WHERE NOW() IS NOT NULL) \
         SELECT value FROM candidate",
        "SELECT value FROM inner_rows WHERE NOW() IS NOT NULL \
         UNION ALL SELECT value FROM other_rows",
        "SELECT value FROM inner_rows \
         UNION DISTINCT SELECT value FROM other_rows WHERE NOW() IS NOT NULL",
        "WITH candidate AS (SELECT value FROM inner_rows WHERE NOW() IS NOT NULL) \
         SELECT c.value FROM candidate c JOIN other_rows j ON c.value = j.value",
        "WITH candidate AS (SELECT value FROM other_rows WHERE NOW() IS NOT NULL) \
         SELECT i.value FROM inner_rows i JOIN candidate c ON i.value = c.value",
        "SELECT i.value FROM inner_rows i JOIN other_rows j ON i.value = j.value \
         WHERE NOW() IS NOT NULL",
    ] {
        let sql = format!("SELECT * FROM outer_rows WHERE value IN ({subquery})");
        Parser::parse_sql(&DozerDialect {}, &sql).unwrap();
        let result = Pipeline::try_new(
            &sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
                ("other_rows", table_schema("other_rows", false)),
            ],
        );
        let error = match result {
            Ok(_) => panic!("volatile membership dependency was accepted: {subquery}"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("deterministic"),
            "expected the replay guard to reject {subquery}, got {error}"
        );
    }
}

#[test]
fn ancestor_cte_and_derived_modifiers_cannot_bypass_membership_validation() {
    use dozer_sql_expression::sqlparser::{dialect::DozerDialect, parser::Parser};

    for (subquery, expected) in [
        ("SELECT TOP (1) value FROM inner_rows", "SELECT modifiers"),
        (
            "SELECT value FROM inner_rows FETCH FIRST 1 ROW ONLY",
            "FETCH",
        ),
    ] {
        for sql in [
            format!("SELECT * FROM outer_rows WHERE value IN ({subquery})"),
            format!(
                "WITH candidate AS ({subquery}) \
                     SELECT * FROM outer_rows WHERE value IN (SELECT value FROM candidate)"
            ),
            format!(
                "WITH candidate AS \
                     (SELECT value FROM inner_rows UNION ALL ({subquery})) \
                     SELECT * FROM outer_rows WHERE value IN (SELECT value FROM candidate)"
            ),
            format!(
                "SELECT * FROM outer_rows WHERE value IN \
                     (SELECT value FROM ({subquery}) derived_rows)"
            ),
        ] {
            Parser::parse_sql(&DozerDialect {}, &sql).unwrap();
            let error = match Pipeline::try_new(
                &sql,
                vec![
                    ("outer_rows", table_schema("outer_rows", true)),
                    ("inner_rows", table_schema("inner_rows", false)),
                ],
            ) {
                Ok(_) => panic!("unsupported dependency modifier was accepted: {sql}"),
                Err(error) => error,
            };
            assert!(error.to_string().contains(expected), "{sql}: {error}");
        }
    }
}

#[test]
fn unsupported_aggregate_shapes_are_rejected_before_processing() {
    use dozer_sql_expression::sqlparser::{dialect::DozerDialect, parser::Parser};

    for (sql, expected) in [
        (
            "SELECT * FROM outer_rows WHERE value IN \
          (SELECT COUNT(*) FROM inner_rows)",
            "without GROUP BY",
        ),
        (
            "WITH candidate AS (SELECT COUNT(*) AS value FROM inner_rows) \
          SELECT * FROM outer_rows WHERE value IN (SELECT value FROM candidate)",
            "without GROUP BY",
        ),
        (
            "SELECT * FROM outer_rows WHERE value IN \
          (SELECT COUNT(DISTINCT value) FROM inner_rows GROUP BY value)",
            "function modifiers",
        ),
        (
            "SELECT * FROM outer_rows WHERE value IN \
          (SELECT COUNT(*) OVER () FROM inner_rows GROUP BY value)",
            "function modifiers",
        ),
    ] {
        Parser::parse_sql(&DozerDialect {}, sql).unwrap();
        let error = match Pipeline::try_new(
            sql,
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
            ],
        ) {
            Ok(_) => panic!("unsupported aggregate was accepted: {sql}"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(expected), "{sql}: {error}");
    }
}

#[test]
fn parenthesized_select_and_union_branches_keep_inner_with_bindings() {
    for (subquery, union) in [
        (
            "WITH candidate AS (SELECT value FROM inner_rows) \
          (SELECT candidate.value FROM candidate)",
            false,
        ),
        (
            "SELECT value FROM inner_rows UNION ALL \
          (WITH candidate AS (SELECT value FROM other_rows) \
           SELECT candidate.value FROM candidate)",
            true,
        ),
    ] {
        let mut pipeline = Pipeline::new(
            &format!("SELECT * FROM outer_rows WHERE value IN ({subquery})"),
            vec![
                ("outer_rows", table_schema("outer_rows", true)),
                ("inner_rows", table_schema("inner_rows", false)),
                ("other_rows", table_schema("other_rows", false)),
            ],
        );
        let seven = outer(1, Field::Int(7));
        let nine = outer(2, Field::Int(9));
        pipeline.feed(
            "outer_rows",
            Operation::BatchInsert {
                new: vec![seven.clone(), nine.clone()],
            },
        );
        pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
        pipeline.assert_rows(vec![seven.clone()]);
        if union {
            pipeline.feed("other_rows", insert(inner(Field::Int(9))));
            pipeline.assert_rows(vec![seven, nine.clone()]);
        }
        pipeline.feed(
            "inner_rows",
            Operation::Update {
                old: inner(Field::Int(7)),
                new: inner(Field::Int(9)),
            },
        );
        pipeline.assert_rows(vec![nine.clone()]);
        pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
        if union {
            pipeline.assert_rows(vec![nine]);
            pipeline.feed("other_rows", delete(inner(Field::Int(9))));
        }
        pipeline.assert_rows(vec![]);
    }
}

#[test]
fn cte_join_without_alias_uses_its_visible_cte_name() {
    let mut pipeline = Pipeline::new(
        "WITH candidate AS (SELECT value FROM inner_rows) \
         SELECT * FROM outer_rows WHERE value IN \
         (SELECT candidate.value FROM candidate JOIN other_rows \
          ON candidate.value = other_rows.value)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
            ("other_rows", table_schema("other_rows", false)),
        ],
    );
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![seven.clone(), nine.clone()],
        },
    );
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![]);
    pipeline.feed("other_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![seven]);
    pipeline.feed(
        "other_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![]);
    pipeline.feed("inner_rows", insert(inner(Field::Int(9))));
    pipeline.assert_rows(vec![nine]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn grouped_count_projection_keeps_shared_counts_until_the_last_group_changes() {
    let mut pipeline = Pipeline::two_tables(
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT COUNT(*) FROM inner_rows GROUP BY value)",
    );
    let one = outer(1, Field::Int(1));
    let two = outer(2, Field::Int(2));
    pipeline.feed(
        "outer_rows",
        Operation::BatchInsert {
            new: vec![one.clone(), two.clone()],
        },
    );
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![one.clone()]);
    pipeline.feed("inner_rows", insert(inner(Field::Int(9))));
    pipeline.assert_rows(vec![one.clone()]);
    pipeline.feed("inner_rows", insert(inner(Field::Int(7))));
    pipeline.assert_rows(vec![one.clone(), two.clone()]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![two]);
    pipeline.feed(
        "inner_rows",
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(9)),
        },
    );
    pipeline.assert_rows(vec![one.clone()]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(7))));
    pipeline.assert_rows(vec![one]);
    pipeline.feed("inner_rows", delete(inner(Field::Int(9))));
    pipeline.assert_rows(vec![]);
}

#[test]
fn probabilistic_union_distinct_is_rejected_for_direct_and_cte_membership() {
    let mut flags = PipelineFlags::default();
    flags.enable_probabilistic_optimizations.in_sets = Some(true);
    for quantifier in ["", " DISTINCT"] {
        let union =
            format!("SELECT value FROM inner_rows UNION{quantifier} SELECT value FROM other_rows");
        for sql in [
            format!("SELECT * FROM outer_rows WHERE value IN ({union})"),
            format!(
                "WITH candidate AS ({union}) SELECT * FROM outer_rows \
                     WHERE value IN (SELECT value FROM candidate)"
            ),
        ] {
            let error = match Pipeline::try_new_with_flags(
                &sql,
                vec![
                    ("outer_rows", table_schema("outer_rows", true)),
                    ("inner_rows", table_schema("inner_rows", false)),
                    ("other_rows", table_schema("other_rows", false)),
                ],
                flags.clone(),
            ) {
                Ok(_) => panic!("membership accepted probabilistic UNION counting: {sql}"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("exact UNION counting"),
                "{sql}: {error}"
            );
        }
    }
}

#[test]
fn probabilistic_set_flag_keeps_union_all_and_ordinary_union_distinct_working() {
    let mut flags = PipelineFlags::default();
    flags.enable_probabilistic_optimizations.in_sets = Some(true);
    let mut membership = Pipeline::try_new_with_flags(
        "SELECT * FROM outer_rows WHERE value IN \
         (SELECT value FROM inner_rows UNION ALL SELECT value FROM other_rows)",
        vec![
            ("outer_rows", table_schema("outer_rows", true)),
            ("inner_rows", table_schema("inner_rows", false)),
            ("other_rows", table_schema("other_rows", false)),
        ],
        flags.clone(),
    )
    .unwrap();
    let row = outer(1, Field::Int(7));
    membership.feed("outer_rows", insert(row.clone()));
    membership.feed("inner_rows", insert(inner(Field::Int(7))));
    membership.assert_rows(vec![row.clone()]);
    membership.feed("other_rows", insert(inner(Field::Int(7))));
    membership.feed("inner_rows", delete(inner(Field::Int(7))));
    membership.assert_rows(vec![row]);
    membership.feed("other_rows", delete(inner(Field::Int(7))));
    membership.assert_rows(vec![]);

    let mut ordinary = Pipeline::try_new_with_flags(
        "SELECT value FROM inner_rows UNION DISTINCT SELECT value FROM other_rows",
        vec![
            ("inner_rows", table_schema("inner_rows", false)),
            ("other_rows", table_schema("other_rows", false)),
        ],
        flags,
    )
    .unwrap();
    let row = inner(Field::Int(7));
    ordinary.feed("inner_rows", insert(row.clone()));
    ordinary.assert_rows(vec![row.clone()]);
    ordinary.feed("other_rows", insert(row.clone()));
    ordinary.assert_rows(vec![row.clone()]);
    ordinary.feed("inner_rows", delete(row.clone()));
    ordinary.assert_rows(vec![row.clone()]);
    ordinary.feed("other_rows", delete(row));
    ordinary.assert_rows(vec![]);
}

#[test]
fn membership_demo_with_synthetic_rows() {
    let sql = "SELECT * FROM outer_rows WHERE value IN (SELECT value FROM inner_rows)";
    let mut pipeline = Pipeline::two_tables(sql);
    println!("Synthetic data; real SQL planner and processors; synchronous event queue.");
    println!("SQL: {sql}");
    let seven = outer(1, Field::Int(7));
    let nine = outer(2, Field::Int(9));
    let steps = [
        ("outer_rows", insert(seven.clone()), vec![]),
        ("outer_rows", insert(nine.clone()), vec![]),
        ("inner_rows", insert(inner(Field::Int(7))), vec![seven]),
        (
            "inner_rows",
            Operation::Update {
                old: inner(Field::Int(7)),
                new: inner(Field::Int(9)),
            },
            vec![nine.clone()],
        ),
        ("outer_rows", delete(nine), vec![]),
        ("inner_rows", delete(inner(Field::Int(9))), vec![]),
    ];
    for (step, (table, operation, expected)) in steps.into_iter().enumerate() {
        println!("Step {} input {table}: {operation:?}", step + 1);
        let output = pipeline.feed(table, operation);
        println!("  Emitted operations: {output:?}");
        println!("  Result rows: {:?}", pipeline.rows);
        pipeline.assert_rows(expected);
    }
    println!("PASS: all six input changes produced the expected result rows.");
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
