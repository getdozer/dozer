//! Exercise actual parser, DAG wiring, schema propagation and processors. The
//! deterministic driver controls the order of changes on both source ports.
use std::collections::{HashMap, VecDeque};

use dozer_core::{
    app::{App, AppPipeline},
    appsource::{AppSourceManager, AppSourceMappings},
    channels::ProcessorChannelForwarder,
    dag_schemas::{DagHaveSchemas, DagSchemas},
    event::EventHub,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, Source, SourceFactory},
    petgraph::visit::EdgeRef,
    NodeKind, DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    errors::internal::BoxedError,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema, TableOperation},
};

use super::{
    builder_test::{TestSinkFactory, TestSource},
    utils::create_test_runtime,
};
use crate::builder::statement_to_pipeline;

#[derive(Debug)]
struct Sources(Vec<Schema>);
impl SourceFactory for Sources {
    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        (0..self.0.len())
            .map(|port| OutputPortDef::new(port as u16, OutputPortType::Stateless))
            .collect()
    }
    fn get_output_schema(&self, port: &PortHandle) -> Result<Schema, BoxedError> {
        Ok(self.0[*port as usize].clone())
    }
    fn get_output_port_name(&self, port: &PortHandle) -> String {
        format!("table_{port}")
    }
    fn build(
        &self,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
        _: Option<Vec<u8>>,
    ) -> Result<Box<dyn Source>, BoxedError> {
        Ok(Box::new(TestSource {}))
    }
}

#[derive(Default)]
struct Output(Vec<TableOperation>);
impl ProcessorChannelForwarder for Output {
    fn send(&mut self, op: TableOperation) {
        self.0.push(op);
    }
}

struct Pipeline {
    processors: HashMap<usize, Box<dyn Processor>>,
    edges: HashMap<(usize, PortHandle), Vec<(usize, PortHandle)>>,
    source: usize,
    sink: usize,
    schema: Schema,
    rows: Vec<Record>,
}

fn schema(value_type: FieldType) -> Schema {
    Schema {
        fields: vec![
            FieldDefinition::new("id".into(), FieldType::Int, false, Default::default()),
            FieldDefinition::new("value".into(), value_type, true, Default::default()),
        ],
        primary_index: vec![],
    }
}

impl Pipeline {
    fn new(sql: &str) -> Self {
        Self::build(sql, vec![schema(FieldType::Int); 3]).unwrap()
    }

    fn build(sql: &str, schemas: Vec<Schema>) -> Result<Self, BoxedError> {
        let runtime = create_test_runtime();
        let mut pipeline = AppPipeline::new_with_default_flags();
        let context = statement_to_pipeline(
            sql,
            &mut pipeline,
            Some("results".into()),
            vec![],
            runtime.clone(),
        )?;
        let output = &context.output_tables_map["results"];
        pipeline.add_sink(
            Box::new(TestSinkFactory::new(vec![DEFAULT_PORT_HANDLE])),
            "sink".into(),
        );
        pipeline.connect_nodes(
            output.node.clone(),
            output.port,
            "sink".into(),
            DEFAULT_PORT_HANDLE,
        );
        let mut sources = AppSourceManager::new();
        let names = ["outer_rows", "inner_rows", "third_rows"];
        let mappings = names
            .iter()
            .enumerate()
            .take(schemas.len())
            .map(|(port, name)| (name.to_string(), port as u16))
            .collect();
        sources.add(
            Box::new(Sources(schemas)),
            AppSourceMappings::new("memory".into(), mappings),
        )?;
        let mut app = App::new(sources);
        app.add_pipeline(pipeline);
        let dag = runtime.block_on(DagSchemas::new(app.into_dag()?))?;
        let graph = dag.graph();
        let mut result = Self {
            processors: HashMap::new(),
            edges: HashMap::new(),
            source: 0,
            sink: 0,
            schema: Schema::default(),
            rows: vec![],
        };
        for node in graph.graph().node_indices() {
            match &graph[node].kind {
                NodeKind::Source(_) => result.source = node.index(),
                NodeKind::Sink(_) => {
                    result.sink = node.index();
                    result.schema = dag.get_node_input_schemas(node)[&DEFAULT_PORT_HANDLE].clone();
                }
                NodeKind::Processor(factory) => {
                    let processor = runtime.block_on(factory.build(
                        dag.get_node_input_schemas(node),
                        dag.get_node_output_schemas(node),
                        EventHub::new(16),
                    ))?;
                    result.processors.insert(node.index(), processor);
                }
            }
        }
        for edge in graph.graph().edge_references() {
            result
                .edges
                .entry((edge.source().index(), edge.weight().output_port))
                .or_default()
                .push((edge.target().index(), edge.weight().input_port));
        }
        Ok(result)
    }

    fn send(&mut self, source_port: PortHandle, op: Operation) -> Vec<Operation> {
        let mut pending =
            VecDeque::from([(self.source, TableOperation::without_id(op, source_port))]);
        let mut emitted = Vec::new();
        while let Some((from, op)) = pending.pop_front() {
            for &(target, port) in self.edges.get(&(from, op.port)).into_iter().flatten() {
                let mut input = op.clone();
                input.port = port;
                if target == self.sink {
                    emitted.push(input.op);
                } else {
                    let mut output = Output::default();
                    self.processors
                        .get_mut(&target)
                        .unwrap()
                        .process(input, &mut output)
                        .unwrap();
                    pending.extend(output.0.into_iter().map(|op| (target, op)));
                }
            }
        }
        for op in &emitted {
            match op {
                Operation::Insert { new } => self.rows.push(new.clone()),
                Operation::Delete { old } => {
                    let index = self
                        .rows
                        .iter()
                        .position(|row| row == old)
                        .expect("retracted row must exist");
                    self.rows.remove(index);
                }
                Operation::Update { old, new } => {
                    let index = self
                        .rows
                        .iter()
                        .position(|row| row == old)
                        .expect("updated row must exist");
                    self.rows[index] = new.clone();
                }
                Operation::BatchInsert { new } => self.rows.extend(new.clone()),
            }
        }
        emitted
    }

    fn expect(&self, rows: &[Record]) {
        let bag = |rows: &[Record]| {
            let mut counts = HashMap::new();
            for row in rows {
                *counts.entry(row.clone()).or_insert(0) += 1;
            }
            counts
        };
        assert_eq!(bag(&self.rows), bag(rows));
    }
}

fn row(id: i64, value: i64) -> Record {
    Record::new(vec![Field::Int(id), Field::Int(value)])
}
fn null_row(id: i64) -> Record {
    Record::new(vec![Field::Int(id), Field::Null])
}
fn insert(new: Record) -> Operation {
    Operation::Insert { new }
}
fn delete(old: Record) -> Operation {
    Operation::Delete { old }
}
fn update(old: Record, new: Record) -> Operation {
    Operation::Update { old, new }
}
const IN_SQL: &str = "SELECT * FROM outer_rows WHERE value IN (SELECT value FROM inner_rows)";

#[test]
fn in_subquery_late_inner_changes_and_duplicates() {
    let mut p = Pipeline::new(IN_SQL);
    assert_eq!(
        p.schema.fields.len(),
        2,
        "SELECT * must hide internal marks"
    );
    assert!(p.send(0, insert(row(1, 10))).is_empty());
    p.send(0, insert(row(1, 10)));
    p.send(0, insert(row(2, 20)));
    p.send(1, insert(row(101, 10)));
    p.expect(&[row(1, 10), row(1, 10)]);
    assert!(p.send(1, insert(row(102, 10))).is_empty());
    assert!(p.send(1, delete(row(101, 10))).is_empty());
    p.send(0, delete(row(1, 10)));
    p.expect(&[row(1, 10)]);
    p.send(1, delete(row(102, 10)));
    p.expect(&[]);
    p.send(1, insert(row(103, 20)));
    p.expect(&[row(2, 20)]);
}

#[test]
fn in_subquery_outer_updates_cover_where_transitions() {
    let mut p = Pipeline::new(IN_SQL);
    p.send(
        1,
        Operation::BatchInsert {
            new: vec![row(101, 10), row(102, 20)],
        },
    );
    p.send(0, insert(row(1, 30)));
    assert!(p.send(0, update(row(1, 30), row(1, 40))).is_empty());
    p.send(0, update(row(1, 40), row(1, 10)));
    p.expect(&[row(1, 10)]);
    assert!(matches!(
        &p.send(0, update(row(1, 10), row(1, 10)))[..],
        [Operation::Update { .. }]
    ));
    assert!(matches!(
        &p.send(0, update(row(1, 10), row(1, 20)))[..],
        [Operation::Update { .. }]
    ));
    p.expect(&[row(1, 20)]);
    p.send(0, update(row(1, 20), row(1, 30)));
    p.expect(&[]);
    p.send(0, delete(row(1, 30)));
    p.send(1, insert(row(103, 30)));
    p.expect(&[]);
}

#[test]
fn in_subquery_inner_updates_are_atomic() {
    let mut p = Pipeline::new(IN_SQL);
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 20)],
        },
    );
    p.send(1, insert(row(101, 10)));
    assert!(p.send(1, update(row(101, 10), row(102, 10))).is_empty());
    p.send(1, update(row(102, 10), row(102, 20)));
    p.expect(&[row(2, 20)]);
}

#[test]
fn in_subquery_null_and_empty_set_truth_tables() {
    for negated in [false, true] {
        let sql = format!(
            "SELECT * FROM outer_rows WHERE value {}IN (SELECT value FROM inner_rows)",
            if negated { "NOT " } else { "" }
        );
        let mut p = Pipeline::new(&sql);
        p.send(
            0,
            Operation::BatchInsert {
                new: vec![row(1, 10), null_row(2)],
            },
        );
        let expected = if negated {
            vec![row(1, 10), null_row(2)]
        } else {
            vec![]
        };
        p.expect(&expected);
        p.send(1, insert(null_row(101)));
        p.expect(&[]);
        p.send(1, insert(row(102, 10)));
        let expected = if negated { vec![] } else { vec![row(1, 10)] };
        p.expect(&expected);
        p.send(1, delete(null_row(101)));
        let expected = if negated { vec![] } else { vec![row(1, 10)] };
        p.expect(&expected);
        p.send(1, delete(row(102, 10)));
        let expected = if negated {
            vec![row(1, 10), null_row(2)]
        } else {
            vec![]
        };
        p.expect(&expected);
    }
}

#[test]
fn in_subquery_compound_predicates_preserve_three_valued_logic() {
    let mut p = Pipeline::new("SELECT * FROM outer_rows WHERE (value IN (SELECT value FROM inner_rows) OR id = 1) AND value NOT IN (SELECT value FROM third_rows)");
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 20)],
        },
    );
    p.expect(&[row(1, 10)]);
    assert!(
        p.send(1, insert(row(101, 10))).is_empty(),
        "unchanged OR arm must not emit a spurious update"
    );
    p.send(1, insert(row(102, 20)));
    p.expect(&[row(1, 10), row(2, 20)]);
    p.send(2, insert(null_row(201)));
    p.expect(&[]);
    p.send(2, delete(null_row(201)));
    p.expect(&[row(1, 10), row(2, 20)]);
    p.send(2, insert(row(202, 20)));
    p.expect(&[row(1, 10)]);
}

#[test]
fn in_subquery_nested_select_and_cte() {
    let mut p = Pipeline::new("WITH allowed AS (SELECT value FROM third_rows) SELECT * FROM outer_rows WHERE value IN (SELECT value FROM inner_rows WHERE value IN (SELECT value FROM allowed))");
    p.send(0, insert(row(1, 10)));
    p.send(1, insert(row(101, 10)));
    p.expect(&[]);
    p.send(2, insert(row(201, 10)));
    p.expect(&[row(1, 10)]);
    p.send(2, delete(row(201, 10)));
    p.expect(&[]);
}

#[test]
fn in_subquery_mixed_numeric_and_string_values_use_sql_equality() {
    for (typ, value) in [
        (FieldType::UInt, Field::UInt(10)),
        (FieldType::String, Field::String("10".into())),
    ] {
        let mut p = Pipeline::build(IN_SQL, vec![schema(FieldType::Int), schema(typ)]).unwrap();
        p.send(0, insert(row(1, 10)));
        let inner = Record::new(vec![Field::Int(101), value]);
        p.send(1, insert(inner.clone()));
        p.expect(&[row(1, 10)]);
        p.send(1, delete(inner));
        p.expect(&[]);
    }
}

#[test]
fn in_subquery_validates_width_and_correlated_references() {
    let error = Pipeline::build(
        "SELECT * FROM outer_rows WHERE value IN (SELECT * FROM inner_rows)",
        vec![schema(FieldType::Int); 2],
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("exactly one column"), "{error}");
    assert!(Pipeline::build("SELECT * FROM outer_rows o WHERE value IN (SELECT value FROM inner_rows i WHERE i.id = o.id)", vec![schema(FieldType::Int); 2]).is_err());
}

#[test]
fn in_subquery_left_expression_and_filtered_inner() {
    let mut p = Pipeline::new("SELECT id FROM outer_rows WHERE value + 1 IN (SELECT value FROM inner_rows WHERE id > 100)");
    p.send(0, insert(row(1, 10)));
    p.send(1, insert(row(99, 11)));
    p.expect(&[]);
    p.send(1, update(row(99, 11), row(101, 11)));
    p.expect(&[Record::new(vec![Field::Int(1)])]);
    p.send(1, update(row(101, 11), row(99, 11)));
    p.expect(&[]);
}

#[test]
fn in_subquery_deterministic_stream_matches_snapshot_oracle() {
    for negated in [false, true] {
        let sql = format!(
            "SELECT * FROM outer_rows WHERE value {}IN (SELECT value FROM inner_rows)",
            if negated { "NOT " } else { "" }
        );
        let mut p = Pipeline::new(&sql);
        let mut tables: [Vec<Record>; 2] = Default::default();
        let mut seed = 7_u64;
        for step in 0..180 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let side = ((seed >> 32) & 1) as usize;
            let value = ((seed >> 36) % 5) as i64;
            let new = if value == 4 {
                null_row(step)
            } else {
                row(step, value)
            };
            let change = match (seed >> 40) % 3 {
                0 if !tables[side].is_empty() => delete(tables[side].remove(0)),
                1 if !tables[side].is_empty() => {
                    update(std::mem::replace(&mut tables[side][0], new.clone()), new)
                }
                _ => {
                    tables[side].push(new.clone());
                    insert(new)
                }
            };
            p.send(side as u16, change);
            let expected: Vec<_> = tables[0]
                .iter()
                .filter(|outer| {
                    let matched = tables[1].iter().any(|inner| {
                        outer.values[1] != Field::Null && outer.values[1] == inner.values[1]
                    });
                    if negated {
                        tables[1].is_empty()
                            || (!matched
                                && outer.values[1] != Field::Null
                                && tables[1].iter().all(|inner| inner.values[1] != Field::Null))
                    } else {
                        matched
                    }
                })
                .cloned()
                .collect();
            p.expect(&expected);
        }
    }
}

#[test]
fn in_subquery_negation_keeps_unknown_compounds_unknown() {
    for predicate in [
        "NOT (value IN (SELECT value FROM inner_rows) AND 1 = 1)",
        "NOT (value IN (SELECT value FROM inner_rows) OR 1 = 2)",
        "NOT (1 = 2 OR value IN (SELECT value FROM inner_rows))",
    ] {
        let mut p = Pipeline::new(&format!("SELECT * FROM outer_rows WHERE {predicate}"));
        p.send(1, insert(null_row(101)));
        p.send(0, insert(row(1, 10)));
        p.expect(&[]);
    }
}

#[test]
fn in_subquery_inner_cte_shadows_without_leaking() {
    let mut p = Pipeline::new("WITH ids AS (SELECT value FROM third_rows) SELECT * FROM outer_rows WHERE value IN (WITH ids AS (SELECT value FROM inner_rows) SELECT value FROM ids) AND value IN (SELECT value FROM ids)");
    p.send(0, insert(row(1, 10)));
    p.send(1, insert(row(101, 10)));
    p.expect(&[]);
    p.send(2, insert(row(201, 10)));
    p.expect(&[row(1, 10)]);
}

// Supplemental cases for dozer-sql/src/tests/in_subquery.rs.
// Uses its real SQL-to-DAG Pipeline fixture. Kept outside the root checkout
// while the implementation is being edited by the parent agent.

#[test]
fn in_subquery_review_duplicate_null_batch_is_one_membership_change() {
    let mut p = Pipeline::new(IN_SQL);
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(1, 10), row(2, 20), null_row(3)],
        },
    );
    let emitted = p.send(
        1,
        Operation::BatchInsert {
            new: vec![row(101, 10), null_row(102), row(103, 10)],
        },
    );
    assert_eq!(emitted.len(), 2, "each outer duplicate enters once");
    assert!(emitted
        .iter()
        .all(|op| matches!(op, Operation::Insert { .. })));
    p.expect(&[row(1, 10), row(1, 10)]);
    assert!(p.send(1, delete(row(101, 10))).is_empty());
    assert_eq!(p.send(1, delete(row(103, 10))).len(), 2);
    p.expect(&[]);
    assert!(p.send(1, delete(null_row(102))).is_empty());
}

#[test]
fn in_subquery_review_not_in_batch_null_then_last_match_removal() {
    let mut p =
        Pipeline::new("SELECT * FROM outer_rows WHERE value NOT IN (SELECT value FROM inner_rows)");
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 20), null_row(3)],
        },
    );
    let emitted = p.send(
        1,
        Operation::BatchInsert {
            new: vec![row(101, 10), null_row(102), row(103, 10)],
        },
    );
    assert_eq!(emitted.len(), 3);
    assert!(emitted
        .iter()
        .all(|op| matches!(op, Operation::Delete { .. })));
    p.expect(&[]);
    assert!(p.send(1, delete(row(101, 10))).is_empty());
    assert_eq!(p.send(1, delete(null_row(102))).len(), 1);
    p.expect(&[row(2, 20)]);
    assert_eq!(p.send(1, delete(row(103, 10))).len(), 2);
    p.expect(&[row(1, 10), row(2, 20), null_row(3)]);
}

#[test]
fn in_subquery_review_distinct_cross_type_keys_share_membership_count() {
    let mut p = Pipeline::build(
        IN_SQL,
        vec![schema(FieldType::Int), schema(FieldType::String)],
    )
    .unwrap();
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 11)],
        },
    );
    let string_row =
        |id, value: &str| Record::new(vec![Field::Int(id), Field::String(value.into())]);
    p.send(
        1,
        Operation::BatchInsert {
            new: vec![string_row(101, "10"), string_row(102, "010")],
        },
    );
    p.expect(&[row(1, 10)]);
    assert!(p.send(1, delete(string_row(101, "10"))).is_empty());
    p.send(1, update(string_row(102, "010"), string_row(102, "11")));
    p.expect(&[row(2, 11)]);
}

#[test]
fn in_subquery_review_outer_and_inner_share_source() {
    let mut p =
        Pipeline::new("SELECT * FROM outer_rows WHERE value IN (SELECT value FROM outer_rows)");
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(1, 10), null_row(2)],
        },
    );
    p.expect(&[row(1, 10), row(1, 10)]);
    p.send(0, update(row(1, 10), row(1, 20)));
    p.expect(&[row(1, 10), row(1, 20)]);
    p.send(0, delete(row(1, 10)));
    p.expect(&[row(1, 20)]);
    p.send(0, update(null_row(2), row(2, 20)));
    p.expect(&[row(1, 20), row(2, 20)]);
    p.send(0, delete(row(1, 20)));
    p.send(0, delete(row(2, 20)));
    p.expect(&[]);
}

#[test]
fn in_subquery_review_ttl_rejection_preserves_both_ports_state() {
    use crate::product::join::mark::MarkJoinProcessorFactory;
    use dozer_core::node::ProcessorFactory;
    use dozer_sql_expression::sqlparser::ast::{Expr, Ident};
    use dozer_types::types::Lifetime;

    let runtime = create_test_runtime();
    let factory = MarkJoinProcessorFactory::new(
        "mark".into(),
        Expr::Identifier(Ident::new("value")),
        "membership".into(),
        false,
        vec![],
        runtime.clone(),
    );
    let inputs = HashMap::from([
        (0, schema(FieldType::Int)),
        (
            1,
            Schema {
                fields: vec![FieldDefinition::new(
                    "value".into(),
                    FieldType::Int,
                    true,
                    Default::default(),
                )],
                primary_index: vec![],
            },
        ),
    ]);
    let outer = row(1, 10);
    let inner = Record::new(vec![Field::Int(10)]);
    for port in [0, 1] {
        let normal = if port == 0 {
            outer.clone()
        } else {
            inner.clone()
        };
        let mut expiring = normal.clone();
        expiring.lifetime = Some(Lifetime {
            reference: "2024-01-01T00:00:00Z".parse().unwrap(),
            duration: std::time::Duration::from_secs(10),
        });
        for rejected in [
            insert(expiring.clone()),
            delete(expiring.clone()),
            update(expiring.clone(), normal.clone()),
            update(normal.clone(), expiring.clone()),
            Operation::BatchInsert {
                new: vec![normal.clone(), expiring.clone()],
            },
        ] {
            let mut processor = runtime
                .block_on(factory.build(inputs.clone(), HashMap::new(), EventHub::new(16)))
                .unwrap();
            let mut output = Output::default();
            processor
                .process(
                    TableOperation::without_id(insert(inner.clone()), 1),
                    &mut output,
                )
                .unwrap();
            processor
                .process(
                    TableOperation::without_id(insert(outer.clone()), 0),
                    &mut output,
                )
                .unwrap();
            output.0.clear();
            let error = processor
                .process(TableOperation::without_id(rejected, port), &mut output)
                .unwrap_err();
            assert!(
                error.to_string().contains("do not support TTL inputs"),
                "{error}"
            );
            assert!(output.0.is_empty());

            processor
                .process(
                    TableOperation::without_id(delete(inner.clone()), 1),
                    &mut output,
                )
                .unwrap();
            let mut old = outer.clone();
            old.values.push(Field::Boolean(true));
            let mut new = outer.clone();
            new.values.push(Field::Boolean(false));
            assert_eq!(
                output.0.iter().map(|op| op.op.clone()).collect::<Vec<_>>(),
                vec![update(old, new.clone())]
            );
            output.0.clear();
            processor
                .process(
                    TableOperation::without_id(delete(outer.clone()), 0),
                    &mut output,
                )
                .unwrap();
            assert_eq!(
                output.0.iter().map(|op| op.op.clone()).collect::<Vec<_>>(),
                vec![delete(new)]
            );
        }
    }
}

#[test]
fn in_subquery_review_incompatible_comparison_rejects_without_state_change() {
    use crate::product::join::mark::MarkJoinProcessorFactory;
    use dozer_core::node::ProcessorFactory;
    use dozer_sql_expression::sqlparser::ast::{Expr, Ident};

    let runtime = create_test_runtime();
    let factory = MarkJoinProcessorFactory::new(
        "mark".into(),
        Expr::Identifier(Ident::new("value")),
        "membership".into(),
        true,
        vec![],
        runtime.clone(),
    );
    let inputs = HashMap::from([
        (0, schema(FieldType::Boolean)),
        (
            1,
            Schema {
                fields: vec![FieldDefinition::new(
                    "value".into(),
                    FieldType::Binary,
                    true,
                    Default::default(),
                )],
                primary_index: vec![],
            },
        ),
    ]);
    let outer = Record::new(vec![Field::Int(1), Field::Boolean(true)]);
    let inner = Record::new(vec![Field::Binary(vec![1])]);
    let mut included = outer.clone();
    included.values.push(Field::Boolean(true));
    for first_port in [0, 1] {
        let mut processor = runtime
            .block_on(factory.build(inputs.clone(), HashMap::new(), EventHub::new(16)))
            .unwrap();
        let mut output = Output::default();
        let first = if first_port == 0 {
            outer.clone()
        } else {
            inner.clone()
        };
        let second = if first_port == 0 {
            inner.clone()
        } else {
            outer.clone()
        };
        processor
            .process(
                TableOperation::without_id(insert(first.clone()), first_port),
                &mut output,
            )
            .unwrap();
        output.0.clear();
        let error = processor
            .process(
                TableOperation::without_id(insert(second), 1 - first_port),
                &mut output,
            )
            .unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("compar"),
            "{error}"
        );
        assert!(output.0.is_empty());
        processor
            .process(
                TableOperation::without_id(delete(first), first_port),
                &mut output,
            )
            .unwrap();
        let expected = if first_port == 0 {
            vec![delete(included.clone())]
        } else {
            vec![]
        };
        assert_eq!(
            output.0.iter().map(|op| op.op.clone()).collect::<Vec<_>>(),
            expected
        );
        output.0.clear();
        processor
            .process(
                TableOperation::without_id(insert(outer.clone()), 0),
                &mut output,
            )
            .unwrap();
        assert_eq!(
            output.0.iter().map(|op| op.op.clone()).collect::<Vec<_>>(),
            vec![insert(included.clone())]
        );
    }
}

#[test]
fn in_subquery_review_logical_output_schema_remains_nullable() {
    for expression in [
        "value AND (1 = 1)",
        "value OR (1 = 2)",
        "NOT (value AND (1 = 1))",
    ] {
        let sql = format!("SELECT {expression} AS result FROM outer_rows");
        let mut p = Pipeline::build(&sql, vec![schema(FieldType::Boolean)]).unwrap();
        assert!(p.schema.fields[0].nullable, "{expression} can produce NULL");
        p.send(0, insert(null_row(1)));
        p.expect(&[Record::new(vec![Field::Null])]);
    }
}

#[test]
fn in_subquery_review_literal_null_membership_empty_and_nonempty() {
    for negated in [false, true] {
        let sql = format!(
            "SELECT * FROM outer_rows WHERE NULL {}IN (SELECT value FROM inner_rows)",
            if negated { "NOT " } else { "" }
        );
        let mut p = Pipeline::new(&sql);
        let expected_empty = if negated { vec![row(1, 10)] } else { vec![] };
        p.send(0, insert(row(1, 10)));
        p.expect(&expected_empty);
        p.send(1, insert(null_row(101)));
        p.expect(&[]);
        p.send(1, delete(null_row(101)));
        p.expect(&expected_empty);
        p.send(1, insert(row(102, 10)));
        p.expect(&[]);
        p.send(1, delete(row(102, 10)));
        p.expect(&expected_empty);
    }
}

#[test]
fn in_subquery_review_parenthesized_union_branch() {
    let mut p = Pipeline::new("SELECT * FROM outer_rows WHERE value IN (SELECT value FROM inner_rows UNION (SELECT value FROM third_rows))");
    p.send(0, insert(row(1, 10)));
    p.send(1, insert(row(101, 10)));
    p.expect(&[row(1, 10)]);
    p.send(2, insert(row(201, 10)));
    p.send(1, delete(row(101, 10)));
    p.expect(&[row(1, 10)]);
    p.send(2, delete(row(201, 10)));
    p.expect(&[]);
}

#[test]
fn in_subquery_review_parenthesized_cte_query_body() {
    let mut p = Pipeline::new("SELECT * FROM outer_rows WHERE value IN (WITH allowed AS (SELECT value FROM inner_rows) (SELECT value FROM allowed))");
    p.send(0, insert(row(1, 10)));
    p.send(1, insert(row(101, 10)));
    p.expect(&[row(1, 10)]);
    p.send(1, delete(row(101, 10)));
    p.expect(&[]);
}

#[test]
fn in_subquery_review_parenthesized_union_cte_does_not_leak_to_sibling() {
    let mut p = Pipeline::new("SELECT * FROM outer_rows WHERE value IN (WITH allowed AS (SELECT value FROM third_rows) (WITH allowed AS (SELECT value FROM inner_rows) SELECT value FROM allowed) UNION SELECT value FROM allowed)");
    p.send(
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 20)],
        },
    );
    p.send(1, insert(row(101, 10)));
    p.expect(&[row(1, 10)]);
    p.send(2, insert(row(201, 20)));
    p.expect(&[row(1, 10), row(2, 20)]);
    p.send(1, delete(row(101, 10)));
    p.expect(&[row(2, 20)]);
    p.send(2, delete(row(201, 20)));
    p.expect(&[]);
}

#[test]
fn in_subquery_demo() {
    let mut p = Pipeline::new(IN_SQL);
    println!("SQL: {IN_SQL}");
    let step = |p: &mut Pipeline, label: &str, port, input: Operation, expected: &[Record]| {
        println!("\n{label}\nSource {port}: {input:?}");
        let emitted = p.send(port, input);
        println!("Emitted: {emitted:?}\nResult rows: {:?}", p.rows);
        p.expect(expected);
    };
    step(
        &mut p,
        "Outer batch waits for membership",
        0,
        Operation::BatchInsert {
            new: vec![row(1, 10), row(2, 20)],
        },
        &[],
    );
    step(
        &mut p,
        "Inner insert admits the matching outer row",
        1,
        insert(row(101, 10)),
        &[row(1, 10)],
    );
    step(
        &mut p,
        "Inner duplicate does not multiply the result",
        1,
        insert(row(102, 10)),
        &[row(1, 10)],
    );
    step(
        &mut p,
        "Removing one duplicate preserves membership",
        1,
        delete(row(102, 10)),
        &[row(1, 10)],
    );
    step(
        &mut p,
        "Inner key update retracts old membership and admits the new one",
        1,
        update(row(101, 10), row(101, 20)),
        &[row(2, 20)],
    );
    step(
        &mut p,
        "Last matching inner delete retracts the outer row",
        1,
        delete(row(101, 20)),
        &[],
    );
}
