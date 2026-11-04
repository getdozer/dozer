use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    channels::ProcessorChannelForwarder,
    event::EventHub,
    node::{Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::sqlparser::{
    ast::{SetExpr, Statement},
    dialect::DozerDialect,
    parser::Parser,
};
use dozer_sql_expression::{builder::ExpressionBuilder, error::Error as ExpressionError};
use dozer_types::{
    node::OpIdentifier,
    types::{
        Field, FieldDefinition, FieldType, Lifetime, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};

use super::{membership_port, plan::Plan, DynamicSelectionFactory};
use crate::errors::PipelineError;

#[derive(Default)]
struct Output(Vec<TableOperation>);
impl ProcessorChannelForwarder for Output {
    fn send(&mut self, op: TableOperation) {
        self.0.push(op);
    }
}

fn row(value: i64) -> Record {
    Record::new(vec![Field::Int(value)])
}
fn null() -> Record {
    Record::new(vec![Field::Null])
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

fn schema(typ: FieldType) -> Schema {
    Schema::default()
        .field(
            FieldDefinition::new("value".into(), typ, true, SourceDefinition::Dynamic),
            false,
        )
        .clone()
}

fn factory(predicate: &str) -> DynamicSelectionFactory {
    let sql = format!("SELECT value FROM outer_rows WHERE {predicate}");
    let Statement::Query(query) = Parser::parse_sql(&DozerDialect {}, &sql).unwrap().remove(0)
    else {
        panic!()
    };
    let SetExpr::Select(select) = *query.body else {
        panic!()
    };
    let mut count = 0;
    let plan = Plan::parse(select.selection.unwrap(), &mut |_| {
        count += 1;
        Ok(count)
    })
    .unwrap();
    DynamicSelectionFactory {
        id: "test".into(),
        plan,
        input_count: count,
        udfs: vec![],
        runtime: Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        ),
    }
}

fn processor(predicate: &str) -> Box<dyn Processor> {
    let factory = factory(predicate);
    let inputs = factory
        .get_input_ports()
        .into_iter()
        .map(|port| (port, schema(FieldType::Int)))
        .collect();
    factory
        .runtime
        .block_on(factory.build(inputs, HashMap::new(), EventHub::new(1)))
        .unwrap()
}

fn send(processor: &mut dyn Processor, port: u16, op: Operation) -> Vec<Operation> {
    let mut output = Output::default();
    processor
        .process(TableOperation::without_id(op, port), &mut output)
        .unwrap();
    assert!(output.0.iter().all(|op| op.port == DEFAULT_PORT_HANDLE));
    output.0.into_iter().map(|op| op.op).collect()
}

#[test]
fn factory_and_processor_ports_match_the_engine_contract() {
    let factory =
        factory("value IN (SELECT value FROM members) AND value NOT IN (SELECT value FROM others)");
    assert_eq!(factory.get_input_ports(), vec![DEFAULT_PORT_HANDLE, 1, 2]);
    assert_eq!(factory.get_output_ports(), vec![DEFAULT_PORT_HANDLE]);
    let inputs: HashMap<_, _> = factory
        .get_input_ports()
        .into_iter()
        .map(|port| (port, schema(FieldType::Int)))
        .collect();
    let output = factory
        .runtime
        .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &inputs))
        .unwrap();
    assert_eq!(output, inputs[&DEFAULT_PORT_HANDLE]);
    let mut p = factory
        .runtime
        .block_on(factory.build(inputs.clone(), HashMap::new(), EventHub::new(1)))
        .unwrap();
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty());
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![insert(row(1))]);
    for port in [0, 3] {
        let mut output = Output::default();
        let error = p
            .process(
                TableOperation::without_id(insert(row(1)), port),
                &mut output,
            )
            .unwrap_err();
        assert!(matches!(error.downcast_ref::<PipelineError>(),
            Some(PipelineError::InvalidPortHandle(actual)) if *actual == port));
        assert!(output.0.is_empty());
    }
    assert_eq!(send(&mut *p, 2, insert(row(1))), vec![delete(row(1))]);
    assert_eq!(send(&mut *p, 2, delete(row(1))), vec![insert(row(1))]);
    for missing in [DEFAULT_PORT_HANDLE, 1, 2] {
        let mut incomplete = inputs.clone();
        incomplete.remove(&missing);
        let error = factory
            .runtime
            .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &incomplete))
            .unwrap_err();
        assert!(matches!(error.downcast_ref::<PipelineError>(),
            Some(PipelineError::InvalidPortHandle(actual)) if *actual == missing));
    }
    let mut unexpected = inputs.clone();
    unexpected.insert(0, schema(FieldType::Int));
    let error = factory
        .runtime
        .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &unexpected))
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<PipelineError>(),
        Some(PipelineError::InvalidPortHandle(0))
    ));
    let error = factory
        .runtime
        .block_on(factory.get_output_schema(&0, &inputs))
        .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<PipelineError>(),
        Some(PipelineError::InvalidPortHandle(0))
    ));
}

#[test]
fn membership_ports_cannot_alias_the_outer_port_or_overflow() {
    assert_eq!(membership_port(0).unwrap(), 1);
    assert_eq!(
        membership_port(usize::from(DEFAULT_PORT_HANDLE) - 2).unwrap(),
        DEFAULT_PORT_HANDLE - 1
    );
    for index in [
        usize::from(DEFAULT_PORT_HANDLE) - 1,
        usize::from(DEFAULT_PORT_HANDLE),
        usize::MAX,
    ] {
        assert!(
            matches!(membership_port(index), Err(PipelineError::InvalidQuery(message))
            if message == "Too many IN subqueries in one filter")
        );
    }
    let mut factory = factory("value IN (SELECT value FROM members)");
    factory.input_count = DEFAULT_PORT_HANDLE;
    assert_eq!(
        factory
            .get_input_ports()
            .iter()
            .filter(|port| **port == DEFAULT_PORT_HANDLE)
            .count(),
        1
    );
    let error = factory
        .runtime
        .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &HashMap::new()))
        .unwrap_err();
    assert!(matches!(error.downcast_ref::<PipelineError>(),
        Some(PipelineError::InvalidQuery(message)) if message == "Too many IN subqueries in one filter"));
}

#[test]
fn duplicate_membership_and_outer_multiplicity() {
    let mut p = processor("value IN (SELECT value FROM members)");
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty());
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty());
    assert_eq!(
        send(&mut *p, 1, insert(row(1))),
        vec![insert(row(1)), insert(row(1))]
    );
    assert!(send(&mut *p, 1, insert(row(1))).is_empty());
    assert!(send(&mut *p, 1, delete(row(1))).is_empty());
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, delete(row(1))),
        vec![delete(row(1))]
    );
    assert_eq!(send(&mut *p, 1, delete(row(1))), vec![delete(row(1))]);
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, delete(row(1))).is_empty());
    assert!(send(&mut *p, 1, insert(row(1))).is_empty());
}

#[test]
fn all_outer_update_transitions_and_ids() {
    let mut p = processor("value IN (SELECT value FROM members)");
    send(
        &mut *p,
        1,
        Operation::BatchInsert {
            new: vec![row(1), row(2)],
        },
    );
    send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1)));
    let id = Some(OpIdentifier {
        txid: 7,
        seq_in_tx: 9,
    });
    let mut output = Output::default();
    p.process(
        TableOperation {
            id,
            port: DEFAULT_PORT_HANDLE,
            op: update(row(1), row(2)),
        },
        &mut output,
    )
    .unwrap();
    assert_eq!(
        output.0,
        vec![TableOperation {
            id,
            port: DEFAULT_PORT_HANDLE,
            op: update(row(1), row(2))
        }]
    );
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, update(row(2), row(3))),
        vec![delete(row(2))]
    );
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, update(row(3), row(4))).is_empty());
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, update(row(4), row(1))),
        vec![insert(row(1))]
    );
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, update(row(1), row(1))),
        vec![update(row(1), row(1))]
    );
    assert_eq!(send(&mut *p, 1, delete(row(1))), vec![delete(row(1))]);
}

#[test]
fn inner_updates_and_batches_are_atomic() {
    let mut p = processor("value IN (SELECT value FROM members) OR value = 9");
    send(
        &mut *p,
        DEFAULT_PORT_HANDLE,
        Operation::BatchInsert {
            new: vec![row(1), row(2), row(9)],
        },
    );
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![insert(row(1))]);
    assert!(send(&mut *p, 1, update(row(1), row(1))).is_empty());
    let outputs = send(&mut *p, 1, update(row(1), row(2)));
    assert_eq!(outputs.len(), 2);
    assert!(outputs.contains(&delete(row(1))) && outputs.contains(&insert(row(2))));
    assert_eq!(
        send(
            &mut *p,
            1,
            Operation::BatchInsert {
                new: vec![row(1), row(1), row(2)]
            }
        ),
        vec![insert(row(1))]
    );
    assert!(send(&mut *p, 1, delete(row(2))).is_empty());
    assert_eq!(send(&mut *p, 1, delete(row(2))), vec![delete(row(2))]);
}

#[test]
fn null_empty_and_negation_truth_table() {
    for negated in [false, true] {
        let predicate = format!(
            "value {}IN (SELECT value FROM members)",
            if negated { "NOT " } else { "" }
        );
        for value in [null(), row(1), row(2)] {
            for inner in [vec![], vec![null()], vec![row(1)], vec![row(1), null()]] {
                let mut p = processor(&predicate);
                send(&mut *p, 1, Operation::BatchInsert { new: inner.clone() });
                let truth = if inner.is_empty() {
                    Some(false)
                } else if value == null() {
                    None
                } else if inner.contains(&value) {
                    Some(true)
                } else if inner.contains(&null()) {
                    None
                } else {
                    Some(false)
                };
                let selected = truth.map(|truth| truth != negated) == Some(true);
                assert_eq!(
                    !send(&mut *p, DEFAULT_PORT_HANDLE, insert(value.clone())).is_empty(),
                    selected,
                    "value={value:?}, inner={inner:?}, negated={negated}"
                );
            }
        }
    }
}

#[test]
fn null_boundaries_retract_not_in_including_null_outer() {
    let mut p = processor("value NOT IN (SELECT value FROM members)");
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, insert(null())),
        vec![insert(null())]
    );
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![delete(null())]);
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(2))),
        vec![insert(row(2))]
    );
    assert_eq!(
        send(&mut *p, 1, update(row(1), null())),
        vec![delete(row(2))]
    );
    let outputs = send(&mut *p, 1, delete(null()));
    assert_eq!(outputs.len(), 2);
    assert!(outputs.contains(&insert(null())) && outputs.contains(&insert(row(2))));
}

#[test]
fn compound_three_valued_predicates_preserve_unknown() {
    for predicate in [
        "NOT (value IN (SELECT value FROM members) AND value = 1)",
        "NOT (value IN (SELECT value FROM members) OR value = 2)",
        "(value IN (SELECT value FROM members)) IS TRUE",
        "CASE WHEN value IN (SELECT value FROM members) THEN 1 ELSE 0 END = 1",
        "CAST(value IN (SELECT value FROM members) AS STRING) = 'TRUE'",
        "value IN (SELECT value FROM members) AND value = NULL",
    ] {
        let mut p = processor(predicate);
        send(&mut *p, 1, insert(null()));
        assert!(
            send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty(),
            "{predicate}"
        );
    }
    let mut p = processor("(value IN (SELECT value FROM members)) IS UNKNOWN");
    send(&mut *p, 1, insert(null()));
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))),
        vec![insert(row(1))]
    );
}

#[test]
fn supported_cast_tracks_false_unknown_and_true_membership() {
    let mut p = processor("CAST(value IN (SELECT value FROM members) AS STRING) = 'TRUE'");
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty());
    assert!(send(&mut *p, 1, insert(null())).is_empty());
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![insert(row(1))]);
    assert_eq!(send(&mut *p, 1, delete(row(1))), vec![delete(row(1))]);
    assert!(send(&mut *p, 1, delete(null())).is_empty());
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![insert(row(1))]);
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, delete(row(1))),
        vec![delete(row(1))]
    );
}

#[test]
fn casts_keep_existing_static_types_and_nullable_value_conversion() {
    let factory = factory("CAST(value IN (SELECT value FROM members) AS INT) = 1");
    let static_schema = schema(FieldType::Boolean);
    for target in ["STRING", "INT"] {
        let sql = format!(
            "SELECT value FROM outer_rows WHERE CAST(value AS {target}) = {}",
            if target == "STRING" { "'TRUE'" } else { "1" }
        );
        let Statement::Query(query) = Parser::parse_sql(&DozerDialect {}, &sql).unwrap().remove(0)
        else {
            panic!()
        };
        let SetExpr::Select(select) = *query.body else {
            panic!()
        };
        let mut expression = factory
            .runtime
            .block_on(ExpressionBuilder::new(1, factory.runtime.clone()).build(
                false,
                &select.selection.unwrap(),
                &static_schema,
                &[],
            ))
            .unwrap();
        if target == "STRING" {
            assert_eq!(
                expression.get_type(&static_schema).unwrap().return_type,
                FieldType::Boolean
            );
            for (value, expected) in [
                (Field::Boolean(false), false),
                (Field::Null, false),
                (Field::Boolean(true), true),
            ] {
                assert_eq!(
                    expression
                        .evaluate(&Record::new(vec![value]), &static_schema)
                        .unwrap(),
                    Field::Boolean(expected)
                );
            }
        } else {
            let static_error = expression
                .get_type(&static_schema)
                .err()
                .expect("Boolean to INT must remain unsupported");
            assert_unsupported_boolean_int_cast(&static_error);
        }
    }
    let inputs = [
        (DEFAULT_PORT_HANDLE, schema(FieldType::Int)),
        (1, schema(FieldType::Int)),
    ]
    .into_iter()
    .collect();
    let error = factory
        .runtime
        .block_on(factory.build(inputs, HashMap::new(), EventHub::new(1)))
        .err()
        .unwrap();
    let Some(PipelineError::Expression(error)) = error.downcast_ref::<PipelineError>() else {
        panic!("wrong error: {error:?}")
    };
    assert_unsupported_boolean_int_cast(error);
}

fn assert_unsupported_boolean_int_cast(error: &ExpressionError) {
    let ExpressionError::InvalidFunctionArgumentType {
        function_name,
        argument_index,
        expected,
        actual,
    } = error
    else {
        panic!("wrong error: {error:?}")
    };
    assert_eq!(function_name, "CAST AS INT");
    assert_eq!(*argument_index, 0);
    assert_eq!(*actual, FieldType::Boolean);
    assert_eq!(
        expected,
        &vec![
            FieldType::Int,
            FieldType::Int8,
            FieldType::String,
            FieldType::UInt,
            FieldType::I128,
            FieldType::U128,
            FieldType::Json
        ]
    );
}

#[test]
fn case_does_not_evaluate_an_unselected_result() {
    let mut p =
        processor("CASE WHEN value IN (SELECT value FROM members) THEN 1 ELSE 1 / 0 END = 1");
    send(&mut *p, 1, insert(row(1)));
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))),
        vec![insert(row(1))]
    );
}

#[test]
fn multiple_membership_inputs_and_static_null_compatibility() {
    let mut p = processor("(value IN (SELECT value FROM a) AND value NOT IN (SELECT value FROM b)) OR value IN (NULL)");
    assert_eq!(
        send(&mut *p, DEFAULT_PORT_HANDLE, insert(null())),
        vec![insert(null())]
    );
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1))).is_empty());
    assert_eq!(send(&mut *p, 1, insert(row(1))), vec![insert(row(1))]);
    assert_eq!(send(&mut *p, 2, insert(row(1))), vec![delete(row(1))]);
    assert_eq!(send(&mut *p, 2, delete(row(1))), vec![insert(row(1))]);
}

#[test]
fn lifetime_and_primary_key_schema_are_preserved() {
    let factory = factory("value IN (SELECT value FROM members)");
    let mut outer = schema(FieldType::Int);
    outer.primary_index = vec![0];
    let inputs = [
        (DEFAULT_PORT_HANDLE, outer.clone()),
        (1, schema(FieldType::Int)),
    ]
    .into_iter()
    .collect();
    let output_schema = factory
        .runtime
        .block_on(factory.get_output_schema(&DEFAULT_PORT_HANDLE, &inputs))
        .unwrap();
    assert_eq!(output_schema, outer);
    let mut p = factory
        .runtime
        .block_on(factory.build(inputs, HashMap::new(), EventHub::new(1)))
        .unwrap();
    let mut record = row(1);
    record.lifetime = Some(Lifetime {
        reference: dozer_types::chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .unwrap(),
        duration: std::time::Duration::from_secs(60),
    });
    send(&mut *p, DEFAULT_PORT_HANDLE, insert(record.clone()));
    assert_eq!(
        send(&mut *p, 1, insert(row(1))),
        vec![insert(record.clone())]
    );
    assert_eq!(send(&mut *p, 1, delete(row(1))), vec![delete(record)]);
}

#[test]
fn invalid_changes_do_not_mutate_or_emit_partial_state() {
    let mut p = processor("value IN (SELECT value FROM members)");
    send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(1)));
    send(&mut *p, 1, insert(row(1)));
    for (port, op) in [
        (8, insert(row(1))),
        (1, delete(row(8))),
        (DEFAULT_PORT_HANDLE, delete(row(8))),
        (1, update(row(1), Record::new(vec![]))),
        (
            1,
            Operation::BatchInsert {
                new: vec![row(2), Record::new(vec![])],
            },
        ),
    ] {
        let mut output = Output::default();
        assert!(p
            .process(TableOperation::without_id(op, port), &mut output)
            .is_err());
        assert!(output.0.is_empty());
    }
    assert_eq!(send(&mut *p, 1, delete(row(1))), vec![delete(row(1))]);
    assert!(send(&mut *p, DEFAULT_PORT_HANDLE, insert(row(2))).is_empty());
}

#[test]
fn subquery_schema_and_type_errors_are_explicit() {
    let factory = factory("value IN (SELECT value FROM members)");
    let valid_inputs = [
        (DEFAULT_PORT_HANDLE, schema(FieldType::Int)),
        (1, schema(FieldType::Int)),
    ]
    .into_iter()
    .collect();
    factory
        .runtime
        .block_on(factory.build(valid_inputs, HashMap::new(), EventHub::new(1)))
        .unwrap();
    for inner in [
        Schema::default(),
        Schema {
            fields: vec![schema(FieldType::Int).fields[0].clone(); 2],
            primary_index: vec![],
        },
        schema(FieldType::Timestamp),
    ] {
        let expected = if inner.fields.len() == 1 {
            "IN subquery types are incompatible:"
        } else {
            "IN subquery must return exactly one column"
        };
        let inputs = [(DEFAULT_PORT_HANDLE, schema(FieldType::Int)), (1, inner)]
            .into_iter()
            .collect();
        let error = factory
            .runtime
            .block_on(factory.build(inputs, HashMap::new(), EventHub::new(1)))
            .err()
            .unwrap();
        assert!(
            matches!(error.downcast_ref::<PipelineError>(),
            Some(PipelineError::InvalidQuery(message)) if message.starts_with(expected)),
            "{error:?}"
        );
    }
    assert!(factory
        .runtime
        .block_on(factory.get_output_schema(&9, &HashMap::new()))
        .is_err());
}

#[test]
fn deterministic_multiset_model_matches_every_legal_event() {
    // Independent materialized SQL model; bounded values include NULL, duplicates,
    // and all operations. No expected value comes from the processor evaluator.
    for negated in [false, true] {
        let mut p = processor(&format!(
            "value {}IN (SELECT value FROM members)",
            if negated { "NOT " } else { "" }
        ));
        let mut outer: Vec<Record> = vec![];
        let mut inner: Vec<Record> = vec![];
        let mut materialized: Vec<Record> = vec![];
        let mut seed = 0x1659_u64;
        for step in 0..400 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let outer_event = ((seed >> 32) & 1) == 0;
            let port = if outer_event { DEFAULT_PORT_HANDLE } else { 1 };
            let rows = if outer_event { &mut outer } else { &mut inner };
            let value = match (seed >> 40) % 5 {
                0 => null(),
                value => row(value as i64),
            };
            let action = (seed >> 48) % 4;
            let op = if rows.is_empty() || action == 0 {
                rows.push(value.clone());
                insert(value)
            } else if action == 1 {
                let old = rows.remove((seed as usize) % rows.len());
                delete(old)
            } else if action == 2 {
                let index = (seed as usize) % rows.len();
                let old = std::mem::replace(&mut rows[index], value.clone());
                update(old, value)
            } else {
                rows.extend([value.clone(), value.clone()]);
                Operation::BatchInsert {
                    new: vec![value.clone(), value],
                }
            };
            for op in send(&mut *p, port, op) {
                match op {
                    Operation::Insert { new } => materialized.push(new),
                    Operation::Delete { old } => {
                        let index = materialized
                            .iter()
                            .position(|record| record == &old)
                            .expect("extra retraction");
                        materialized.remove(index);
                    }
                    Operation::Update { old, new } => {
                        let index = materialized
                            .iter()
                            .position(|record| record == &old)
                            .expect("update of missing row");
                        materialized[index] = new;
                    }
                    Operation::BatchInsert { new } => materialized.extend(new),
                }
            }
            let expected = outer
                .iter()
                .filter(|value| {
                    let truth = if inner.is_empty() {
                        Some(false)
                    } else if **value == null() {
                        None
                    } else if inner.contains(value) {
                        Some(true)
                    } else if inner.contains(&null()) {
                        None
                    } else {
                        Some(false)
                    };
                    truth.map(|truth| truth != negated) == Some(true)
                })
                .cloned()
                .collect::<Vec<_>>();
            let counts = |rows: &[Record]| {
                let mut map = HashMap::new();
                for row in rows {
                    *map.entry(row.clone()).or_insert(0usize) += 1;
                }
                map
            };
            assert_eq!(
                counts(&materialized),
                counts(&expected),
                "step {step}, NOT={negated}"
            );
        }
    }
}
