use super::*;
use crate::selection::processor::SelectionProcessor;
use dozer_types::{node::OpIdentifier, types::Lifetime};

#[derive(Default)]
struct Output(Vec<TableOperation>);

impl ProcessorChannelForwarder for Output {
    fn send(&mut self, operation: TableOperation) {
        self.0.push(operation);
    }
}

fn schema() -> Schema {
    Schema {
        fields: vec![
            FieldDefinition::new("id".into(), FieldType::Int, false, Default::default()),
            FieldDefinition::new("value".into(), FieldType::Int, true, Default::default()),
        ],
        primary_index: vec![0],
    }
}

fn processor(negated: bool) -> MembershipProcessor {
    MembershipProcessor::new(schema(), Expression::Column { index: 1 }, negated).unwrap()
}

fn outer(id: i64, value: Field) -> Record {
    Record::new(vec![Field::Int(id), value])
}

fn inner(value: Field) -> Record {
    Record::new(vec![value])
}

fn insert(row: Record) -> Operation {
    Operation::Insert { new: row }
}

fn delete(row: Record) -> Operation {
    Operation::Delete { old: row }
}

fn feed(
    processor: &mut MembershipProcessor,
    port: PortHandle,
    operation: Operation,
) -> Result<Vec<Operation>, BoxedError> {
    let id = Some(OpIdentifier::new(19, 7));
    let mut output = Output::default();
    let result = processor.process(
        TableOperation {
            id,
            port,
            op: operation,
        },
        &mut output,
    );
    if let Err(error) = result {
        assert!(
            output.0.is_empty(),
            "a failed operation emitted partial output"
        );
        return Err(error);
    }
    for operation in &output.0 {
        assert_eq!(operation.port, DEFAULT_PORT_HANDLE);
        assert_eq!(operation.id, id);
    }
    Ok(output.0.into_iter().map(|operation| operation.op).collect())
}

fn marked(row: &Record, mark: Field) -> Record {
    let mut row = row.clone();
    row.values.push(mark);
    row
}

fn transition(row: &Record, before: Field, after: Field) -> Operation {
    Operation::Update {
        old: marked(row, before),
        new: marked(row, after),
    }
}

fn selected(operations: Vec<Operation>) -> Vec<Operation> {
    let mut schema = schema();
    schema.fields.push(FieldDefinition::new(
        "member".into(),
        FieldType::Boolean,
        true,
        Default::default(),
    ));
    let mut selection = SelectionProcessor::new(schema, Expression::Column { index: 2 }).unwrap();
    let mut output = Output::default();
    for op in operations {
        selection
            .process(
                TableOperation {
                    id: None,
                    port: DEFAULT_PORT_HANDLE,
                    op,
                },
                &mut output,
            )
            .unwrap();
    }
    output.0.into_iter().map(|operation| operation.op).collect()
}

#[test]
fn membership_truth_table_including_null_operand_and_empty_relation() {
    let cases = [
        (vec![], Field::Boolean(false), Field::Boolean(false)),
        (vec![Field::Int(7)], Field::Boolean(true), Field::Null),
        (vec![Field::Int(9)], Field::Boolean(false), Field::Null),
        (vec![Field::Null], Field::Null, Field::Null),
        (
            vec![Field::Int(7), Field::Null],
            Field::Boolean(true),
            Field::Null,
        ),
        (vec![Field::Int(9), Field::Null], Field::Null, Field::Null),
    ];
    for negated in [false, true] {
        for (values, expected, expected_null) in &cases {
            let mut p = processor(negated);
            feed(
                &mut p,
                VALUES_PORT,
                Operation::BatchInsert {
                    new: values.iter().cloned().map(inner).collect(),
                },
            )
            .unwrap();
            for (value, expected) in [(Field::Int(7), expected), (Field::Null, expected_null)] {
                let expected = match expected {
                    Field::Boolean(value) => Field::Boolean(value ^ negated),
                    _ => Field::Null,
                };
                let row = outer(1, value);
                assert_eq!(
                    feed(&mut p, OUTER_PORT, insert(row.clone())).unwrap(),
                    vec![insert(marked(&row, expected))],
                    "negated={negated}, values={values:?}"
                );
            }
        }
    }
}

#[test]
fn duplicate_inner_rows_do_not_multiply_outer_rows() {
    let mut p = processor(false);
    let row = outer(1, Field::Int(7));
    feed(
        &mut p,
        OUTER_PORT,
        Operation::BatchInsert {
            new: vec![row.clone(), row.clone()],
        },
    )
    .unwrap();
    let changes = feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7)))).unwrap();
    assert_eq!(
        changes,
        vec![transition(&row, Field::Boolean(false), Field::Boolean(true)); 2]
    );
    assert!(feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7))))
        .unwrap()
        .is_empty());
    assert!(feed(&mut p, VALUES_PORT, delete(inner(Field::Int(7))))
        .unwrap()
        .is_empty());
    feed(&mut p, OUTER_PORT, delete(row.clone())).unwrap();
    assert_eq!(
        feed(&mut p, VALUES_PORT, delete(inner(Field::Int(7)))).unwrap(),
        vec![transition(
            &row,
            Field::Boolean(true),
            Field::Boolean(false)
        )]
    );
    feed(&mut p, OUTER_PORT, delete(row)).unwrap();
    assert!(p.rows.is_empty());
    assert!(p.values.is_empty());
}

#[test]
fn outer_updates_produce_all_four_selection_transitions() {
    let mut p = processor(false);
    feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7)))).unwrap();
    let mut old = outer(1, Field::Int(9));
    assert!(selected(feed(&mut p, OUTER_PORT, insert(old.clone())).unwrap()).is_empty());
    for (id, value, expected_kind) in [(2, 8, 0), (3, 7, 1), (4, 7, 2), (5, 8, 3)] {
        let new = outer(id, Field::Int(value));
        let output = selected(
            feed(
                &mut p,
                OUTER_PORT,
                Operation::Update {
                    old: old.clone(),
                    new: new.clone(),
                },
            )
            .unwrap(),
        );
        let expected = match expected_kind {
            0 => vec![],
            1 => vec![insert(marked(&new, Field::Boolean(true)))],
            2 => vec![Operation::Update {
                old: marked(&old, Field::Boolean(true)),
                new: marked(&new, Field::Boolean(true)),
            }],
            3 => vec![delete(marked(&old, Field::Boolean(true)))],
            _ => unreachable!(),
        };
        assert_eq!(output, expected);
        old = new;
    }
}

#[test]
fn inner_update_and_batch_have_no_intermediate_membership_event() {
    let mut p = processor(true);
    let row = outer(1, Field::Int(7));
    feed(&mut p, OUTER_PORT, insert(row.clone())).unwrap();
    let batch = feed(
        &mut p,
        VALUES_PORT,
        Operation::BatchInsert {
            new: vec![inner(Field::Null), inner(Field::Int(7))],
        },
    )
    .unwrap();
    assert_eq!(
        batch,
        vec![transition(
            &row,
            Field::Boolean(true),
            Field::Boolean(false)
        )]
    );
    assert!(feed(
        &mut p,
        VALUES_PORT,
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Int(7)),
        },
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        feed(&mut p, VALUES_PORT, delete(inner(Field::Int(7)))).unwrap(),
        vec![transition(&row, Field::Boolean(false), Field::Null)]
    );
    assert_eq!(
        feed(&mut p, VALUES_PORT, delete(inner(Field::Null))).unwrap(),
        vec![transition(&row, Field::Null, Field::Boolean(true))]
    );
}

#[test]
fn membership_uses_sql_numeric_and_text_equality() {
    for (left, right) in [
        (Field::Int(7), Field::Float(7.0.into())),
        (Field::UInt(7), Field::Int(7)),
        (Field::String("seven".into()), Field::Text("seven".into())),
    ] {
        let mut p = processor(false);
        let row = outer(1, left);
        feed(&mut p, OUTER_PORT, insert(row.clone())).unwrap();
        assert_eq!(
            feed(&mut p, VALUES_PORT, insert(inner(right.clone()))).unwrap(),
            vec![transition(
                &row,
                Field::Boolean(false),
                Field::Boolean(true)
            )]
        );
        assert_eq!(
            feed(&mut p, VALUES_PORT, delete(inner(right))).unwrap(),
            vec![transition(
                &row,
                Field::Boolean(true),
                Field::Boolean(false)
            )]
        );
    }
}

#[test]
fn replacing_equal_values_of_different_types_is_atomic() {
    let mut p = processor(false);
    let row = outer(1, Field::Int(7));
    feed(&mut p, OUTER_PORT, insert(row)).unwrap();
    feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7)))).unwrap();
    assert!(feed(
        &mut p,
        VALUES_PORT,
        Operation::Update {
            old: inner(Field::Int(7)),
            new: inner(Field::Float(7.0.into())),
        },
    )
    .unwrap()
    .is_empty());
    assert_eq!(p.value_count, 1);
}

#[test]
fn malformed_changes_leave_state_and_output_untouched() {
    let mut p = processor(false);
    let row = outer(1, Field::Int(7));
    feed(&mut p, OUTER_PORT, insert(row.clone())).unwrap();
    feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7)))).unwrap();
    let bad = inner(Field::Binary(vec![7]));
    for operation in [
        Operation::Update {
            old: inner(Field::Int(7)),
            new: bad.clone(),
        },
        Operation::BatchInsert {
            new: vec![inner(Field::Int(9)), bad],
        },
        delete(inner(Field::Int(9))),
        Operation::Update {
            old: inner(Field::Int(9)),
            new: inner(Field::Int(9)),
        },
        insert(Record::new(vec![Field::Int(1), Field::Int(2)])),
    ] {
        assert!(feed(&mut p, VALUES_PORT, operation).is_err());
        assert_eq!(p.values, HashMap::from([(Field::Int(7), 1)]));
        assert_eq!(p.value_count, 1);
        assert_eq!(p.retained(&row).unwrap().matches, 1);
    }
    assert!(feed(
        &mut p,
        OUTER_PORT,
        Operation::BatchInsert {
            new: vec![outer(2, Field::Int(7)), Record::new(vec![])],
        },
    )
    .is_err());
    assert_eq!(p.rows.len(), 1);
    assert!(feed(
        &mut p,
        OUTER_PORT,
        Operation::Update {
            old: row.clone(),
            new: outer(2, Field::Binary(vec![7])),
        },
    )
    .is_err());
    assert_eq!(p.retained(&row).unwrap().copies, 1);
    assert!(feed(&mut p, OUTER_PORT, delete(outer(99, Field::Int(7)))).is_err());
    assert!(feed(&mut p, 9, insert(row)).is_err());
}

#[test]
fn ttl_rejection_does_not_partially_apply_a_batch() {
    let mut p = processor(false);
    let mut expired = inner(Field::Int(7));
    expired.lifetime = Some(Lifetime {
        reference: dozer_types::chrono::Utc::now().into(),
        duration: std::time::Duration::from_secs(10),
    });
    assert!(feed(
        &mut p,
        VALUES_PORT,
        Operation::BatchInsert {
            new: vec![inner(Field::Int(1)), expired],
        },
    )
    .is_err());
    assert!(p.values.is_empty());
    assert_eq!(p.value_count, 0);
}

#[test]
fn unknown_comparisons_are_counted_even_without_null_values() {
    let mut p = processor(false);
    let row = outer(1, Field::Boolean(true));
    feed(&mut p, OUTER_PORT, insert(row.clone())).unwrap();
    assert_eq!(
        feed(&mut p, VALUES_PORT, insert(inner(Field::Binary(vec![1])))).unwrap(),
        vec![transition(&row, Field::Boolean(false), Field::Null)]
    );
    assert_eq!(
        feed(&mut p, VALUES_PORT, insert(inner(Field::Boolean(true)))).unwrap(),
        vec![transition(&row, Field::Null, Field::Boolean(true))]
    );
    assert_eq!(
        feed(&mut p, VALUES_PORT, delete(inner(Field::Boolean(true)))).unwrap(),
        vec![transition(&row, Field::Boolean(true), Field::Null)]
    );
    assert_eq!(
        feed(&mut p, VALUES_PORT, delete(inner(Field::Binary(vec![1])))).unwrap(),
        vec![transition(&row, Field::Null, Field::Boolean(false))]
    );
}

#[test]
fn volatile_operand_is_rejected_even_inside_a_scalar_expression() {
    let runtime = crate::tests::utils::create_test_runtime();
    for sql in [
        "SELECT * FROM rows WHERE NOW()",
        "SELECT * FROM rows WHERE NOW() IS NULL",
    ] {
        let select = crate::tests::utils::get_select(sql).unwrap();
        let operand = runtime
            .block_on(ExpressionBuilder::new(2, runtime.clone()).build(
                false,
                &select.selection.unwrap(),
                &schema(),
                &[],
            ))
            .unwrap();
        let error = MembershipProcessor::new(schema(), operand, false).unwrap_err();
        assert!(error
            .to_string()
            .contains("deterministic scalar expressions"));
    }
}

#[test]
fn updating_into_an_existing_outer_row_preserves_multiplicity() {
    let mut p = processor(false);
    let old = outer(1, Field::Int(3));
    let new = outer(2, Field::Int(7));
    feed(&mut p, OUTER_PORT, insert(old.clone())).unwrap();
    feed(&mut p, OUTER_PORT, insert(new.clone())).unwrap();
    feed(
        &mut p,
        OUTER_PORT,
        Operation::Update {
            old,
            new: new.clone(),
        },
    )
    .unwrap();
    assert_eq!(p.retained(&new).unwrap().copies, 2);
    assert_eq!(
        feed(&mut p, VALUES_PORT, insert(inner(Field::Int(7)))).unwrap(),
        vec![transition(&new, Field::Boolean(false), Field::Boolean(true)); 2]
    );
    feed(&mut p, OUTER_PORT, delete(new.clone())).unwrap();
    feed(&mut p, OUTER_PORT, delete(new)).unwrap();
    assert!(p.rows.is_empty());
}

fn representation_sensitive_operand(sql: &str, typ: FieldType) -> MembershipProcessor {
    let runtime = crate::tests::utils::create_test_runtime();
    let mut schema = schema();
    schema.fields[1].typ = typ;
    let select = crate::tests::utils::get_select(sql).unwrap();
    let operand = runtime
        .block_on(ExpressionBuilder::new(2, runtime.clone()).build(
            false,
            &select.selection.unwrap(),
            &schema,
            &[],
        ))
        .unwrap();
    MembershipProcessor::new(schema, operand, false).unwrap()
}

fn assert_distinct_representations(
    mut p: MembershipProcessor,
    first: Record,
    second: Record,
    matching: Field,
) {
    assert_eq!(
        first, second,
        "this regression requires semantic Record equality"
    );
    assert_ne!(row_key(&first).unwrap(), row_key(&second).unwrap());
    feed(&mut p, VALUES_PORT, insert(inner(matching.clone()))).unwrap();
    assert_eq!(
        feed(&mut p, OUTER_PORT, insert(first.clone())).unwrap(),
        vec![insert(marked(&first, Field::Boolean(true)))]
    );
    assert_eq!(
        feed(&mut p, OUTER_PORT, insert(second.clone())).unwrap(),
        vec![insert(marked(&second, Field::Boolean(false)))]
    );
    assert_eq!(p.rows.len(), 2);
    let changes = feed(&mut p, VALUES_PORT, delete(inner(matching))).unwrap();
    assert_eq!(
        changes,
        vec![transition(
            &first,
            Field::Boolean(true),
            Field::Boolean(false)
        )]
    );
    // Compare encoded records too: ordinary equality hides the very difference
    // which this test is intended to protect.
    match &changes[0] {
        Operation::Update { new, .. } => assert_eq!(
            row_key(new).unwrap(),
            row_key(&marked(&first, Field::Boolean(false))).unwrap()
        ),
        _ => panic!("expected membership update"),
    }
    let deletion = feed(&mut p, OUTER_PORT, delete(second.clone())).unwrap();
    assert_eq!(
        deletion,
        vec![delete(marked(&second, Field::Boolean(false)))]
    );
    assert_eq!(p.rows.len(), 1);
    assert_eq!(p.retained(&first).unwrap().copies, 1);
    feed(&mut p, OUTER_PORT, delete(first)).unwrap();
    assert!(p.rows.is_empty());
}

#[test]
fn equal_decimal_values_keep_distinct_scale_sensitive_operands() {
    use dozer_types::rust_decimal::Decimal;
    assert_distinct_representations(
        representation_sensitive_operand(
            "SELECT * FROM rows WHERE CAST(value AS STRING)",
            FieldType::Decimal,
        ),
        outer(1, Field::Decimal(Decimal::new(10, 1))),
        outer(1, Field::Decimal(Decimal::new(100, 2))),
        Field::String("1.0".into()),
    );
}

#[test]
fn equal_timestamp_instants_keep_distinct_local_hour_operands() {
    use dozer_types::chrono::DateTime;
    assert_distinct_representations(
        representation_sensitive_operand(
            "SELECT * FROM rows WHERE EXTRACT(HOUR FROM value)",
            FieldType::Timestamp,
        ),
        outer(
            1,
            Field::Timestamp(DateTime::parse_from_rfc3339("2024-01-01T00:00:00+00:00").unwrap()),
        ),
        outer(
            1,
            Field::Timestamp(DateTime::parse_from_rfc3339("2024-01-01T01:00:00+01:00").unwrap()),
        ),
        Field::Int(0),
    );
}

#[test]
fn factory_validates_width_and_builds_a_nullable_membership_column() {
    use dozer_sql_expression::sqlparser::ast::Ident;
    let runtime = crate::tests::utils::create_test_runtime();
    let factory = MembershipProcessorFactory {
        id: "membership".into(),
        operand: Expr::Identifier(Ident::new("value")),
        negated: false,
        column: "membership_result".into(),
        udfs: vec![],
        runtime: runtime.clone(),
    };
    let mut values = schema();
    values.fields.remove(0);
    values.primary_index.clear();
    let inputs = HashMap::from([(OUTER_PORT, schema()), (VALUES_PORT, values)]);
    runtime.block_on(async {
        let output = factory
            .get_output_schema(&DEFAULT_PORT_HANDLE, &inputs)
            .await
            .unwrap();
        assert_eq!(&output.fields[..2], schema().fields);
        assert_eq!(output.primary_index, vec![0]);
        assert_eq!(output.fields[2].typ, FieldType::Boolean);
        assert!(output.fields[2].nullable);
        let mut processor = factory
            .build(inputs.clone(), HashMap::new(), EventHub::new(1))
            .await
            .unwrap();
        let mut captured = Output::default();
        processor
            .process(
                TableOperation {
                    id: None,
                    port: OUTER_PORT,
                    op: insert(outer(1, Field::Int(7))),
                },
                &mut captured,
            )
            .unwrap();
        assert_eq!(
            captured.0[0].op,
            insert(marked(&outer(1, Field::Int(7)), Field::Boolean(false)))
        );
        let mut invalid = inputs;
        invalid.insert(VALUES_PORT, schema());
        assert!(factory
            .get_output_schema(&DEFAULT_PORT_HANDLE, &invalid)
            .await
            .is_err());
        invalid.remove(&VALUES_PORT);
        assert!(factory
            .get_output_schema(&DEFAULT_PORT_HANDLE, &invalid)
            .await
            .is_err());
    });
}

fn apply(multiset: &mut Vec<Record>, operation: Operation) {
    match operation {
        Operation::Insert { new } => multiset.push(new),
        Operation::Delete { old } => {
            let position = multiset.iter().position(|row| row == &old).unwrap();
            multiset.remove(position);
        }
        Operation::Update { old, new } => {
            apply(multiset, delete(old));
            apply(multiset, insert(new));
        }
        Operation::BatchInsert { new } => multiset.extend(new),
    }
}

fn frequencies(rows: impl IntoIterator<Item = Record>) -> HashMap<Record, usize> {
    let mut result = HashMap::new();
    for row in rows {
        *result.entry(row).or_default() += 1;
    }
    result
}

#[test]
fn event_stream_matches_an_independent_snapshot_multiset() {
    // Reference calculation intentionally uses only integer/NULL snapshot rows,
    // with no cached match counts or the processor's comparison helpers.
    for negated in [false, true] {
        let mut p = processor(negated);
        let mut outer_rows: Vec<Record> = Vec::new();
        let mut inner_rows: Vec<Record> = Vec::new();
        let mut observed = Vec::new();
        let mut random = 0x1665_u64;
        for step in 0..600 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let port = if random & 8 == 0 {
                OUTER_PORT
            } else {
                VALUES_PORT
            };
            let rows = if port == OUTER_PORT {
                &mut outer_rows
            } else {
                &mut inner_rows
            };
            let value = match (random >> 8) % 5 {
                0 => Field::Null,
                value => Field::Int(value as i64),
            };
            let new = if port == OUTER_PORT {
                outer(((random >> 16) % 4) as i64, value)
            } else {
                inner(value)
            };
            let operation = match (random >> 24) % 4 {
                0 if !rows.is_empty() => delete(rows[(random as usize >> 32) % rows.len()].clone()),
                1 if !rows.is_empty() => Operation::Update {
                    old: rows[(random as usize >> 32) % rows.len()].clone(),
                    new,
                },
                2 => Operation::BatchInsert {
                    new: vec![new.clone(), new],
                },
                _ => insert(new),
            };
            apply(rows, operation.clone());
            for operation in feed(&mut p, port, operation).unwrap() {
                apply(&mut observed, operation);
            }
            let expected = outer_rows.iter().map(|row| {
                let value = &row.values[1];
                let found = value != &Field::Null
                    && inner_rows.iter().any(|inner| &inner.values[0] == value);
                let unknown = !inner_rows.is_empty()
                    && !found
                    && (value == &Field::Null
                        || inner_rows
                            .iter()
                            .any(|inner| inner.values[0] == Field::Null));
                let mark = if unknown {
                    Field::Null
                } else {
                    Field::Boolean(found ^ negated)
                };
                marked(row, mark)
            });
            assert_eq!(
                frequencies(observed.clone()),
                frequencies(expected),
                "step={step}, negated={negated}"
            );
        }
    }
}
