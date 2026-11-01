use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    channels::ProcessorChannelForwarder,
    epoch::Epoch,
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::ExpressionBuilder, execution::Expression, operator::BinaryOperatorType,
    sqlparser::ast::Expr,
};
use dozer_types::{
    errors::internal::BoxedError,
    models::udf_config::UdfConfig,
    tonic::async_trait,
    types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    },
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

#[derive(Debug)]
pub(crate) struct MembershipFactory {
    pub id: String,
    pub predicate: Expr,
    pub probes: Vec<(Expr, bool, String)>,
    pub udfs: Vec<UdfConfig>,
    pub runtime: Arc<Runtime>,
}

#[async_trait]
impl ProcessorFactory for MembershipFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "Membership".to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        std::iter::once(DEFAULT_PORT_HANDLE)
            .chain(1..=self.probes.len() as PortHandle)
            .collect()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }
    async fn get_output_schema(
        &self,
        _: &PortHandle,
        inputs: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        for port in 1..=self.probes.len() as PortHandle {
            let schema = inputs
                .get(&port)
                .ok_or(PipelineError::InvalidPortHandle(port))?;
            if schema.fields.len() != 1 {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery must return exactly one column".into(),
                )
                .into());
            }
        }
        Ok(inputs
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?
            .clone())
    }
    async fn build(
        &self,
        inputs: HashMap<PortHandle, Schema>,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let outer = self
            .get_output_schema(&DEFAULT_PORT_HANDLE, &inputs)
            .await?;
        let mut extended = outer.clone();
        let mut probes = Vec::new();
        for (expr, negated, name) in &self.probes {
            let expression = ExpressionBuilder::new(outer.fields.len(), self.runtime.clone())
                .build(false, expr, &outer, &self.udfs)
                .await?;
            probes.push((expression, *negated));
            extended.fields.push(FieldDefinition::new(
                name.clone(),
                FieldType::Boolean,
                true,
                SourceDefinition::Dynamic,
            ));
        }
        let predicate = ExpressionBuilder::new(extended.fields.len(), self.runtime.clone())
            .build(false, &self.predicate, &extended, &self.udfs)
            .await?;
        Ok(Box::new(MembershipProcessor {
            schema: outer,
            extended,
            predicate,
            inner: vec![Vec::new(); probes.len()],
            probes,
            outer: Vec::new(),
        }))
    }
}

#[derive(Debug)]
struct MembershipProcessor {
    schema: Schema,
    extended: Schema,
    predicate: Expression,
    probes: Vec<(Expression, bool)>,
    // Counts are kept separately from outer rows: duplicate inner values must not multiply output.
    inner: Vec<Vec<(Field, usize)>>,
    outer: Vec<Record>,
}

impl MembershipProcessor {
    fn matches(&mut self, row: &Record) -> Result<bool, BoxedError> {
        if row.values.len() != self.schema.fields.len() {
            return Err(PipelineError::InvalidQuery(
                "IN outer row does not match its schema".into(),
            )
            .into());
        }
        let mut extended = row.clone();
        for (index, (probe, negated)) in self.probes.iter_mut().enumerate() {
            let value = probe.evaluate(row, &self.schema)?;
            let mut unknown = false;
            let mut found = false;
            for (item, _) in &self.inner[index] {
                if value == Field::Null || *item == Field::Null {
                    unknown = true;
                    continue;
                }
                let equal = BinaryOperatorType::Eq.evaluate(
                    &self.schema,
                    &mut Expression::Literal(value.clone()),
                    &mut Expression::Literal(item.clone()),
                    row,
                )?;
                if equal == Field::Boolean(true) {
                    found = true;
                    break;
                }
            }
            extended.values.push(if !found && unknown {
                Field::Null
            } else {
                Field::Boolean(found != *negated)
            });
        }
        Ok(
            evaluate_predicate(&mut self.predicate, &extended, &self.extended)?
                == Field::Boolean(true),
        )
    }

    fn adjust_inner(&mut self, index: usize, row: Record, insert: bool) -> Result<(), BoxedError> {
        if row.values.len() != 1 {
            return Err(
                PipelineError::InvalidQuery("IN subquery row must have one column".into()).into(),
            );
        }
        let value = row
            .values
            .into_iter()
            .next()
            .ok_or_else(|| PipelineError::InvalidQuery("Empty IN subquery row".into()))?;
        let values = &mut self.inner[index];
        if let Some(position) = values.iter().position(|(item, _)| *item == value) {
            if insert {
                values[position].1 += 1;
            } else if values[position].1 == 1 {
                values.remove(position);
            } else {
                values[position].1 -= 1;
            }
        } else if insert {
            values.push((value, 1));
        }
        Ok(())
    }

    fn outer_change(&mut self, op: Operation) -> Result<Vec<Operation>, BoxedError> {
        let mut output = Vec::new();
        match op {
            Operation::Insert { new } => {
                if self.matches(&new)? {
                    output.push(Operation::Insert { new: new.clone() });
                }
                self.outer.push(new);
            }
            Operation::Delete { old } => {
                if let Some(index) = self.outer.iter().position(|row| *row == old) {
                    if self.matches(&old)? {
                        output.push(Operation::Delete { old });
                    }
                    self.outer.remove(index);
                }
            }
            Operation::Update { old, new } => {
                let before = self.matches(&old)?;
                let after = self.matches(&new)?;
                if let Some(index) = self.outer.iter().position(|row| *row == old) {
                    self.outer[index] = new.clone();
                } else {
                    return Err(PipelineError::InvalidQuery(
                        "IN update has no retained outer row".into(),
                    )
                    .into());
                }
                match (before, after) {
                    (true, true) => output.push(Operation::Update { old, new }),
                    (true, false) => output.push(Operation::Delete { old }),
                    (false, true) => output.push(Operation::Insert { new }),
                    _ => {}
                }
            }
            Operation::BatchInsert { new } => {
                for row in new {
                    output.extend(self.outer_change(Operation::Insert { new: row })?);
                }
            }
        }
        Ok(output)
    }
}

// Preserve UNKNOWN through boolean combinations; filtering happens only at the final predicate.
fn evaluate_predicate(
    expr: &mut Expression,
    row: &Record,
    schema: &Schema,
) -> Result<Field, BoxedError> {
    use dozer_sql_expression::operator::UnaryOperatorType;
    match expr {
        Expression::IsNull { arg } => Ok(Field::Boolean(
            evaluate_predicate(arg, row, schema)? == Field::Null,
        )),
        Expression::IsNotNull { arg } => Ok(Field::Boolean(
            evaluate_predicate(arg, row, schema)? != Field::Null,
        )),
        Expression::BinaryOperator {
            left,
            operator: op @ (BinaryOperatorType::And | BinaryOperatorType::Or),
            right,
        } => {
            let left = evaluate_predicate(left, row, schema)?;
            let right = evaluate_predicate(right, row, schema)?;
            let truth = |value| match value {
                Field::Boolean(value) => Ok(Some(value)),
                Field::Null => Ok(None),
                _ => Err(PipelineError::InvalidQuery(
                    "IN predicate must be boolean".into(),
                )),
            };
            let (left, right) = (truth(left)?, truth(right)?);
            let result = match op {
                BinaryOperatorType::And => match (left, right) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
                _ => match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                },
            };
            Ok(result.map(Field::Boolean).unwrap_or(Field::Null))
        }
        Expression::UnaryOperator {
            operator: UnaryOperatorType::Not,
            arg,
        } => Ok(match evaluate_predicate(arg, row, schema)? {
            Field::Boolean(value) => Field::Boolean(!value),
            Field::Null => Field::Null,
            _ => {
                return Err(
                    PipelineError::InvalidQuery("NOT predicate must be boolean".into()).into(),
                )
            }
        }),
        _ => Ok(expr.evaluate(row, schema)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn random_event_order_matches_multiset_model(
            negated in any::<bool>(),
            events in proptest::collection::vec((any::<bool>(), any::<bool>(), proptest::option::of(0i64..5)), 1..60)
        ) {
            let mut p = processor(negated);
            let mut outer = Vec::new();
            let mut inner = Vec::new();
            let mut visible = Vec::new();
            for (outer_event, inserting, value) in events {
                let target = if outer_event { &mut outer } else { &mut inner };
                if inserting { target.push(value); }
                else if let Some(index) = target.iter().position(|item| *item == value) { target.remove(index); }
                else { continue; }
                let record = row(value.map(Field::Int).unwrap_or(Field::Null));
                let operation = if inserting { Operation::Insert { new: record } } else { Operation::Delete { old: record } };
                for operation in send(&mut p, if outer_event { DEFAULT_PORT_HANDLE } else { 1 }, operation) {
                    match operation {
                        Operation::Insert { new } => visible.push(new.values[0].clone()),
                        Operation::Delete { old } => {
                            let index = visible.iter().position(|value| *value == old.values[0]).expect("retraction must exist");
                            visible.remove(index);
                        }
                        _ => panic!("insert/delete inputs must not emit another operation"),
                    }
                }
                let mut expected: Vec<_> = outer.iter().filter(|value| {
                    let matched = value.is_some() && inner.contains(value);
                    if matched { !negated }
                    else if inner.is_empty() { negated }
                    else if value.is_none() || inner.contains(&None) { false }
                    else { negated }
                }).copied().collect();
                let mut actual: Vec<_> = visible.iter().map(|value| match value {
                    Field::Null => None, Field::Int(value) => Some(*value), _ => panic!("unexpected output type"),
                }).collect();
                actual.sort();
                expected.sort();
                prop_assert_eq!(actual, expected);
            }
        }
    }

    #[derive(Default)]
    struct Output(Vec<Operation>);
    impl ProcessorChannelForwarder for Output {
        fn send(&mut self, op: TableOperation) {
            assert_eq!(op.port, DEFAULT_PORT_HANDLE);
            self.0.push(op.op);
        }
    }
    fn row(value: Field) -> Record {
        Record::new(vec![value])
    }
    fn int(value: i64) -> Record {
        row(Field::Int(value))
    }
    fn insert(value: i64) -> Operation {
        Operation::Insert { new: int(value) }
    }
    fn delete(value: i64) -> Operation {
        Operation::Delete { old: int(value) }
    }
    fn processor(negated: bool) -> MembershipProcessor {
        let mut schema = Schema::default();
        schema.fields.push(FieldDefinition::new(
            "k".into(),
            FieldType::Int,
            true,
            SourceDefinition::Dynamic,
        ));
        let mut extended = schema.clone();
        extended.fields.push(FieldDefinition::new(
            "member".into(),
            FieldType::Boolean,
            true,
            SourceDefinition::Dynamic,
        ));
        MembershipProcessor {
            schema,
            extended,
            predicate: Expression::Column { index: 1 },
            probes: vec![(Expression::Column { index: 0 }, negated)],
            inner: vec![vec![]],
            outer: vec![],
        }
    }
    fn send(
        processor: &mut MembershipProcessor,
        port: PortHandle,
        op: Operation,
    ) -> Vec<Operation> {
        let mut output = Output::default();
        processor
            .process(TableOperation::without_id(op, port), &mut output)
            .unwrap();
        output.0
    }
    #[test]
    fn inner_duplicates_do_not_multiply_or_retract_early() {
        let mut p = processor(false);
        assert!(send(&mut p, DEFAULT_PORT_HANDLE, insert(1)).is_empty());
        assert_eq!(send(&mut p, 1, insert(1)), vec![insert(1)]);
        assert!(send(&mut p, 1, insert(1)).is_empty());
        assert!(send(&mut p, 1, delete(1)).is_empty());
        assert_eq!(send(&mut p, 1, delete(1)), vec![delete(1)]);
    }
    #[test]
    fn outer_duplicates_and_deletion_preserve_multiplicity() {
        let mut p = processor(false);
        send(
            &mut p,
            DEFAULT_PORT_HANDLE,
            Operation::BatchInsert {
                new: vec![int(1), int(1)],
            },
        );
        assert_eq!(send(&mut p, 1, insert(1)), vec![insert(1), insert(1)]);
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, delete(1)),
            vec![delete(1)]
        );
        assert_eq!(send(&mut p, 1, delete(1)), vec![delete(1)]);
    }
    #[test]
    fn inner_update_retracts_old_and_emits_new_members() {
        let mut p = processor(false);
        send(
            &mut p,
            DEFAULT_PORT_HANDLE,
            Operation::BatchInsert {
                new: vec![int(1), int(2)],
            },
        );
        send(&mut p, 1, insert(1));
        assert_eq!(
            send(
                &mut p,
                1,
                Operation::Update {
                    old: int(1),
                    new: int(2)
                }
            ),
            vec![delete(1), insert(2)]
        );
    }
    #[test]
    fn outer_updates_cover_all_filter_transitions() {
        let mut p = processor(false);
        send(
            &mut p,
            1,
            Operation::BatchInsert {
                new: vec![int(1), int(2)],
            },
        );
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, insert(1)),
            vec![insert(1)]
        );
        let change = Operation::Update {
            old: int(1),
            new: int(2),
        };
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, change.clone()),
            vec![change]
        );
        assert_eq!(
            send(
                &mut p,
                DEFAULT_PORT_HANDLE,
                Operation::Update {
                    old: int(2),
                    new: int(3)
                }
            ),
            vec![delete(2)]
        );
        assert_eq!(
            send(
                &mut p,
                DEFAULT_PORT_HANDLE,
                Operation::Update {
                    old: int(3),
                    new: int(1)
                }
            ),
            vec![insert(1)]
        );
    }
    #[test]
    fn not_in_null_and_empty_set_follow_sql_semantics() {
        let mut p = processor(true);
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, insert(1)),
            vec![insert(1)]
        );
        assert_eq!(
            send(
                &mut p,
                1,
                Operation::Insert {
                    new: row(Field::Null)
                }
            ),
            vec![delete(1)]
        );
        assert_eq!(
            send(
                &mut p,
                1,
                Operation::Delete {
                    old: row(Field::Null)
                }
            ),
            vec![insert(1)]
        );
        send(&mut p, 1, insert(1));
        assert!(!p.matches(&row(Field::Null)).unwrap());
        assert!(!p.matches(&int(1)).unwrap());
    }
    #[test]
    fn null_probe_only_matches_not_in_an_empty_set() {
        let mut p = processor(false);
        assert!(!p.matches(&row(Field::Null)).unwrap());
        p.probes[0].1 = true;
        assert!(p.matches(&row(Field::Null)).unwrap());
        send(&mut p, 1, insert(2));
        assert!(!p.matches(&row(Field::Null)).unwrap());
    }
    #[test]
    fn not_of_unknown_conjunction_stays_unknown() {
        use dozer_sql_expression::operator::UnaryOperatorType;
        let mut p = processor(false);
        p.predicate = Expression::UnaryOperator {
            operator: UnaryOperatorType::Not,
            arg: Box::new(Expression::BinaryOperator {
                left: Box::new(Expression::Column { index: 1 }),
                operator: BinaryOperatorType::And,
                right: Box::new(Expression::Literal(Field::Boolean(true))),
            }),
        };
        send(
            &mut p,
            1,
            Operation::Insert {
                new: row(Field::Null),
            },
        );
        assert!(!p.matches(&int(1)).unwrap());
        p.predicate = Expression::IsNull {
            arg: Box::new(p.predicate.clone()),
        };
        assert!(p.matches(&int(1)).unwrap());
    }

    #[test]
    fn negated_comparison_of_unknown_membership_stays_unknown() {
        use dozer_sql_expression::operator::UnaryOperatorType;
        let mut p = processor(false);
        send(
            &mut p,
            1,
            Operation::Insert {
                new: row(Field::Null),
            },
        );
        p.predicate = Expression::UnaryOperator {
            operator: UnaryOperatorType::Not,
            arg: Box::new(Expression::BinaryOperator {
                left: Box::new(Expression::Column { index: 1 }),
                operator: BinaryOperatorType::Eq,
                right: Box::new(Expression::Literal(Field::Boolean(false))),
            }),
        };
        assert!(!p.matches(&int(1)).unwrap());
    }
    #[test]
    fn mixed_numeric_members_use_existing_sql_comparison() {
        let mut p = processor(false);
        send(
            &mut p,
            1,
            Operation::Insert {
                new: row(Field::Float(1.0.into())),
            },
        );
        assert!(p.matches(&int(1)).unwrap());
    }

    #[test]
    fn failed_batch_does_not_retain_partial_outer_state() {
        let mut p = processor(false);
        send(&mut p, 1, insert(1));
        let mut output = Output::default();
        let result = p.process(
            TableOperation::without_id(
                Operation::BatchInsert {
                    new: vec![int(1), Record::new(vec![])],
                },
                DEFAULT_PORT_HANDLE,
            ),
            &mut output,
        );
        assert!(result.is_err());
        assert!(p.outer.is_empty());
        assert!(output.0.is_empty());
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, insert(1)),
            vec![insert(1)]
        );
    }

    #[test]
    fn failed_inner_batch_rolls_back_without_touching_outer_rows() {
        let mut p = processor(false);
        send(&mut p, DEFAULT_PORT_HANDLE, insert(1));
        let mut output = Output::default();
        let result = p.process(
            TableOperation::without_id(
                Operation::BatchInsert {
                    new: vec![int(1), Record::new(vec![])],
                },
                1,
            ),
            &mut output,
        );
        assert!(result.is_err());
        assert!(p.inner[0].is_empty());
        assert_eq!(p.outer, vec![int(1)]);
        assert!(output.0.is_empty());
        assert_eq!(send(&mut p, 1, insert(1)), vec![insert(1)]);
    }

    #[test]
    fn failed_outer_update_keeps_original_row_without_a_snapshot() {
        let mut p = processor(false);
        send(&mut p, 1, insert(1));
        send(&mut p, DEFAULT_PORT_HANDLE, insert(1));
        let mut output = Output::default();
        let result = p.process(
            TableOperation::without_id(
                Operation::Update {
                    old: int(1),
                    new: Record::new(vec![]),
                },
                DEFAULT_PORT_HANDLE,
            ),
            &mut output,
        );
        assert!(result.is_err());
        assert_eq!(p.outer, vec![int(1)]);
        assert!(output.0.is_empty());
        assert_eq!(
            send(&mut p, DEFAULT_PORT_HANDLE, delete(1)),
            vec![delete(1)]
        );
    }

    #[test]
    fn multiple_inner_inputs_keep_independent_reference_counts() {
        let mut p = processor(false);
        p.inner.push(Vec::new());
        p.probes.push((Expression::Column { index: 0 }, false));
        p.extended.fields.push(FieldDefinition::new(
            "member2".into(),
            FieldType::Boolean,
            true,
            SourceDefinition::Dynamic,
        ));
        p.predicate = Expression::BinaryOperator {
            left: Box::new(Expression::Column { index: 1 }),
            operator: BinaryOperatorType::And,
            right: Box::new(Expression::Column { index: 2 }),
        };
        send(&mut p, DEFAULT_PORT_HANDLE, insert(1));
        assert!(send(&mut p, 1, insert(1)).is_empty());
        assert_eq!(send(&mut p, 2, insert(1)), vec![insert(1)]);
        assert_eq!(send(&mut p, 1, delete(1)), vec![delete(1)]);
        assert!(send(&mut p, 2, delete(1)).is_empty());
    }

    #[test]
    fn demo_streaming_membership() {
        let mut p = processor(false);
        for (description, port, operation, expected) in [
            (
                "outer INSERT 1 (no match yet)",
                DEFAULT_PORT_HANDLE,
                insert(1),
                vec![],
            ),
            (
                "inner INSERT 1 -> emit retained outer row",
                1,
                insert(1),
                vec![insert(1)],
            ),
            (
                "inner duplicate INSERT 1 -> no duplicate output",
                1,
                insert(1),
                vec![],
            ),
            (
                "inner DELETE 1 -> one reference remains",
                1,
                delete(1),
                vec![],
            ),
            (
                "inner last DELETE 1 -> retract outer row",
                1,
                delete(1),
                vec![delete(1)],
            ),
            (
                "inner INSERT 1 -> outer row becomes visible again",
                1,
                insert(1),
                vec![insert(1)],
            ),
            (
                "outer DELETE 1 -> retract and forget row",
                DEFAULT_PORT_HANDLE,
                delete(1),
                vec![delete(1)],
            ),
        ] {
            let actual = send(&mut p, port, operation);
            println!("{description}\n  output: {actual:?}");
            assert_eq!(actual, expected);
        }
    }
}

impl MembershipProcessor {
    fn apply(&mut self, port: PortHandle, op: Operation) -> Result<Vec<Operation>, BoxedError> {
        if port == DEFAULT_PORT_HANDLE {
            self.outer_change(op)
        } else {
            let index = usize::from(
                port.checked_sub(1)
                    .ok_or(PipelineError::InvalidPortHandle(port))?,
            );
            if index >= self.inner.len() {
                return Err(PipelineError::InvalidPortHandle(port).into());
            }
            let before = (0..self.outer.len())
                .map(|index| self.matches(&self.outer[index].clone()))
                .collect::<Result<Vec<_>, _>>()?;
            match op {
                Operation::Insert { new } => self.adjust_inner(index, new, true)?,
                Operation::Delete { old } => self.adjust_inner(index, old, false)?,
                Operation::Update { old, new } => {
                    self.adjust_inner(index, old, false)?;
                    self.adjust_inner(index, new, true)?;
                }
                Operation::BatchInsert { new } => {
                    for row in new {
                        self.adjust_inner(index, row, true)?;
                    }
                }
            }
            let mut changes = Vec::new();
            for (index, before) in before.into_iter().enumerate() {
                let row = self.outer[index].clone();
                match (before, self.matches(&row)?) {
                    (false, true) => changes.push(Operation::Insert { new: row }),
                    (true, false) => changes.push(Operation::Delete { old: row }),
                    _ => {}
                }
            }
            Ok(changes)
        }
    }
}

impl Processor for MembershipProcessor {
    fn commit(&self, _: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }
    fn process(
        &mut self,
        op: TableOperation,
        forwarder: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        // Single outer changes evaluate before mutation; only a batch can need rollback.
        let previous_outer =
            if op.port == DEFAULT_PORT_HANDLE && matches!(&op.op, Operation::BatchInsert { .. }) {
                Some(self.outer.clone())
            } else {
                None
            };
        let previous_inner = if op.port != DEFAULT_PORT_HANDLE {
            Some(self.inner.clone())
        } else {
            None
        };
        let output = match self.apply(op.port, op.op) {
            Ok(output) => output,
            Err(error) => {
                if let Some(outer) = previous_outer {
                    self.outer = outer;
                }
                if let Some(inner) = previous_inner {
                    self.inner = inner;
                }
                return Err(error);
            }
        };
        for change in output {
            forwarder.send(TableOperation {
                id: op.id,
                op: change,
                port: DEFAULT_PORT_HANDLE,
            });
        }
        Ok(())
    }
}
