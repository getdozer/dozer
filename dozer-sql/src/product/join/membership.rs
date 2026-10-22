//! Stateful membership joins for WHERE ... IN (SELECT ...).
//!
//! The right inputs are multisets, not ordinary joins: duplicate right values
//! must not multiply the left output. The output schema is exactly the left
//! schema, including its primary key and source annotations.
use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    channels::ProcessorChannelForwarder,
    epoch::Epoch,
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::ExpressionBuilder,
    execution::Expression,
    sqlparser::ast::{BinaryOperator, Expr, Query, UnaryOperator},
};
use dozer_types::{
    errors::internal::BoxedError,
    models::udf_config::UdfConfig,
    tonic::async_trait,
    types::{Field, Operation, Record, Schema, TableOperation},
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

#[derive(Clone, Debug)]
pub(crate) enum Predicate {
    Scalar(usize),
    Membership(usize),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

#[derive(Clone, Debug)]
pub(crate) struct Membership {
    expr: Expr,
    negated: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct MembershipPlan {
    predicate: Predicate,
    scalars: Vec<Expr>,
    memberships: Vec<Membership>,
}

impl MembershipPlan {
    pub(crate) fn extract(expr: Expr) -> (Self, Vec<Box<Query>>) {
        let mut scalars = vec![];
        let mut memberships = vec![];
        let mut queries = vec![];
        fn extract(
            expr: Expr,
            scalars: &mut Vec<Expr>,
            memberships: &mut Vec<Membership>,
            queries: &mut Vec<Box<Query>>,
        ) -> Predicate {
            match expr {
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let index = memberships.len();
                    memberships.push(Membership {
                        expr: *expr,
                        negated,
                    });
                    queries.push(subquery);
                    Predicate::Membership(index)
                }
                Expr::Nested(expr) => extract(*expr, scalars, memberships, queries),
                Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr,
                } => Predicate::Not(Box::new(extract(*expr, scalars, memberships, queries))),
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::And,
                    right,
                } => Predicate::And(
                    Box::new(extract(*left, scalars, memberships, queries)),
                    Box::new(extract(*right, scalars, memberships, queries)),
                ),
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Or,
                    right,
                } => Predicate::Or(
                    Box::new(extract(*left, scalars, memberships, queries)),
                    Box::new(extract(*right, scalars, memberships, queries)),
                ),
                expr => {
                    let index = scalars.len();
                    scalars.push(expr);
                    Predicate::Scalar(index)
                }
            }
        }
        let predicate = extract(expr, &mut scalars, &mut memberships, &mut queries);
        (
            Self {
                predicate,
                scalars,
                memberships,
            },
            queries,
        )
    }
}

#[derive(Debug)]
pub(crate) struct MembershipJoinFactory {
    id: String,
    plan: MembershipPlan,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl MembershipJoinFactory {
    pub(crate) fn new(
        id: String,
        plan: MembershipPlan,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            plan,
            udfs,
            runtime,
        }
    }

    fn validate<'a>(
        &self,
        schemas: &'a HashMap<PortHandle, Schema>,
    ) -> Result<&'a Schema, BoxedError> {
        let outer = schemas.get(&0).ok_or(PipelineError::InvalidPortHandle(0))?;
        for index in 0..self.plan.memberships.len() {
            let port = (index + 1) as PortHandle;
            let inner = schemas
                .get(&port)
                .ok_or(PipelineError::InvalidPortHandle(port))?;
            if inner.fields.len() != 1 {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery must return exactly one column".into(),
                )
                .into());
            }
        }
        Ok(outer)
    }
}

#[async_trait]
impl ProcessorFactory for MembershipJoinFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "MembershipJoin".into()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        (0..=self.plan.memberships.len())
            .map(|port| port as PortHandle)
            .collect()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }
    async fn get_output_schema(
        &self,
        _port: &PortHandle,
        schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        Ok(self.validate(schemas)?.clone())
    }
    async fn build(
        &self,
        schemas: HashMap<PortHandle, Schema>,
        _outputs: HashMap<PortHandle, Schema>,
        _hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = self.validate(&schemas)?.clone();
        let mut scalars = vec![];
        for scalar in &self.plan.scalars {
            scalars.push(
                ExpressionBuilder::new(schema.fields.len(), self.runtime.clone())
                    .build(false, scalar, &schema, &self.udfs)
                    .await?,
            );
        }
        let mut keys = vec![];
        for (index, membership) in self.plan.memberships.iter().enumerate() {
            let key = ExpressionBuilder::new(schema.fields.len(), self.runtime.clone())
                .build(false, &membership.expr, &schema, &self.udfs)
                .await?;
            // An untyped NULL can be compared with any inner type. Its result
            // still depends on whether the right multiset is empty.
            if !matches!(key, Expression::Literal(Field::Null))
                && key.get_type(&schema)?.return_type
                    != schemas[&((index + 1) as PortHandle)].fields[0].typ
            {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery and outer expression must have the same type".into(),
                )
                .into());
            }
            keys.push(key);
        }
        Ok(Box::new(MembershipJoin {
            predicate: self.plan.predicate.clone(),
            schema,
            scalars,
            keys,
            negated: self.plan.memberships.iter().map(|m| m.negated).collect(),
            inner: vec![HashMap::new(); self.plan.memberships.len()],
            outer: vec![],
        }))
    }
}

#[derive(Debug)]
struct MembershipJoin {
    predicate: Predicate,
    schema: Schema,
    scalars: Vec<Expression>,
    keys: Vec<Expression>,
    negated: Vec<bool>,
    inner: Vec<HashMap<Field, usize>>,
    // Keep multiplicity and the original records, including unmatched rows.
    outer: Vec<Record>,
}

// None is SQL UNKNOWN. Boolean composition must retain it until WHERE is
// evaluated, particularly under NOT (NULL IN (...) AND TRUE).
fn truth(field: Field) -> Result<Option<bool>, PipelineError> {
    match field {
        Field::Null => Ok(None),
        Field::Boolean(value) => Ok(Some(value)),
        _ => Err(PipelineError::InvalidQuery(
            "WHERE condition must be boolean".into(),
        )),
    }
}

impl MembershipJoin {
    fn evaluate(&mut self, record: &Record) -> Result<bool, PipelineError> {
        fn eval(
            predicate: &Predicate,
            scalar: &[Option<bool>],
            membership: &[Option<bool>],
        ) -> Option<bool> {
            match predicate {
                Predicate::Scalar(index) => scalar[*index],
                Predicate::Membership(index) => membership[*index],
                Predicate::Not(arg) => eval(arg, scalar, membership).map(|value| !value),
                Predicate::And(left, right) => match (
                    eval(left, scalar, membership),
                    eval(right, scalar, membership),
                ) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
                Predicate::Or(left, right) => match (
                    eval(left, scalar, membership),
                    eval(right, scalar, membership),
                ) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                },
            }
        }
        let scalar = self
            .scalars
            .iter_mut()
            .map(|expr| {
                expr.evaluate(record, &self.schema)
                    .map_err(PipelineError::from)
                    .and_then(truth)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut membership = vec![];
        for (index, expr) in self.keys.iter_mut().enumerate() {
            let key = expr.evaluate(record, &self.schema)?;
            let values = &self.inner[index];
            let value = if values.is_empty() {
                Some(false)
            } else if key == Field::Null {
                None
            } else if values.contains_key(&key) {
                Some(true)
            } else if values.contains_key(&Field::Null) {
                None
            } else {
                Some(false)
            };
            membership.push(if self.negated[index] {
                value.map(|value| !value)
            } else {
                value
            });
        }
        Ok(eval(&self.predicate, &scalar, &membership) == Some(true))
    }

    fn update_inner(
        values: &mut HashMap<Field, usize>,
        op: &Operation,
    ) -> Result<(), PipelineError> {
        fn insert(values: &mut HashMap<Field, usize>, row: &Record) -> Result<(), PipelineError> {
            let [key] = row.values.as_slice() else {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery must return exactly one column".into(),
                ));
            };
            *values.entry(key.clone()).or_default() += 1;
            Ok(())
        }
        fn delete(values: &mut HashMap<Field, usize>, row: &Record) -> Result<(), PipelineError> {
            let [key] = row.values.as_slice() else {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery must return exactly one column".into(),
                ));
            };
            let count = values.get_mut(key).ok_or_else(|| {
                PipelineError::InvalidQuery("IN subquery deleted an unknown row".into())
            })?;
            *count -= 1;
            if *count == 0 {
                values.remove(key);
            }
            Ok(())
        }
        match op {
            Operation::Insert { new } => insert(values, new),
            Operation::Delete { old } => delete(values, old),
            Operation::Update { old, new } => {
                delete(values, old)?;
                insert(values, new)
            }
            Operation::BatchInsert { new } => {
                for row in new {
                    insert(values, row)?;
                }
                Ok(())
            }
        }
    }
}

impl Processor for MembershipJoin {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }
    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        // A source may supply lifetimes without a SQL TTL operator. Reject
        // these before state mutation rather than retaining stale membership.
        let has_lifetime = match &op.op {
            Operation::Insert { new } => new.lifetime.is_some(),
            Operation::Delete { old } => old.lifetime.is_some(),
            Operation::Update { old, new } => old.lifetime.is_some() || new.lifetime.is_some(),
            Operation::BatchInsert { new } => new.iter().any(|row| row.lifetime.is_some()),
        };
        if has_lifetime {
            return Err(PipelineError::InvalidQuery(
                "IN subqueries with TTL/window inputs are not supported".into(),
            )
            .into());
        }
        let mut output = vec![];
        if op.port == 0 {
            match op.op {
                Operation::Insert { new } => {
                    if self.evaluate(&new)? {
                        output.push(Operation::Insert { new: new.clone() });
                    }
                    self.outer.push(new);
                }
                Operation::Delete { old } => {
                    let index = self
                        .outer
                        .iter()
                        .position(|row| row == &old)
                        .ok_or_else(|| {
                            PipelineError::InvalidQuery(
                                "IN join deleted an unknown outer row".into(),
                            )
                        })?;
                    if self.evaluate(&old)? {
                        output.push(Operation::Delete { old });
                    }
                    self.outer.remove(index);
                }
                Operation::Update { old, new } => {
                    let index = self
                        .outer
                        .iter()
                        .position(|row| row == &old)
                        .ok_or_else(|| {
                            PipelineError::InvalidQuery(
                                "IN join updated an unknown outer row".into(),
                            )
                        })?;
                    match (self.evaluate(&old)?, self.evaluate(&new)?) {
                        (true, true) => output.push(Operation::Update {
                            old,
                            new: new.clone(),
                        }),
                        (true, false) => output.push(Operation::Delete { old }),
                        (false, true) => output.push(Operation::Insert { new: new.clone() }),
                        (false, false) => {}
                    }
                    self.outer[index] = new;
                }
                Operation::BatchInsert { new } => {
                    let mut visible = vec![];
                    for row in &new {
                        if self.evaluate(row)? {
                            visible.push(row.clone());
                        }
                    }
                    self.outer.extend(new);
                    if !visible.is_empty() {
                        output.push(Operation::BatchInsert { new: visible });
                    }
                }
            }
        } else {
            let index = usize::from(op.port - 1);
            if index >= self.inner.len() {
                return Err(PipelineError::InvalidPortHandle(op.port).into());
            }
            let rows = self.outer.clone();
            let before = rows
                .iter()
                .map(|row| self.evaluate(row))
                .collect::<Result<Vec<_>, _>>()?;
            // Apply the entire update/batch before recomputing visibility. Equal
            // old/new keys and duplicate changes never produce transient output.
            let mut next = self.inner[index].clone();
            Self::update_inner(&mut next, &op.op)?;
            let previous = std::mem::replace(&mut self.inner[index], next);
            let after = rows
                .iter()
                .map(|row| self.evaluate(row))
                .collect::<Result<Vec<_>, _>>();
            let after = match after {
                Ok(after) => after,
                Err(error) => {
                    self.inner[index] = previous;
                    return Err(error.into());
                }
            };
            for ((row, before), after) in rows.into_iter().zip(before).zip(after) {
                match (before, after) {
                    (false, true) => output.push(Operation::Insert { new: row }),
                    (true, false) => output.push(Operation::Delete { old: row }),
                    _ => {}
                }
            }
        }
        for operation in output {
            fw.send(TableOperation {
                id: op.id,
                port: DEFAULT_PORT_HANDLE,
                op: operation,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::utils::{create_test_runtime, get_select};
    use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

    #[derive(Default)]
    struct Forwarder(Vec<Operation>);
    impl ProcessorChannelForwarder for Forwarder {
        fn send(&mut self, operation: TableOperation) {
            self.0.push(operation.op);
        }
    }
    struct TestJoin(Box<dyn Processor>);
    impl TestJoin {
        async fn new(predicate: &str) -> Self {
            let (factory, schemas) = setup(predicate);
            let expected = schemas[&0].clone();
            assert_eq!(
                factory.get_output_schema(&0, &schemas).await.unwrap(),
                expected
            );
            Self(
                factory
                    .build(schemas, HashMap::new(), EventHub::new(1))
                    .await
                    .unwrap(),
            )
        }
        fn apply(&mut self, port: PortHandle, operation: Operation) -> Vec<Operation> {
            let mut output = Forwarder::default();
            self.0
                .process(TableOperation::without_id(operation, port), &mut output)
                .unwrap();
            output.0
        }
    }
    fn test_runtime() -> Arc<Runtime> {
        static RUNTIME: std::sync::OnceLock<Arc<Runtime>> = std::sync::OnceLock::new();
        RUNTIME.get_or_init(create_test_runtime).clone()
    }
    fn setup(predicate: &str) -> (MembershipJoinFactory, HashMap<PortHandle, Schema>) {
        let selection = get_select(&format!("SELECT * FROM outer_rows WHERE {predicate}"))
            .unwrap()
            .selection
            .clone()
            .unwrap();
        let (plan, queries) = MembershipPlan::extract(selection);
        let mut outer = Schema::new();
        for (name, typ) in [
            ("id", FieldType::Int),
            ("k", FieldType::Int),
            ("payload", FieldType::String),
        ] {
            outer.field(
                FieldDefinition::new(name.into(), typ, true, SourceDefinition::Dynamic),
                name == "id",
            );
        }
        let mut inner = Schema::new();
        inner.field(
            FieldDefinition::new("k".into(), FieldType::Int, true, SourceDefinition::Dynamic),
            false,
        );
        let mut schemas = HashMap::from([(0, outer)]);
        for index in 0..queries.len() {
            schemas.insert((index + 1) as PortHandle, inner.clone());
        }
        (
            MembershipJoinFactory::new("test".into(), plan, vec![], test_runtime()),
            schemas,
        )
    }
    fn row(id: i64, key: Option<i64>, payload: &str) -> Record {
        Record::new(vec![
            Field::Int(id),
            key.map_or(Field::Null, Field::Int),
            Field::String(payload.into()),
        ])
    }
    fn key(key: Option<i64>) -> Record {
        Record::new(vec![key.map_or(Field::Null, Field::Int)])
    }
    fn insert(new: Record) -> Operation {
        Operation::Insert { new }
    }
    fn delete(old: Record) -> Operation {
        Operation::Delete { old }
    }

    #[tokio::test]
    async fn late_inner_duplicates_and_retractions_preserve_outer_multiplicity() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows)").await;
        let a = row(1, Some(7), "a");
        let b = row(2, Some(7), "b");
        assert!(join.apply(0, insert(a.clone())).is_empty());
        assert!(join.apply(0, insert(a.clone())).is_empty());
        assert!(join.apply(0, insert(b.clone())).is_empty());
        assert_eq!(
            join.apply(1, insert(key(Some(7)))),
            vec![insert(a.clone()), insert(a.clone()), insert(b.clone())]
        );
        assert!(join.apply(1, insert(key(Some(7)))).is_empty());
        assert!(join.apply(1, delete(key(Some(7)))).is_empty());
        assert_eq!(join.apply(0, delete(a.clone())), vec![delete(a.clone())]);
        assert_eq!(
            join.apply(1, delete(key(Some(7)))),
            vec![delete(a), delete(b)]
        );
    }

    #[tokio::test]
    async fn outer_updates_keep_update_events_and_all_visibility_transitions() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows)").await;
        join.apply(1, insert(key(Some(7))));
        let a = row(1, Some(7), "a");
        let b = row(1, Some(7), "b");
        let c = row(1, Some(8), "c");
        let d = row(1, Some(9), "d");
        assert_eq!(join.apply(0, insert(a.clone())), vec![insert(a.clone())]);
        let change = Operation::Update {
            old: a,
            new: b.clone(),
        };
        assert_eq!(join.apply(0, change.clone()), vec![change]);
        assert_eq!(
            join.apply(
                0,
                Operation::Update {
                    old: b.clone(),
                    new: c.clone()
                }
            ),
            vec![delete(b.clone())]
        );
        assert!(join
            .apply(
                0,
                Operation::Update {
                    old: c,
                    new: d.clone()
                }
            )
            .is_empty());
        assert_eq!(
            join.apply(
                0,
                Operation::Update {
                    old: d,
                    new: b.clone()
                }
            ),
            vec![insert(b)]
        );
    }

    #[tokio::test]
    async fn inner_updates_and_batches_are_atomic() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows)").await;
        let a = row(1, Some(7), "a");
        let b = row(2, Some(8), "b");
        assert!(join
            .apply(
                0,
                Operation::BatchInsert {
                    new: vec![a.clone(), b.clone()]
                }
            )
            .is_empty());
        assert_eq!(
            join.apply(
                1,
                Operation::BatchInsert {
                    new: vec![key(Some(7)), key(Some(7))]
                }
            ),
            vec![insert(a.clone())]
        );
        assert!(join
            .apply(
                1,
                Operation::Update {
                    old: key(Some(7)),
                    new: key(Some(7))
                }
            )
            .is_empty());
        assert_eq!(
            join.apply(
                1,
                Operation::Update {
                    old: key(Some(7)),
                    new: key(Some(8))
                }
            ),
            vec![insert(b.clone())]
        );
        assert_eq!(join.apply(1, delete(key(Some(7)))), vec![delete(a.clone())]);
        assert_eq!(
            join.apply(
                0,
                Operation::BatchInsert {
                    new: vec![a, b.clone()]
                }
            ),
            vec![Operation::BatchInsert { new: vec![b] }]
        );
    }

    #[tokio::test]
    async fn null_empty_and_null_only_rhs_obey_three_valued_membership() {
        for negated in [false, true] {
            let op = if negated { "NOT IN" } else { "IN" };
            let mut join = TestJoin::new(&format!("k {op} (SELECT k FROM inner_rows)")).await;
            let a = row(1, Some(7), "a");
            let n = row(2, None, "null");
            assert_eq!(
                join.apply(0, insert(a.clone())),
                if negated {
                    vec![insert(a.clone())]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                join.apply(0, insert(n.clone())),
                if negated {
                    vec![insert(n.clone())]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                join.apply(1, insert(key(None))),
                if negated {
                    vec![delete(a.clone()), delete(n.clone())]
                } else {
                    vec![]
                }
            );
            assert_eq!(
                join.apply(1, insert(key(Some(7)))),
                if negated {
                    vec![]
                } else {
                    vec![insert(a.clone())]
                }
            );
            assert!(join.apply(1, delete(key(None))).is_empty());
            assert_eq!(
                join.apply(1, delete(key(Some(7)))),
                if negated {
                    vec![insert(a.clone()), insert(n)]
                } else {
                    vec![delete(a)]
                }
            );
        }
    }

    #[tokio::test]
    async fn boolean_composition_does_not_filter_before_or_or_lose_unknown_under_not() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows) OR id = 1").await;
        let a = row(1, Some(7), "a");
        let b = row(2, Some(7), "b");
        assert_eq!(join.apply(0, insert(a.clone())), vec![insert(a)]);
        assert!(join.apply(0, insert(b.clone())).is_empty());
        assert_eq!(join.apply(1, insert(key(Some(7)))), vec![insert(b)]);
        let mut join = TestJoin::new("NOT (k IN (SELECT k FROM inner_rows) AND id = 1)").await;
        join.apply(1, insert(key(None)));
        assert!(join.apply(0, insert(row(1, Some(7), "a"))).is_empty());
        let b = row(2, Some(7), "b");
        assert_eq!(join.apply(0, insert(b.clone())), vec![insert(b)]);
    }

    #[tokio::test]
    async fn multiple_subqueries_keep_independent_state() {
        let mut join = TestJoin::new("k IN (SELECT k FROM a) AND k IN (SELECT k FROM b)").await;
        let a = row(1, Some(7), "a");
        join.apply(0, insert(a.clone()));
        assert!(join.apply(1, insert(key(Some(7)))).is_empty());
        assert_eq!(join.apply(2, insert(key(Some(7)))), vec![insert(a.clone())]);
        assert_eq!(join.apply(1, delete(key(Some(7)))), vec![delete(a)]);
    }

    #[tokio::test]
    async fn invalid_arity_type_and_missing_outer_column_are_rejected() {
        let (factory, mut schemas) = setup("k IN (SELECT k FROM inner_rows)");
        schemas.get_mut(&1).unwrap().fields.clear();
        assert!(factory.get_output_schema(&0, &schemas).await.is_err());
        let (factory, mut schemas) = setup("k IN (SELECT k FROM inner_rows)");
        let extra = schemas[&0].fields[0].clone();
        schemas.get_mut(&1).unwrap().fields.push(extra);
        assert!(factory
            .build(schemas, HashMap::new(), EventHub::new(1))
            .await
            .is_err());
        let (factory, mut schemas) = setup("k IN (SELECT k FROM inner_rows)");
        schemas.get_mut(&1).unwrap().fields[0].typ = FieldType::String;
        assert!(factory
            .build(schemas, HashMap::new(), EventHub::new(1))
            .await
            .is_err());
        let (factory, schemas) = setup("missing_column IN (SELECT k FROM inner_rows)");
        assert!(factory
            .build(schemas, HashMap::new(), EventHub::new(1))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn invalid_input_does_not_corrupt_membership_state() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows)").await;
        let a = row(1, Some(7), "a");
        join.apply(0, insert(a.clone()));
        let mut output = Forwarder::default();
        assert!(join
            .0
            .process(
                TableOperation::without_id(insert(key(Some(7))), 2),
                &mut output
            )
            .is_err());
        assert!(join
            .0
            .process(
                TableOperation::without_id(delete(key(Some(7))), 1),
                &mut output
            )
            .is_err());
        assert!(output.0.is_empty());
        assert_eq!(join.apply(1, insert(key(Some(7)))), vec![insert(a)]);
    }

    // Expected snapshots were independently evaluated using SQLite, not the
    // incremental membership implementation. Each input changes one source.
    #[tokio::test]
    async fn incremental_events_match_sqlite_snapshot_oracle() {
        let events = vec![
            (
                0,
                insert(row(1, Some(7), "a")),
                vec![],
                vec![row(1, Some(7), "a")],
            ), // outer before inner
            (
                0,
                insert(row(2, Some(7), "b")),
                vec![],
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
            ), // duplicate outer key
            (
                0,
                insert(row(3, None, "c")),
                vec![],
                vec![
                    row(1, Some(7), "a"),
                    row(2, Some(7), "b"),
                    row(3, None, "c"),
                ],
            ), // null outer with empty inner
            (
                1,
                insert(key(Some(7))),
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
                vec![],
            ), // late inner match
            (
                1,
                insert(key(Some(7))),
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
                vec![],
            ), // duplicate inner key
            (
                1,
                delete(key(Some(7))),
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
                vec![],
            ), // one duplicate retracted
            (
                0,
                Operation::Update {
                    old: row(1, Some(7), "a"),
                    new: row(1, Some(8), "a"),
                },
                vec![row(2, Some(7), "b")],
                vec![row(1, Some(8), "a")],
            ), // outer update matched to unmatched
            (
                1,
                insert(key(Some(8))),
                vec![row(1, Some(8), "a"), row(2, Some(7), "b")],
                vec![],
            ), // second late match
            (
                1,
                insert(key(None)),
                vec![row(1, Some(8), "a"), row(2, Some(7), "b")],
                vec![],
            ), // inner null inserted
            (1, delete(key(Some(7))), vec![row(1, Some(8), "a")], vec![]), // last duplicate retracted
            (
                1,
                delete(key(None)),
                vec![row(1, Some(8), "a")],
                vec![row(2, Some(7), "b")],
            ), // inner null retracted
            (
                0,
                Operation::Update {
                    old: row(1, Some(8), "a"),
                    new: row(1, Some(8), "changed"),
                },
                vec![row(1, Some(8), "changed")],
                vec![row(2, Some(7), "b")],
            ), // outer matched payload update
            (
                1,
                Operation::Update {
                    old: key(Some(8)),
                    new: key(Some(9)),
                },
                vec![],
                vec![row(1, Some(8), "changed"), row(2, Some(7), "b")],
            ), // inner key update
            (
                0,
                Operation::Update {
                    old: row(2, Some(7), "b"),
                    new: row(2, Some(9), "b"),
                },
                vec![row(2, Some(9), "b")],
                vec![row(1, Some(8), "changed")],
            ), // outer update unmatched to matched
            (
                0,
                delete(row(2, Some(9), "b")),
                vec![],
                vec![row(1, Some(8), "changed")],
            ), // outer matched delete
            (
                1,
                delete(key(Some(9))),
                vec![],
                vec![row(1, Some(8), "changed"), row(3, None, "c")],
            ), // inner becomes empty
            (1, insert(key(None)), vec![], vec![]), // inner becomes null only
            (
                1,
                delete(key(None)),
                vec![],
                vec![row(1, Some(8), "changed"), row(3, None, "c")],
            ), // null-only inner becomes empty
        ];
        for negated in [false, true] {
            let operator = if negated { "NOT IN" } else { "IN" };
            let mut join = TestJoin::new(&format!("k {operator} (SELECT k FROM inner_rows)")).await;
            let mut snapshot = vec![];
            for (step, (port, input, positive, negative)) in events.iter().enumerate() {
                for output in join.apply(*port, input.clone()) {
                    match output {
                        Operation::Insert { new } => snapshot.push(new),
                        Operation::Delete { old } => {
                            let index = snapshot
                                .iter()
                                .position(|row| row == &old)
                                .expect("retraction must exist");
                            snapshot.remove(index);
                        }
                        Operation::Update { old, new } => {
                            let index = snapshot
                                .iter()
                                .position(|row| row == &old)
                                .expect("updated row must exist");
                            snapshot[index] = new;
                        }
                        Operation::BatchInsert { new } => snapshot.extend(new),
                    }
                }
                snapshot.sort_by_key(|row| match row.values[0] {
                    Field::Int(id) => id,
                    _ => unreachable!(),
                });
                assert_eq!(
                    &snapshot,
                    if negated { negative } else { positive },
                    "step {step}, negated {negated}"
                );
            }
        }
    }

    #[tokio::test]
    async fn multiple_subquery_boolean_events_match_sqlite_snapshot_oracle() {
        let mut join =
            TestJoin::new("(k IN (SELECT k FROM a) OR id = 1) AND k NOT IN (SELECT k FROM b)")
                .await;
        let events = vec![
            (0, insert(row(1, Some(7), "a")), vec![row(1, Some(7), "a")]),
            (0, insert(row(2, Some(8), "b")), vec![row(1, Some(7), "a")]),
            (
                1,
                insert(key(Some(8))),
                vec![row(1, Some(7), "a"), row(2, Some(8), "b")],
            ),
            (2, insert(key(None)), vec![]),
            (2, insert(key(Some(7))), vec![]),
            (2, delete(key(None)), vec![row(2, Some(8), "b")]),
            (
                1,
                Operation::Update {
                    old: key(Some(8)),
                    new: key(Some(7)),
                },
                vec![],
            ),
            (
                0,
                Operation::Update {
                    old: row(2, Some(8), "b"),
                    new: row(2, Some(7), "b"),
                },
                vec![],
            ),
            (
                2,
                delete(key(Some(7))),
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
            ),
            (
                1,
                insert(key(None)),
                vec![row(1, Some(7), "a"), row(2, Some(7), "b")],
            ),
            (1, delete(key(Some(7))), vec![row(1, Some(7), "a")]),
            (1, delete(key(None)), vec![row(1, Some(7), "a")]),
            (0, insert(row(3, None, "c")), vec![row(1, Some(7), "a")]),
        ];
        let mut snapshot = vec![];
        for (step, (port, input, expected)) in events.into_iter().enumerate() {
            for output in join.apply(port, input) {
                match output {
                    Operation::Insert { new } => snapshot.push(new),
                    Operation::Delete { old } => {
                        let index = snapshot
                            .iter()
                            .position(|row| row == &old)
                            .expect("retraction must exist");
                        snapshot.remove(index);
                    }
                    Operation::Update { old, new } => {
                        let index = snapshot
                            .iter()
                            .position(|row| row == &old)
                            .expect("updated row must exist");
                        snapshot[index] = new;
                    }
                    Operation::BatchInsert { new } => snapshot.extend(new),
                }
            }
            snapshot.sort_by_key(|row| match row.values[0] {
                Field::Int(id) => id,
                _ => unreachable!(),
            });
            assert_eq!(snapshot, expected, "step {step}");
        }
    }

    #[tokio::test]
    async fn lifetime_inputs_are_rejected_without_changing_state() {
        let mut join = TestJoin::new("k IN (SELECT k FROM inner_rows)").await;
        let mut expiring = key(Some(7));
        expiring.lifetime = Some(dozer_types::types::Lifetime {
            reference: dozer_types::chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
                .unwrap(),
            duration: std::time::Duration::from_secs(1),
        });
        let mut output = Forwarder::default();
        assert!(join
            .0
            .process(TableOperation::without_id(insert(expiring), 1), &mut output)
            .is_err());
        assert!(join.apply(0, insert(row(1, Some(7), "a"))).is_empty());
        assert!(output.0.is_empty());
    }

    #[tokio::test]
    async fn null_literal_outer_expression_obeys_empty_rhs_semantics() {
        let mut join = TestJoin::new("NULL NOT IN (SELECT k FROM inner_rows)").await;
        let a = row(1, Some(7), "a");
        assert_eq!(join.apply(0, insert(a.clone())), vec![insert(a.clone())]);
        assert_eq!(join.apply(1, insert(key(Some(7)))), vec![delete(a.clone())]);
        assert_eq!(join.apply(1, delete(key(Some(7)))), vec![insert(a)]);
    }
}
