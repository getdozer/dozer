//! A null-aware mark join for a scalar IN subquery. Unlike an inner join, an
//! inner duplicate does not multiply outer rows. The appended mark lets the
//! normal selection processor evaluate arbitrary compound WHERE expressions.

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
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema, TableOperation},
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

use super::factory::{LEFT_JOIN_PORT, RIGHT_JOIN_PORT};

#[derive(Debug)]
pub(crate) struct MarkJoinProcessorFactory {
    id: String,
    expression: Expr,
    mark_name: String,
    negated: bool,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl MarkJoinProcessorFactory {
    pub(crate) fn new(
        id: String,
        expression: Expr,
        mark_name: String,
        negated: bool,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            expression,
            mark_name,
            negated,
            udfs,
            runtime,
        }
    }

    fn schemas<'a>(
        &self,
        schemas: &'a HashMap<PortHandle, Schema>,
    ) -> Result<(&'a Schema, &'a Schema), PipelineError> {
        let left = schemas
            .get(&LEFT_JOIN_PORT)
            .ok_or(PipelineError::InvalidPortHandle(LEFT_JOIN_PORT))?;
        let right = schemas
            .get(&RIGHT_JOIN_PORT)
            .ok_or(PipelineError::InvalidPortHandle(RIGHT_JOIN_PORT))?;
        if right.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "IN subquery must return exactly one column".into(),
            ));
        }
        if left.fields.iter().any(|field| field.name == self.mark_name) {
            return Err(PipelineError::InvalidQuery(format!(
                "Reserved IN subquery column: {}",
                self.mark_name
            )));
        }
        Ok((left, right))
    }
}

#[async_trait]
impl ProcessorFactory for MarkJoinProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "MarkJoin".into()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![LEFT_JOIN_PORT, RIGHT_JOIN_PORT]
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _port: &PortHandle,
        schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let (left, _) = self.schemas(schemas)?;
        let mut output = left.clone();
        output.fields.push(FieldDefinition::new(
            self.mark_name.clone(),
            FieldType::Boolean,
            true,
            Default::default(),
        ));
        Ok(output)
    }

    async fn build(
        &self,
        schemas: HashMap<PortHandle, Schema>,
        _outputs: HashMap<PortHandle, Schema>,
        _hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let (left, right) = self.schemas(&schemas)?;
        let expression = ExpressionBuilder::new(left.fields.len(), self.runtime.clone())
            .build(false, &self.expression, left, &self.udfs)
            .await?;
        let same_type = matches!(&expression, Expression::Literal(Field::Null))
            || expression.get_type(left)?.return_type == right.fields[0].typ;
        Ok(Box::new(MarkJoinProcessor {
            expression,
            schema: left.clone(),
            same_type,
            negated: self.negated,
            left: HashMap::new(),
            right: HashMap::new(),
            right_len: 0,
        }))
    }
}

#[derive(Debug, Default)]
struct OuterGroup {
    records: HashMap<Record, usize>,
    matches: usize,
}

#[derive(Debug)]
struct MarkJoinProcessor {
    expression: Expression,
    schema: Schema,
    same_type: bool,
    negated: bool,
    left: HashMap<Field, OuterGroup>,
    right: HashMap<Field, usize>,
    right_len: usize,
}

// A batch/update is applied as a single change. In particular replacing one
// matching inner row with another must not produce a transient retraction.
#[derive(Default)]
struct Delta {
    removed: usize,
    added: usize,
}

impl Delta {
    fn apply(&self, count: usize) -> Result<usize, PipelineError> {
        count
            .checked_sub(self.removed)
            .and_then(|count| count.checked_add(self.added))
            .ok_or_else(|| PipelineError::InvalidValue("Unbalanced IN subquery change".into()))
    }
}

impl MarkJoinProcessor {
    fn equals(&self, left: &Field, right: &Field) -> Result<bool, PipelineError> {
        if left == &Field::Null || right == &Field::Null {
            return Ok(false);
        }
        if self.same_type {
            return Ok(left == right);
        }
        match BinaryOperatorType::Eq.evaluate(
            &self.schema,
            &mut Expression::Literal(left.clone()),
            &mut Expression::Literal(right.clone()),
            &Record::default(),
        )? {
            Field::Boolean(equal) => Ok(equal),
            _ => Err(PipelineError::InvalidQuery(format!(
                "IN subquery operands cannot be compared: {left:?} and {right:?}"
            ))),
        }
    }

    fn match_count(&self, key: &Field) -> Result<usize, PipelineError> {
        if let Some(group) = self.left.get(key) {
            return Ok(group.matches);
        }
        if key == &Field::Null {
            return Ok(0);
        }
        if self.same_type {
            return Ok(self.right.get(key).copied().unwrap_or(0));
        }
        self.right.iter().try_fold(0, |total, (value, count)| {
            Ok(total + if self.equals(key, value)? { *count } else { 0 })
        })
    }

    fn mark(&self, key: &Field, matches: usize, nulls: usize, total: usize) -> Field {
        let included = if total == 0 {
            false
        } else if matches > 0 {
            true
        } else if key == &Field::Null || nulls > 0 {
            return Field::Null;
        } else {
            false
        };
        Field::Boolean(included != self.negated)
    }

    fn marked(&self, record: &Record, key: &Field, matches: usize) -> Record {
        let mut record = record.clone();
        record.values.push(self.mark(
            key,
            matches,
            self.right.get(&Field::Null).copied().unwrap_or(0),
            self.right_len,
        ));
        record
    }

    fn insert_left(&mut self, record: Record, key: Field, matches: usize) {
        let group = self.left.entry(key).or_insert_with(|| OuterGroup {
            records: HashMap::new(),
            matches,
        });
        *group.records.entry(record).or_default() += 1;
    }

    fn delete_left(&mut self, record: &Record, key: &Field) -> Result<(), PipelineError> {
        let group = self.left.get_mut(key).ok_or_else(|| {
            PipelineError::InvalidValue("Unknown outer row in IN subquery delete".into())
        })?;
        let count = group.records.get_mut(record).ok_or_else(|| {
            PipelineError::InvalidValue("Unknown outer row in IN subquery delete".into())
        })?;
        *count -= 1;
        if *count == 0 {
            group.records.remove(record);
        }
        if group.records.is_empty() {
            self.left.remove(key);
        }
        Ok(())
    }

    fn process_left(&mut self, op: Operation) -> Result<Operation, PipelineError> {
        Ok(match op {
            Operation::Insert { new } => {
                let key = self.expression.evaluate(&new, &self.schema)?;
                let matches = self.match_count(&key)?;
                let marked = self.marked(&new, &key, matches);
                self.insert_left(new, key, matches);
                Operation::Insert { new: marked }
            }
            Operation::Delete { old } => {
                let key = self.expression.evaluate(&old, &self.schema)?;
                let matches = self.match_count(&key)?;
                let marked = self.marked(&old, &key, matches);
                self.delete_left(&old, &key)?;
                Operation::Delete { old: marked }
            }
            Operation::Update { old, new } => {
                let old_key = self.expression.evaluate(&old, &self.schema)?;
                let new_key = self.expression.evaluate(&new, &self.schema)?;
                let old_matches = self.match_count(&old_key)?;
                let new_matches = self.match_count(&new_key)?;
                let marked_old = self.marked(&old, &old_key, old_matches);
                let marked_new = self.marked(&new, &new_key, new_matches);
                self.delete_left(&old, &old_key)?;
                self.insert_left(new, new_key, new_matches);
                Operation::Update {
                    old: marked_old,
                    new: marked_new,
                }
            }
            Operation::BatchInsert { new } => {
                let prepared = new
                    .into_iter()
                    .map(|record| {
                        let key = self.expression.evaluate(&record, &self.schema)?;
                        let matches = self.match_count(&key)?;
                        Ok((record, key, matches))
                    })
                    .collect::<Result<Vec<_>, PipelineError>>()?;
                let mut new = Vec::with_capacity(prepared.len());
                for (record, key, matches) in prepared {
                    new.push(self.marked(&record, &key, matches));
                    self.insert_left(record, key, matches);
                }
                Operation::BatchInsert { new }
            }
        })
    }

    fn process_right(&mut self, op: Operation) -> Result<Vec<Operation>, PipelineError> {
        let mut deltas: HashMap<Field, Delta> = HashMap::new();
        let (removed, added) = match op {
            Operation::Insert { new } => (vec![], vec![new]),
            Operation::Delete { old } => (vec![old], vec![]),
            Operation::Update { old, new } => (vec![old], vec![new]),
            Operation::BatchInsert { new } => (vec![], new),
        };
        let total = Delta {
            removed: removed.len(),
            added: added.len(),
        }
        .apply(self.right_len)?;
        for record in removed {
            deltas
                .entry(record.get_value(0)?.clone())
                .or_default()
                .removed += 1;
        }
        for record in added {
            deltas
                .entry(record.get_value(0)?.clone())
                .or_default()
                .added += 1;
        }
        let mut next_counts = HashMap::new();
        for (key, delta) in &deltas {
            next_counts.insert(
                key.clone(),
                delta.apply(self.right.get(key).copied().unwrap_or(0))?,
            );
        }
        let old_nulls = self.right.get(&Field::Null).copied().unwrap_or(0);
        let nulls = next_counts.get(&Field::Null).copied().unwrap_or(old_nulls);
        let global_change =
            (old_nulls == 0) != (nulls == 0) || (self.right_len == 0) != (total == 0);
        // Same-type keys use a hash lookup. Cross-type keys use the same SQL
        // equality/coercion implementation as WHERE, over distinct outer keys.
        let affected: Vec<_> = if global_change || !self.same_type {
            self.left.keys().cloned().collect()
        } else {
            deltas
                .keys()
                .filter(|key| **key != Field::Null && self.left.contains_key(*key))
                .cloned()
                .collect()
        };
        let mut changes = Vec::new();
        for key in affected {
            let group = &self.left[&key];
            let mut matching_delta = Delta::default();
            if self.same_type {
                if key != Field::Null {
                    if let Some(delta) = deltas.get(&key) {
                        matching_delta.added = delta.added;
                        matching_delta.removed = delta.removed;
                    }
                }
            } else {
                for (value, delta) in &deltas {
                    if self.equals(&key, value)? {
                        matching_delta.added += delta.added;
                        matching_delta.removed += delta.removed;
                    }
                }
            }
            let matches = matching_delta.apply(group.matches)?;
            let old_mark = self.mark(&key, group.matches, old_nulls, self.right_len);
            let new_mark = self.mark(&key, matches, nulls, total);
            changes.push((key, matches, old_mark, new_mark));
        }
        let mut output = Vec::new();
        for (key, matches, old_mark, new_mark) in changes {
            let group = self.left.get_mut(&key).unwrap();
            group.matches = matches;
            if old_mark != new_mark {
                for (record, count) in &group.records {
                    let mut old = record.clone();
                    old.values.push(old_mark.clone());
                    let mut new = record.clone();
                    new.values.push(new_mark.clone());
                    for _ in 0..*count {
                        output.push(Operation::Update {
                            old: old.clone(),
                            new: new.clone(),
                        });
                    }
                }
            }
        }
        for (key, count) in next_counts {
            if count == 0 {
                self.right.remove(&key);
            } else {
                self.right.insert(key, count);
            }
        }
        self.right_len = total;
        Ok(output)
    }
}

impl Processor for MarkJoinProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        // TTL expiration currently has no explicit delete event in Dozer.
        // Silently expiring a join key would leave downstream marks stale, so
        // reject this combination until expiration can propagate atomically.
        let has_lifetime = match &op.op {
            Operation::Insert { new } => new.lifetime.is_some(),
            Operation::Delete { old } => old.lifetime.is_some(),
            Operation::Update { old, new } => old.lifetime.is_some() || new.lifetime.is_some(),
            Operation::BatchInsert { new } => new.iter().any(|record| record.lifetime.is_some()),
        };
        if has_lifetime {
            return Err(PipelineError::InvalidQuery(
                "IN subqueries do not support TTL inputs".into(),
            )
            .into());
        }
        let output = match op.port {
            LEFT_JOIN_PORT => vec![self.process_left(op.op)?],
            RIGHT_JOIN_PORT => self.process_right(op.op)?,
            port => return Err(PipelineError::InvalidPortHandle(port).into()),
        };
        for change in output {
            fw.send(TableOperation {
                id: op.id,
                op: change,
                port: DEFAULT_PORT_HANDLE,
            });
        }
        Ok(())
    }
}
