//! Incremental membership of an outer expression in a single-column relation.
//!
//! Every outer row is retained, including rows whose membership is false or
//! unknown. The extra nullable boolean is consumed by a downstream selection;
//! this keeps the same operator usable inside compound WHERE predicates.

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

pub(crate) const OUTER_PORT: PortHandle = 0;
pub(crate) const VALUES_PORT: PortHandle = 1;

#[derive(Debug)]
pub(crate) struct MembershipProcessorFactory {
    pub id: String,
    pub operand: Expr,
    pub negated: bool,
    pub column: String,
    pub udfs: Vec<UdfConfig>,
    pub runtime: Arc<Runtime>,
}

impl MembershipProcessorFactory {
    fn outer_schema<'a>(
        &self,
        inputs: &'a HashMap<PortHandle, Schema>,
    ) -> Result<&'a Schema, PipelineError> {
        let outer = inputs
            .get(&OUTER_PORT)
            .ok_or(PipelineError::InvalidPortHandle(OUTER_PORT))?;
        let values = inputs
            .get(&VALUES_PORT)
            .ok_or(PipelineError::InvalidPortHandle(VALUES_PORT))?;
        if values.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "An IN subquery must return one column".into(),
            ));
        }
        if outer.fields.iter().any(|field| field.name == self.column) {
            return Err(PipelineError::InvalidQuery(format!(
                "IN subquery output column already exists: {}",
                self.column
            )));
        }
        Ok(outer)
    }
}

#[async_trait]
impl ProcessorFactory for MembershipProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Membership".into()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![OUTER_PORT, VALUES_PORT]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output: &PortHandle,
        inputs: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let mut schema = self.outer_schema(inputs)?.clone();
        schema.fields.push(FieldDefinition::new(
            self.column.clone(),
            FieldType::Boolean,
            true,
            Default::default(),
        ));
        Ok(schema)
    }

    async fn build(
        &self,
        inputs: HashMap<PortHandle, Schema>,
        _outputs: HashMap<PortHandle, Schema>,
        _events: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = self.outer_schema(&inputs)?;
        let operand = ExpressionBuilder::new(schema.fields.len(), self.runtime.clone())
            .build(false, &self.operand, schema, &self.udfs)
            .await?;
        Ok(Box::new(MembershipProcessor::new(
            schema.clone(),
            operand,
            self.negated,
        )?))
    }
}

#[derive(Debug)]
struct RetainedRow {
    record: Record,
    operand: Field,
    copies: usize,
    matches: usize,
    unknown: usize,
}

#[derive(Debug)]
pub(crate) struct MembershipProcessor {
    schema: Schema,
    operand: Expression,
    negated: bool,
    rows: HashMap<Vec<u8>, RetainedRow>,
    values: HashMap<Field, usize>,
    value_count: usize,
}

impl MembershipProcessor {
    pub(crate) fn new(
        schema: Schema,
        operand: Expression,
        negated: bool,
    ) -> Result<Self, PipelineError> {
        validate_operand(&operand)?;
        Ok(Self {
            schema,
            operand,
            negated,
            rows: HashMap::new(),
            values: HashMap::new(),
            value_count: 0,
        })
    }

    fn result(&self, matches: usize, unknown: usize, total: usize) -> Field {
        if total == 0 {
            Field::Boolean(self.negated)
        } else if matches != 0 {
            Field::Boolean(!self.negated)
        } else if unknown != 0 {
            Field::Null
        } else {
            Field::Boolean(self.negated)
        }
    }

    fn equal(&self, left: &Field, right: &Field) -> Result<Option<bool>, PipelineError> {
        if left == &Field::Null || right == &Field::Null {
            return Ok(None);
        }
        // Field equality distinguishes numeric variants; SQL equality performs
        // the same conversions as an ordinary WHERE comparison.
        let value = BinaryOperatorType::Eq.evaluate(
            &self.schema,
            &mut Expression::Literal(left.clone()),
            &mut Expression::Literal(right.clone()),
            &Record::default(),
        )?;
        match value {
            Field::Boolean(value) => Ok(Some(value)),
            Field::Null => Ok(None),
            _ => Err(PipelineError::InvalidQuery(
                "IN operands have no boolean equality comparison".into(),
            )),
        }
    }

    fn prepare_row(&mut self, row: &Record) -> Result<(Vec<u8>, RetainedRow), PipelineError> {
        validate_lifetime(row)?;
        if row.values.len() != self.schema.fields.len() {
            return Err(PipelineError::InvalidValue(
                "IN outer row does not match its schema".into(),
            ));
        }
        let operand = self.operand.evaluate(row, &self.schema)?;
        let mut matches = 0;
        let mut unknown = 0;
        for (value, copies) in &self.values {
            match self.equal(&operand, value)? {
                Some(true) => matches += copies,
                None => unknown += copies,
                Some(false) => {}
            }
        }
        Ok((
            row_key(row)?,
            RetainedRow {
                record: row.clone(),
                operand,
                copies: 1,
                matches,
                unknown,
            },
        ))
    }

    fn marked(&self, row: &Record, retained: &RetainedRow) -> Record {
        append_result(
            row,
            self.result(retained.matches, retained.unknown, self.value_count),
        )
    }

    fn retained(&self, row: &Record) -> Result<&RetainedRow, PipelineError> {
        validate_lifetime(row)?;
        self.rows.get(&row_key(row)?).ok_or_else(|| {
            PipelineError::InvalidValue("IN received a change for an unknown outer row".into())
        })
    }

    fn remove_row(&mut self, key: &[u8]) {
        let retained = self.rows.get_mut(key).expect("outer row validated");
        retained.copies -= 1;
        if retained.copies == 0 {
            self.rows.remove(key);
        }
    }

    fn add_row(&mut self, key: Vec<u8>, retained: RetainedRow) {
        if let Some(existing) = self.rows.get_mut(&key) {
            existing.copies += 1;
        } else {
            self.rows.insert(key, retained);
        }
    }

    fn change_outer(&mut self, operation: Operation) -> Result<Operation, PipelineError> {
        match operation {
            Operation::Insert { new } => {
                let (key, retained) = self.prepare_row(&new)?;
                let marked = self.marked(&new, &retained);
                self.add_row(key, retained);
                Ok(Operation::Insert { new: marked })
            }
            Operation::Delete { old } => {
                let key = row_key(&old)?;
                let marked = self.marked(&old, self.retained(&old)?);
                self.remove_row(&key);
                Ok(Operation::Delete { old: marked })
            }
            Operation::Update { old, new } => {
                let old_key = row_key(&old)?;
                let previous = self.marked(&old, self.retained(&old)?);
                let (new_key, retained) = self.prepare_row(&new)?;
                let next = self.marked(&new, &retained);
                self.remove_row(&old_key);
                self.add_row(new_key, retained);
                Ok(Operation::Update {
                    old: previous,
                    new: next,
                })
            }
            Operation::BatchInsert { new } => {
                // Evaluate the whole batch before retaining or emitting any row.
                let mut prepared = Vec::with_capacity(new.len());
                for row in new {
                    prepared.push(self.prepare_row(&row)?);
                }
                let mut output = Vec::with_capacity(prepared.len());
                for (key, retained) in prepared {
                    output.push(self.marked(&retained.record, &retained));
                    self.add_row(key, retained);
                }
                Ok(Operation::BatchInsert { new: output })
            }
        }
    }

    fn change_values(&mut self, operation: Operation) -> Result<Vec<Operation>, PipelineError> {
        let (removed, added) = match operation {
            Operation::Insert { new } => (Vec::new(), vec![new]),
            Operation::Delete { old } => (vec![old], Vec::new()),
            Operation::Update { old, new } => (vec![old], vec![new]),
            Operation::BatchInsert { new } => (Vec::new(), new),
        };
        let total = self
            .value_count
            .checked_sub(removed.len())
            .and_then(|count| count.checked_add(added.len()))
            .ok_or_else(|| PipelineError::InvalidValue("Invalid IN inner row count".into()))?;
        let mut replacements = HashMap::new();
        for row in &removed {
            let value = inner_value(row)?;
            let count = replacements
                .entry(value.clone())
                .or_insert_with(|| self.values.get(value).copied().unwrap_or(0));
            *count = count.checked_sub(1).ok_or_else(|| {
                PipelineError::InvalidValue("IN received a delete for an unknown inner row".into())
            })?;
        }
        for row in &added {
            let value = inner_value(row)?;
            *replacements
                .entry(value.clone())
                .or_insert_with(|| self.values.get(value).copied().unwrap_or(0)) += 1;
        }
        let mut changes = Vec::with_capacity(self.rows.len());
        // Calculate every comparison before mutating either input's state. An
        // incompatible value must not partially apply an UPDATE or batch.
        for (key, retained) in &self.rows {
            let mut matches = retained.matches;
            let mut unknown = retained.unknown;
            for (value, next_count) in &replacements {
                let old_count = self.values.get(value).copied().unwrap_or(0);
                if old_count != *next_count {
                    match self.equal(&retained.operand, value)? {
                        Some(true) => matches = matches - old_count + next_count,
                        None => unknown = unknown - old_count + next_count,
                        Some(false) => {}
                    }
                }
            }
            let before = self.result(retained.matches, retained.unknown, self.value_count);
            let after = self.result(matches, unknown, total);
            changes.push((key.clone(), matches, unknown, before, after));
        }
        let mut output = Vec::new();
        for (key, matches, unknown, before, after) in changes {
            let retained = self.rows.get_mut(&key).expect("retained row");
            retained.matches = matches;
            retained.unknown = unknown;
            if before != after {
                let update = Operation::Update {
                    old: append_result(&retained.record, before),
                    new: append_result(&retained.record, after),
                };
                output.extend(std::iter::repeat(update).take(retained.copies));
            }
        }
        for (value, count) in replacements {
            if count == 0 {
                self.values.remove(&value);
            } else {
                self.values.insert(value, count);
            }
        }
        self.value_count = total;
        Ok(output)
    }
}

fn row_key(row: &Record) -> Result<Vec<u8>, PipelineError> {
    // Record equality ignores decimal scale and timestamp offsets, whereas
    // scalar expressions such as CAST(... AS STRING) can distinguish them.
    bincode::encode_to_vec(row, bincode::config::standard())
        .map_err(|error| PipelineError::InternalError(Box::new(error)))
}

fn validate_operand(expression: &Expression) -> Result<(), PipelineError> {
    // The retained operand must be reproducible for identical outer rows.
    // Unknown extension functions are excluded until they declare determinism.
    use Expression::*;
    match expression {
        Column { .. } | Literal(_) => Ok(()),
        UnaryOperator { arg, .. }
        | DateTimeFunction { arg, .. }
        | Cast { arg, .. }
        | IsNull { arg }
        | IsNotNull { arg } => validate_operand(arg),
        BinaryOperator { left, right, .. } => {
            validate_operand(left)?;
            validate_operand(right)
        }
        ScalarFunction { args, .. }
        | GeoFunction { args, .. }
        | ConditionalExpression { args, .. }
        | Json { args, .. } => args.iter().try_for_each(validate_operand),
        Trim { arg, what, .. } => {
            validate_operand(arg)?;
            what.iter()
                .try_for_each(|expression| validate_operand(expression))
        }
        Like { arg, pattern, .. } => {
            validate_operand(arg)?;
            validate_operand(pattern)
        }
        InList { expr, list, .. } => {
            validate_operand(expr)?;
            list.iter().try_for_each(validate_operand)
        }
        Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            operand
                .iter()
                .try_for_each(|expression| validate_operand(expression))?;
            conditions.iter().try_for_each(validate_operand)?;
            results.iter().try_for_each(validate_operand)?;
            else_result
                .iter()
                .try_for_each(|expression| validate_operand(expression))
        }
        _ => Err(PipelineError::InvalidQuery(
            "IN subquery operands must be deterministic scalar expressions".into(),
        )),
    }
}

fn append_result(row: &Record, result: Field) -> Record {
    let mut output = row.clone();
    output.values.push(result);
    output
}

fn validate_lifetime(row: &Record) -> Result<(), PipelineError> {
    if row.lifetime.is_some() {
        // The current TTL path expires join state without sending downstream
        // deletions. Membership cannot stay correct under that contract.
        return Err(PipelineError::InvalidQuery(
            "IN subqueries require explicit deletion events; TTL inputs are unsupported".into(),
        ));
    }
    Ok(())
}

fn inner_value(row: &Record) -> Result<&Field, PipelineError> {
    validate_lifetime(row)?;
    if row.values.len() != 1 {
        return Err(PipelineError::InvalidValue(
            "IN inner row must contain one value".into(),
        ));
    }
    Ok(&row.values[0])
}

impl Processor for MembershipProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        operation: TableOperation,
        forwarder: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let output = match operation.port {
            OUTER_PORT => vec![self.change_outer(operation.op)?],
            VALUES_PORT => self.change_values(operation.op)?,
            port => return Err(PipelineError::InvalidPortHandle(port).into()),
        };
        for op in output {
            forwarder.send(TableOperation {
                id: operation.id,
                port: DEFAULT_PORT_HANDLE,
                op,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
