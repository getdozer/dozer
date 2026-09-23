use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    channels::ProcessorChannelForwarder,
    epoch::Epoch,
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::ExpressionBuilder, execution::Expression, sqlparser::ast::Expr as SqlExpr,
};
use dozer_types::{
    errors::internal::BoxedError,
    models::udf_config::UdfConfig,
    tonic::async_trait,
    types::{Field, Operation, Record, Schema, TableOperation},
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

pub(crate) const IN_SUBQUERY_LEFT_PORT: PortHandle = 0;
pub(crate) const IN_SUBQUERY_RIGHT_PORT: PortHandle = 1;

#[derive(Debug)]
pub struct InSubqueryProcessorFactory {
    id: String,
    expression: SqlExpr,
    negated: bool,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl InSubqueryProcessorFactory {
    pub fn new(
        id: String,
        expression: SqlExpr,
        negated: bool,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            expression,
            negated,
            udfs,
            runtime,
        }
    }

    fn input_schema(
        input_schemas: &HashMap<PortHandle, Schema>,
        port: PortHandle,
    ) -> Result<&Schema, BoxedError> {
        input_schemas
            .get(&port)
            .ok_or_else(|| PipelineError::InvalidPortHandle(port).into())
    }
}

#[async_trait]
impl ProcessorFactory for InSubqueryProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "InSubquery".to_string()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![IN_SUBQUERY_LEFT_PORT, IN_SUBQUERY_RIGHT_PORT]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let right = Self::input_schema(input_schemas, IN_SUBQUERY_RIGHT_PORT)?;
        if right.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "IN subquery must return exactly one column".to_string(),
            )
            .into());
        }
        Ok(Self::input_schema(input_schemas, IN_SUBQUERY_LEFT_PORT)?.clone())
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let left = Self::input_schema(&input_schemas, IN_SUBQUERY_LEFT_PORT)?;
        let right = Self::input_schema(&input_schemas, IN_SUBQUERY_RIGHT_PORT)?;
        if right.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "IN subquery must return exactly one column".to_string(),
            )
            .into());
        }

        let expression = ExpressionBuilder::new(left.fields.len(), self.runtime.clone())
            .build(false, &self.expression, left, &self.udfs)
            .await?;
        Ok(Box::new(InSubqueryProcessor::new(
            left.clone(),
            expression,
            self.negated,
        )))
    }
}

#[derive(Debug)]
pub struct InSubqueryProcessor {
    input_schema: Schema,
    expression: Expression,
    negated: bool,
    left_records: HashMap<Field, HashMap<Record, usize>>,
    right_values: HashMap<Field, usize>,
    right_nulls: usize,
}

impl InSubqueryProcessor {
    pub fn new(input_schema: Schema, expression: Expression, negated: bool) -> Self {
        Self {
            input_schema,
            expression,
            negated,
            left_records: HashMap::new(),
            right_values: HashMap::new(),
            right_nulls: 0,
        }
    }

    fn evaluate(&mut self, record: &Record) -> Result<Field, PipelineError> {
        Ok(self.expression.evaluate(record, &self.input_schema)?)
    }

    fn is_match(&self, value: &Field) -> bool {
        if *value == Field::Null {
            return false;
        }
        let contained = self.right_values.contains_key(value);
        if self.negated {
            !contained && self.right_nulls == 0
        } else {
            contained
        }
    }

    fn remember_left(&mut self, value: Field, record: Record) {
        *self
            .left_records
            .entry(value)
            .or_default()
            .entry(record)
            .or_default() += 1;
    }

    fn forget_left(&mut self, value: &Field, record: &Record) {
        let mut remove_value = false;
        if let Some(records) = self.left_records.get_mut(value) {
            if let Some(count) = records.get_mut(record) {
                *count -= 1;
                if *count == 0 {
                    records.remove(record);
                }
            }
            remove_value = records.is_empty();
        }
        if remove_value {
            self.left_records.remove(value);
        }
    }

    fn send_records(&self, value: &Field, insert: bool, fw: &mut dyn ProcessorChannelForwarder) {
        let Some(records) = self.left_records.get(value) else {
            return;
        };
        for (record, count) in records {
            for _ in 0..*count {
                let op = if insert {
                    Operation::Insert {
                        new: record.clone(),
                    }
                } else {
                    Operation::Delete {
                        old: record.clone(),
                    }
                };
                fw.send(TableOperation::without_id(op, DEFAULT_PORT_HANDLE));
            }
        }
    }

    fn send_all_uncontained(&self, insert: bool, fw: &mut dyn ProcessorChannelForwarder) {
        for value in self.left_records.keys() {
            if *value != Field::Null && !self.right_values.contains_key(value) {
                self.send_records(value, insert, fw);
            }
        }
    }

    fn process_left(
        &mut self,
        op: Operation,
        id: Option<dozer_types::node::OpIdentifier>,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        match op {
            Operation::Insert { new } => {
                let value = self.evaluate(&new)?;
                let matched = self.is_match(&value);
                self.remember_left(value, new.clone());
                if matched {
                    fw.send(TableOperation {
                        id,
                        op: Operation::Insert { new },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::Delete { old } => {
                let value = self.evaluate(&old)?;
                let matched = self.is_match(&value);
                self.forget_left(&value, &old);
                if matched {
                    fw.send(TableOperation {
                        id,
                        op: Operation::Delete { old },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::Update { old, new } => {
                let old_value = self.evaluate(&old)?;
                let new_value = self.evaluate(&new)?;
                let old_matched = self.is_match(&old_value);
                let new_matched = self.is_match(&new_value);
                self.forget_left(&old_value, &old);
                self.remember_left(new_value, new.clone());
                let op = match (old_matched, new_matched) {
                    (true, true) => Some(Operation::Update { old, new }),
                    (true, false) => Some(Operation::Delete { old }),
                    (false, true) => Some(Operation::Insert { new }),
                    (false, false) => None,
                };
                if let Some(op) = op {
                    fw.send(TableOperation {
                        id,
                        op,
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.process_left(Operation::Insert { new: record }, id, fw)?;
                }
            }
        }
        Ok(())
    }

    fn right_value(record: &Record) -> Result<&Field, PipelineError> {
        record.values.first().ok_or_else(|| {
            PipelineError::InvalidValue("IN subquery produced an empty record".to_string())
        })
    }

    fn insert_right(&mut self, value: Field, fw: &mut dyn ProcessorChannelForwarder) {
        if value == Field::Null {
            let was_empty = self.right_nulls == 0;
            self.right_nulls += 1;
            if self.negated && was_empty {
                self.send_all_uncontained(false, fw);
            }
            return;
        }

        let count = self.right_values.entry(value.clone()).or_default();
        let was_absent = *count == 0;
        *count += 1;
        if was_absent {
            if self.negated {
                if self.right_nulls == 0 {
                    self.send_records(&value, false, fw);
                }
            } else {
                self.send_records(&value, true, fw);
            }
        }
    }

    fn delete_right(&mut self, value: &Field, fw: &mut dyn ProcessorChannelForwarder) {
        if *value == Field::Null {
            if self.right_nulls == 0 {
                return;
            }
            self.right_nulls -= 1;
            if self.negated && self.right_nulls == 0 {
                self.send_all_uncontained(true, fw);
            }
            return;
        }

        let remove = if let Some(count) = self.right_values.get_mut(value) {
            *count -= 1;
            *count == 0
        } else {
            false
        };
        if remove {
            self.right_values.remove(value);
            if self.negated {
                if self.right_nulls == 0 {
                    self.send_records(value, true, fw);
                }
            } else {
                self.send_records(value, false, fw);
            }
        }
    }

    fn process_right(
        &mut self,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        match op {
            Operation::Insert { new } => self.insert_right(Self::right_value(&new)?.clone(), fw),
            Operation::Delete { old } => self.delete_right(Self::right_value(&old)?, fw),
            Operation::Update { old, new } => {
                let old = Self::right_value(&old)?.clone();
                let new = Self::right_value(&new)?.clone();
                if old != new {
                    self.delete_right(&old, fw);
                    self.insert_right(new, fw);
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.insert_right(Self::right_value(&record)?.clone(), fw);
                }
            }
        }
        Ok(())
    }
}

impl Processor for InSubqueryProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.port {
            IN_SUBQUERY_LEFT_PORT => self.process_left(op.op, op.id, fw)?,
            IN_SUBQUERY_RIGHT_PORT => self.process_right(op.op, fw)?,
            port => return Err(PipelineError::InvalidPortHandle(port).into()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dozer_core::{channels::ProcessorChannelForwarder, node::Processor};
    use dozer_sql_expression::execution::Expression;
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    };

    use super::{InSubqueryProcessor, IN_SUBQUERY_LEFT_PORT, IN_SUBQUERY_RIGHT_PORT};

    #[derive(Default)]
    struct TestForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn processor(negated: bool) -> InSubqueryProcessor {
        let schema = Schema::default()
            .field(
                FieldDefinition::new(
                    "value".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone();
        InSubqueryProcessor::new(schema, Expression::Column { index: 0 }, negated)
    }

    fn operation(port: u16, op: Operation) -> TableOperation {
        TableOperation::without_id(op, port)
    }

    fn record(value: Field) -> Record {
        Record::new(vec![value])
    }

    #[test]
    fn right_side_changes_update_in_results_once_per_distinct_value() {
        let mut processor = processor(false);
        let mut forwarder = TestForwarder::default();
        let left = record(Field::Int(7));
        let right = record(Field::Int(7));

        processor
            .process(
                operation(
                    IN_SUBQUERY_LEFT_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert!(forwarder.operations.is_empty());

        processor
            .process(
                operation(
                    IN_SUBQUERY_RIGHT_PORT,
                    Operation::Insert { new: right.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Insert { new: left.clone() }
        );

        processor
            .process(
                operation(
                    IN_SUBQUERY_RIGHT_PORT,
                    Operation::Insert { new: right.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                operation(
                    IN_SUBQUERY_RIGHT_PORT,
                    Operation::Delete { old: right.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert!(forwarder.operations.is_empty());

        processor
            .process(
                operation(IN_SUBQUERY_RIGHT_PORT, Operation::Delete { old: right }),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Delete { old: left }
        );
    }

    #[test]
    fn not_in_observes_right_side_nulls() {
        let mut processor = processor(true);
        let mut forwarder = TestForwarder::default();
        let left = record(Field::Int(7));
        let null = record(Field::Null);

        processor
            .process(
                operation(
                    IN_SUBQUERY_LEFT_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Insert { new: left.clone() }
        );

        processor
            .process(
                operation(
                    IN_SUBQUERY_RIGHT_PORT,
                    Operation::Insert { new: null.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Delete { old: left.clone() }
        );

        processor
            .process(
                operation(IN_SUBQUERY_RIGHT_PORT, Operation::Delete { old: null }),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Insert { new: left }
        );
    }

    #[test]
    fn left_updates_follow_selection_semantics() {
        let mut processor = processor(false);
        let mut forwarder = TestForwarder::default();
        processor
            .process(
                operation(
                    IN_SUBQUERY_RIGHT_PORT,
                    Operation::BatchInsert {
                        new: vec![record(Field::Int(1)), record(Field::Int(2))],
                    },
                ),
                &mut forwarder,
            )
            .unwrap();

        let one = record(Field::Int(1));
        let two = record(Field::Int(2));
        processor
            .process(
                operation(
                    IN_SUBQUERY_LEFT_PORT,
                    Operation::Insert { new: one.clone() },
                ),
                &mut forwarder,
            )
            .unwrap();
        forwarder.operations.clear();
        processor
            .process(
                operation(
                    IN_SUBQUERY_LEFT_PORT,
                    Operation::Update {
                        old: one.clone(),
                        new: two.clone(),
                    },
                ),
                &mut forwarder,
            )
            .unwrap();
        assert_eq!(
            forwarder.operations.pop().unwrap().op,
            Operation::Update { old: one, new: two }
        );
    }
}
