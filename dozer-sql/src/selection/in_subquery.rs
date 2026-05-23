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
    node::OpIdentifier,
    tonic::async_trait,
    types::{Field, Operation, Record, Schema, TableOperation},
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

pub(crate) const LEFT_IN_SUBQUERY_PORT: PortHandle = 0;
pub(crate) const RIGHT_IN_SUBQUERY_PORT: PortHandle = 1;

#[derive(Debug)]
pub(crate) struct InSubqueryProcessorFactory {
    id: String,
    left_expr: SqlExpr,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl InSubqueryProcessorFactory {
    pub(crate) fn new(
        id: String,
        left_expr: SqlExpr,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            left_expr,
            udfs,
            runtime,
        }
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
        vec![LEFT_IN_SUBQUERY_PORT, RIGHT_IN_SUBQUERY_PORT]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let left_schema = input_schemas
            .get(&LEFT_IN_SUBQUERY_PORT)
            .ok_or(PipelineError::InvalidPortHandle(LEFT_IN_SUBQUERY_PORT))?;
        let right_schema = input_schemas
            .get(&RIGHT_IN_SUBQUERY_PORT)
            .ok_or(PipelineError::InvalidPortHandle(RIGHT_IN_SUBQUERY_PORT))?;

        validate_right_schema(right_schema)?;
        Ok(left_schema.clone())
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let left_schema = input_schemas
            .get(&LEFT_IN_SUBQUERY_PORT)
            .ok_or(PipelineError::InvalidPortHandle(LEFT_IN_SUBQUERY_PORT))?
            .clone();
        let right_schema = input_schemas
            .get(&RIGHT_IN_SUBQUERY_PORT)
            .ok_or(PipelineError::InvalidPortHandle(RIGHT_IN_SUBQUERY_PORT))?;

        validate_right_schema(right_schema)?;

        let left_expr = match ExpressionBuilder::new(left_schema.fields.len(), self.runtime.clone())
            .build(false, &self.left_expr, &left_schema, &self.udfs)
            .await
        {
            Ok(expression) => expression,
            Err(error) => return Err(error.into()),
        };

        Ok(Box::new(InSubqueryProcessor::new(left_schema, left_expr)))
    }
}

fn validate_right_schema(schema: &Schema) -> Result<(), PipelineError> {
    if schema.fields.len() == 1 {
        Ok(())
    } else {
        Err(PipelineError::InvalidQuery(
            "IN subquery must return exactly one column".to_string(),
        ))
    }
}

#[derive(Debug)]
struct InSubqueryProcessor {
    left_schema: Schema,
    left_expr: Expression,
    left_records: HashMap<Field, Vec<Record>>,
    right_counts: HashMap<Field, usize>,
}

impl InSubqueryProcessor {
    fn new(left_schema: Schema, left_expr: Expression) -> Self {
        Self {
            left_schema,
            left_expr,
            left_records: HashMap::new(),
            right_counts: HashMap::new(),
        }
    }

    fn left_key(&mut self, record: &Record) -> Result<Field, PipelineError> {
        Ok(self.left_expr.evaluate(record, &self.left_schema)?)
    }

    fn right_key(record: &Record) -> Result<Field, PipelineError> {
        record.values.first().cloned().ok_or_else(|| {
            PipelineError::InvalidQuery("IN subquery record has no value".to_string())
        })
    }

    fn is_matchable_key(key: &Field) -> bool {
        !matches!(key, Field::Null)
    }

    fn right_contains(&self, key: &Field) -> bool {
        Self::is_matchable_key(key) && self.right_counts.get(key).copied().unwrap_or_default() > 0
    }

    fn store_left(&mut self, key: Field, record: Record) {
        self.left_records.entry(key).or_default().push(record);
    }

    fn remove_left(&mut self, key: &Field, record: &Record) {
        let Some(records) = self.left_records.get_mut(key) else {
            return;
        };

        if let Some(index) = records.iter().position(|stored| stored == record) {
            records.remove(index);
        }

        if records.is_empty() {
            self.left_records.remove(key);
        }
    }

    fn send(id: Option<OpIdentifier>, op: Operation, fw: &mut dyn ProcessorChannelForwarder) {
        fw.send(TableOperation {
            id,
            op,
            port: DEFAULT_PORT_HANDLE,
        });
    }

    fn send_left_records(
        &self,
        key: &Field,
        visible: bool,
        fw: &mut dyn ProcessorChannelForwarder,
    ) {
        let Some(records) = self.left_records.get(key) else {
            return;
        };

        for record in records {
            let op = if visible {
                Operation::Insert {
                    new: record.clone(),
                }
            } else {
                Operation::Delete {
                    old: record.clone(),
                }
            };
            Self::send(None, op, fw);
        }
    }

    fn process_left_insert(
        &mut self,
        id: Option<OpIdentifier>,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&new)?;
        if self.right_contains(&key) {
            Self::send(id, Operation::Insert { new: new.clone() }, fw);
        }
        self.store_left(key, new);
        Ok(())
    }

    fn process_left_delete(
        &mut self,
        id: Option<OpIdentifier>,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&old)?;
        if self.right_contains(&key) {
            Self::send(id, Operation::Delete { old: old.clone() }, fw);
        }
        self.remove_left(&key, &old);
        Ok(())
    }

    fn process_left_update(
        &mut self,
        id: Option<OpIdentifier>,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let old_key = self.left_key(&old)?;
        let old_visible = self.right_contains(&old_key);
        self.remove_left(&old_key, &old);

        let new_key = self.left_key(&new)?;
        let new_visible = self.right_contains(&new_key);
        self.store_left(new_key, new.clone());

        match (old_visible, new_visible) {
            (true, true) => Self::send(id, Operation::Update { old, new }, fw),
            (true, false) => Self::send(id, Operation::Delete { old }, fw),
            (false, true) => Self::send(id, Operation::Insert { new }, fw),
            (false, false) => {}
        }
        Ok(())
    }

    fn add_right_key(&mut self, key: Field) -> bool {
        if !Self::is_matchable_key(&key) {
            return false;
        }

        let count = self.right_counts.entry(key).or_default();
        let became_present = *count == 0;
        *count += 1;
        became_present
    }

    fn remove_right_key(&mut self, key: &Field) -> bool {
        if !Self::is_matchable_key(key) {
            return false;
        }

        let Some(count) = self.right_counts.get_mut(key) else {
            return false;
        };

        if *count == 0 {
            return false;
        }

        *count -= 1;
        if *count == 0 {
            self.right_counts.remove(key);
            true
        } else {
            false
        }
    }

    fn process_right_insert(
        &mut self,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = Self::right_key(&new)?;
        if self.add_right_key(key.clone()) {
            self.send_left_records(&key, true, fw);
        }
        Ok(())
    }

    fn process_right_delete(
        &mut self,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = Self::right_key(&old)?;
        if self.remove_right_key(&key) {
            self.send_left_records(&key, false, fw);
        }
        Ok(())
    }

    fn process_right_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let old_key = Self::right_key(&old)?;
        let new_key = Self::right_key(&new)?;
        if old_key == new_key {
            return Ok(());
        }

        if self.remove_right_key(&old_key) {
            self.send_left_records(&old_key, false, fw);
        }
        if self.add_right_key(new_key.clone()) {
            self.send_left_records(&new_key, true, fw);
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
        let id = op.id;
        match (op.port, op.op) {
            (LEFT_IN_SUBQUERY_PORT, Operation::Insert { new }) => {
                self.process_left_insert(id, new, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::Delete { old }) => {
                self.process_left_delete(id, old, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::Update { old, new }) => {
                self.process_left_update(id, old, new, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::BatchInsert { new }) => {
                for record in new {
                    self.process_left_insert(id, record, fw)?;
                }
            }
            (RIGHT_IN_SUBQUERY_PORT, Operation::Insert { new }) => {
                self.process_right_insert(new, fw)?
            }
            (RIGHT_IN_SUBQUERY_PORT, Operation::Delete { old }) => {
                self.process_right_delete(old, fw)?
            }
            (RIGHT_IN_SUBQUERY_PORT, Operation::Update { old, new }) => {
                self.process_right_update(old, new, fw)?
            }
            (RIGHT_IN_SUBQUERY_PORT, Operation::BatchInsert { new }) => {
                for record in new {
                    self.process_right_insert(record, fw)?;
                }
            }
            (port, _) => return Err(PipelineError::InvalidPortHandle(port).into()),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dozer_core::{channels::ProcessorChannelForwarder, node::Processor};
    use dozer_sql_expression::execution::Expression;
    use dozer_types::{
        node::OpIdentifier,
        types::{
            Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
            TableOperation,
        },
    };

    use super::{InSubqueryProcessor, LEFT_IN_SUBQUERY_PORT, RIGHT_IN_SUBQUERY_PORT};

    #[derive(Default)]
    struct TestForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn schema(fields: &[(&str, FieldType)]) -> Schema {
        let mut schema = Schema::default();
        for (name, typ) in fields {
            schema.field(
                FieldDefinition::new((*name).to_string(), *typ, false, SourceDefinition::Dynamic),
                false,
            );
        }
        schema
    }

    fn left_record(city: &str, id: i64) -> Record {
        Record::new(vec![Field::String(city.to_string()), Field::Int(id)])
    }

    fn left_null_record(id: i64) -> Record {
        Record::new(vec![Field::Null, Field::Int(id)])
    }

    fn right_record(city: &str) -> Record {
        Record::new(vec![Field::String(city.to_string())])
    }

    fn right_null_record() -> Record {
        Record::new(vec![Field::Null])
    }

    fn processor() -> InSubqueryProcessor {
        InSubqueryProcessor::new(
            schema(&[("city", FieldType::String), ("id", FieldType::Int)]),
            Expression::Column { index: 0 },
        )
    }

    fn apply(
        processor: &mut InSubqueryProcessor,
        port: u16,
        op: Operation,
        forwarder: &mut TestForwarder,
    ) -> Vec<Operation> {
        processor
            .process(TableOperation::without_id(op, port), forwarder)
            .unwrap();
        let operations = forwarder
            .operations
            .drain(..)
            .map(|operation| operation.op)
            .collect::<Vec<_>>();
        operations
    }

    #[test]
    fn right_membership_changes_emit_existing_left_rows_once() {
        let mut processor = processor();
        let mut forwarder = TestForwarder::default();
        let paris_order = left_record("Paris", 1);

        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: paris_order.clone()
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: right_record("Paris")
                },
                &mut forwarder,
            ),
            vec![Operation::Insert {
                new: paris_order.clone()
            }]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: right_record("Paris")
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Delete {
                    old: right_record("Paris")
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Delete {
                    old: right_record("Paris")
                },
                &mut forwarder,
            ),
            vec![Operation::Delete { old: paris_order }]
        );
    }

    #[test]
    fn right_updates_toggle_existing_left_rows() {
        let mut processor = processor();
        let mut forwarder = TestForwarder::default();
        let paris_order = left_record("Paris", 1);

        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: paris_order.clone()
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: right_record("London")
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Update {
                    old: right_record("London"),
                    new: right_record("Paris"),
                },
                &mut forwarder,
            ),
            vec![Operation::Insert {
                new: paris_order.clone()
            }]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Update {
                    old: right_record("Paris"),
                    new: right_record("Berlin"),
                },
                &mut forwarder,
            ),
            vec![Operation::Delete { old: paris_order }]
        );
    }

    #[test]
    fn left_updates_follow_where_transition_rules() {
        let mut processor = processor();
        let mut forwarder = TestForwarder::default();
        let london_order = left_record("London", 1);
        let paris_order = left_record("Paris", 1);
        let paris_order_updated = left_record("Paris", 2);
        let berlin_order = left_record("Berlin", 2);

        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: right_record("Paris")
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: london_order.clone()
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Update {
                    old: london_order,
                    new: paris_order.clone(),
                },
                &mut forwarder,
            ),
            vec![Operation::Insert {
                new: paris_order.clone()
            }]
        );
        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Update {
                    old: paris_order.clone(),
                    new: paris_order_updated.clone(),
                },
                &mut forwarder,
            ),
            vec![Operation::Update {
                old: paris_order,
                new: paris_order_updated.clone(),
            }]
        );
        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Update {
                    old: paris_order_updated,
                    new: berlin_order,
                },
                &mut forwarder,
            ),
            vec![Operation::Delete {
                old: left_record("Paris", 2)
            }]
        );
    }

    #[test]
    fn null_keys_never_match() {
        let mut processor = processor();
        let mut forwarder = TestForwarder::default();
        let null_order = left_null_record(1);

        assert_eq!(
            apply(
                &mut processor,
                LEFT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: null_order.clone()
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Insert {
                    new: right_null_record()
                },
                &mut forwarder,
            ),
            vec![]
        );
        assert_eq!(
            apply(
                &mut processor,
                RIGHT_IN_SUBQUERY_PORT,
                Operation::Delete {
                    old: right_null_record()
                },
                &mut forwarder,
            ),
            vec![]
        );
    }

    #[test]
    fn left_side_operations_preserve_operation_ids() {
        let mut processor = processor();
        let mut forwarder = TestForwarder::default();
        let id = OpIdentifier::new(7, 11);

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: right_record("Paris"),
                    },
                    RIGHT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation {
                    id: Some(id),
                    op: Operation::Insert {
                        new: left_record("Paris", 1),
                    },
                    port: LEFT_IN_SUBQUERY_PORT,
                },
                &mut forwarder,
            )
            .unwrap();

        assert_eq!(forwarder.operations.len(), 1);
        assert_eq!(forwarder.operations[0].id, Some(id));
    }
}
