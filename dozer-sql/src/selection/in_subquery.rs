use std::collections::HashMap;
use std::sync::Arc;

use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::event::EventHub;
use dozer_core::node::{PortHandle, Processor, ProcessorFactory};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql_expression::builder::ExpressionBuilder;
use dozer_sql_expression::execution::Expression;
use dozer_sql_expression::sqlparser::ast::Expr as SqlExpr;
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::tonic::async_trait;
use dozer_types::types::{Field, Operation, Record, Schema, TableOperation};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;

pub(crate) const LEFT_IN_SUBQUERY_PORT: PortHandle = 0;
pub(crate) const RIGHT_IN_SUBQUERY_PORT: PortHandle = 1;

#[derive(Debug)]
pub(crate) struct InSubqueryProcessorFactory {
    id: String,
    left_expr: SqlExpr,
    negated: bool,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl InSubqueryProcessorFactory {
    pub(crate) fn new(
        id: String,
        left_expr: SqlExpr,
        negated: bool,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            left_expr,
            negated,
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

        let left_expr = ExpressionBuilder::new(left_schema.fields.len(), self.runtime.clone())
            .build(false, &self.left_expr, &left_schema, &self.udfs)
            .await?;

        Ok(Box::new(InSubqueryProcessor::new(
            left_schema,
            left_expr,
            self.negated,
        )))
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
pub(crate) struct InSubqueryProcessor {
    left_schema: Schema,
    left_expr: Expression,
    negated: bool,
    left_records: HashMap<Field, Vec<Record>>,
    right_counts: HashMap<Field, usize>,
}

impl InSubqueryProcessor {
    fn new(left_schema: Schema, left_expr: Expression, negated: bool) -> Self {
        Self {
            left_schema,
            left_expr,
            negated,
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

    fn key_is_visible(&self, key: &Field) -> bool {
        if matches!(key, Field::Null) {
            return false;
        }
        let matched = self.right_counts.get(key).copied().unwrap_or(0) > 0;
        if self.negated {
            !matched
        } else {
            matched
        }
    }

    fn store_left(&mut self, key: Field, record: Record) {
        self.left_records.entry(key).or_default().push(record);
    }

    fn remove_left(&mut self, key: &Field, record: &Record) {
        let Some(records) = self.left_records.get_mut(key) else {
            return;
        };
        if let Some(index) = records.iter().position(|item| item == record) {
            records.remove(index);
        }
        if records.is_empty() {
            self.left_records.remove(key);
        }
    }

    fn send_left_records(
        &self,
        key: &Field,
        action: JoinVisibilityAction,
        fw: &mut dyn ProcessorChannelForwarder,
    ) {
        if matches!(key, Field::Null) {
            return;
        }
        let Some(records) = self.left_records.get(key) else {
            return;
        };
        for record in records {
            let op = match action {
                JoinVisibilityAction::Insert => Operation::Insert {
                    new: record.clone(),
                },
                JoinVisibilityAction::Delete => Operation::Delete {
                    old: record.clone(),
                },
            };
            fw.send(TableOperation::without_id(op, DEFAULT_PORT_HANDLE));
        }
    }

    fn process_left_insert(
        &mut self,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&new)?;
        if self.key_is_visible(&key) {
            fw.send(TableOperation::without_id(
                Operation::Insert { new: new.clone() },
                DEFAULT_PORT_HANDLE,
            ));
        }
        self.store_left(key, new);
        Ok(())
    }

    fn process_left_delete(
        &mut self,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&old)?;
        if self.key_is_visible(&key) {
            fw.send(TableOperation::without_id(
                Operation::Delete { old: old.clone() },
                DEFAULT_PORT_HANDLE,
            ));
        }
        self.remove_left(&key, &old);
        Ok(())
    }

    fn process_left_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let old_key = self.left_key(&old)?;
        let old_visible = self.key_is_visible(&old_key);
        self.remove_left(&old_key, &old);

        let new_key = self.left_key(&new)?;
        let new_visible = self.key_is_visible(&new_key);
        self.store_left(new_key, new.clone());

        match (old_visible, new_visible) {
            (true, true) => fw.send(TableOperation::without_id(
                Operation::Update { old, new },
                DEFAULT_PORT_HANDLE,
            )),
            (true, false) => fw.send(TableOperation::without_id(
                Operation::Delete { old },
                DEFAULT_PORT_HANDLE,
            )),
            (false, true) => fw.send(TableOperation::without_id(
                Operation::Insert { new },
                DEFAULT_PORT_HANDLE,
            )),
            (false, false) => {}
        }

        Ok(())
    }

    fn process_right_insert(
        &mut self,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = Self::right_key(&new)?;
        if matches!(key, Field::Null) {
            return Ok(());
        }
        let count = self.right_counts.entry(key.clone()).or_default();
        let was_empty = *count == 0;
        *count += 1;

        if was_empty {
            let action = if self.negated {
                JoinVisibilityAction::Delete
            } else {
                JoinVisibilityAction::Insert
            };
            self.send_left_records(&key, action, fw);
        }

        Ok(())
    }

    fn process_right_delete(
        &mut self,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = Self::right_key(&old)?;
        if matches!(key, Field::Null) {
            return Ok(());
        }
        let became_empty = if let Some(count) = self.right_counts.get_mut(&key) {
            *count -= 1;
            *count == 0
        } else {
            false
        };

        if became_empty {
            self.right_counts.remove(&key);
            let action = if self.negated {
                JoinVisibilityAction::Insert
            } else {
                JoinVisibilityAction::Delete
            };
            self.send_left_records(&key, action, fw);
        }

        Ok(())
    }

    fn process_right_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        if Self::right_key(&old)? == Self::right_key(&new)? {
            return Ok(());
        }

        self.process_right_delete(old, fw)?;
        self.process_right_insert(new, fw)
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
            LEFT_IN_SUBQUERY_PORT => match op.op {
                Operation::Insert { new } => self.process_left_insert(new, fw)?,
                Operation::Delete { old } => self.process_left_delete(old, fw)?,
                Operation::Update { old, new } => self.process_left_update(old, new, fw)?,
                Operation::BatchInsert { new } => {
                    for record in new {
                        self.process_left_insert(record, fw)?;
                    }
                }
            },
            RIGHT_IN_SUBQUERY_PORT => match op.op {
                Operation::Insert { new } => self.process_right_insert(new, fw)?,
                Operation::Delete { old } => self.process_right_delete(old, fw)?,
                Operation::Update { old, new } => self.process_right_update(old, new, fw)?,
                Operation::BatchInsert { new } => {
                    for record in new {
                        self.process_right_insert(record, fw)?;
                    }
                }
            },
            _ => return Err(PipelineError::InvalidPortHandle(op.port).into()),
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum JoinVisibilityAction {
    Insert,
    Delete,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dozer_core::channels::ProcessorChannelForwarder;
    use dozer_core::event::EventHub;
    use dozer_core::node::{Processor, ProcessorFactory};
    use dozer_sql_expression::sqlparser::ast::{Expr as SqlExpr, Ident};
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    };

    use crate::tests::utils::create_test_runtime;

    use super::{InSubqueryProcessorFactory, LEFT_IN_SUBQUERY_PORT, RIGHT_IN_SUBQUERY_PORT};

    #[derive(Default)]
    struct TestForwarder {
        operations: Vec<Operation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op.op);
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

    fn right_record(city: &str) -> Record {
        Record::new(vec![Field::String(city.to_string())])
    }

    fn build_processor(negated: bool) -> Box<dyn Processor> {
        let runtime = create_test_runtime();
        let factory = InSubqueryProcessorFactory::new(
            "in_subquery".to_string(),
            SqlExpr::Identifier(Ident::new("city")),
            negated,
            vec![],
            runtime.clone(),
        );
        let schemas = [
            (
                LEFT_IN_SUBQUERY_PORT,
                schema(&[("city", FieldType::String), ("id", FieldType::Int)]),
            ),
            (
                RIGHT_IN_SUBQUERY_PORT,
                schema(&[("city", FieldType::String)]),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        runtime
            .block_on(factory.build(schemas, HashMap::new(), EventHub::new(1)))
            .unwrap()
    }

    fn apply(
        processor: &mut Box<dyn Processor>,
        port: u16,
        op: Operation,
        forwarder: &mut TestForwarder,
    ) -> Vec<Operation> {
        processor
            .process(TableOperation::without_id(op, port), forwarder)
            .unwrap();
        let operations = forwarder.operations.clone();
        forwarder.operations.clear();
        operations
    }

    #[test]
    fn emits_left_records_when_right_key_appears_and_disappears() {
        let mut processor = build_processor(false);
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
    fn not_in_inverts_visibility_when_right_key_changes() {
        let mut processor = build_processor(true);
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
            vec![Operation::Delete {
                old: paris_order.clone()
            }]
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
            vec![Operation::Insert { new: paris_order }]
        );
    }

    #[test]
    fn left_updates_follow_where_transition_rules() {
        let mut processor = build_processor(false);
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
}
