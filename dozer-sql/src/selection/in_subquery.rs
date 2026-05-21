use std::{collections::HashMap, sync::Arc};

use crate::errors::PipelineError;
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

pub(crate) const LEFT_IN_SUBQUERY_PORT: PortHandle = 0;
pub(crate) const RIGHT_IN_SUBQUERY_PORT: PortHandle = 1;

#[derive(Debug)]
pub struct InSubqueryProcessorFactory {
    id: String,
    left_expr: SqlExpr,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl InSubqueryProcessorFactory {
    pub fn new(
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
        input_schemas
            .get(&LEFT_IN_SUBQUERY_PORT)
            .cloned()
            .ok_or_else(|| PipelineError::InvalidPortHandle(LEFT_IN_SUBQUERY_PORT).into())
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
            .ok_or(PipelineError::InvalidPortHandle(RIGHT_IN_SUBQUERY_PORT))?
            .clone();

        if right_schema.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "IN subquery must return exactly one column".to_string(),
            )
            .into());
        }

        let left_expr = ExpressionBuilder::new(left_schema.fields.len(), self.runtime.clone())
            .build(false, &self.left_expr, &left_schema, &self.udfs)
            .await?;
        let right_expr = Expression::Column { index: 0 };

        Ok(Box::new(InSubqueryProcessor::new(
            left_schema,
            right_schema,
            left_expr,
            right_expr,
        )))
    }
}

#[derive(Debug)]
pub struct InSubqueryProcessor {
    left_schema: Schema,
    right_schema: Schema,
    left_expr: Expression,
    right_expr: Expression,
    left_records: HashMap<Field, Vec<Record>>,
    right_counts: HashMap<Field, usize>,
}

impl InSubqueryProcessor {
    fn new(
        left_schema: Schema,
        right_schema: Schema,
        left_expr: Expression,
        right_expr: Expression,
    ) -> Self {
        Self {
            left_schema,
            right_schema,
            left_expr,
            right_expr,
            left_records: HashMap::new(),
            right_counts: HashMap::new(),
        }
    }

    fn left_key(&mut self, record: &Record) -> Result<Field, PipelineError> {
        Ok(self.left_expr.evaluate(record, &self.left_schema)?)
    }

    fn right_key(&mut self, record: &Record) -> Result<Field, PipelineError> {
        Ok(self.right_expr.evaluate(record, &self.right_schema)?)
    }

    fn right_contains(&self, key: &Field) -> bool {
        self.right_counts.get(key).copied().unwrap_or_default() > 0
    }

    fn add_left_record(&mut self, key: Field, record: Record) {
        self.left_records.entry(key).or_default().push(record);
    }

    fn remove_left_record(&mut self, key: &Field, record: &Record) -> bool {
        let Some(records) = self.left_records.get_mut(key) else {
            return false;
        };

        let Some(index) = records.iter().position(|stored| stored == record) else {
            return false;
        };

        records.remove(index);
        let should_remove_key = records.is_empty();
        if should_remove_key {
            self.left_records.remove(key);
        }
        true
    }

    fn matching_left_records(&self, key: &Field) -> Vec<Record> {
        self.left_records.get(key).cloned().unwrap_or_default()
    }

    fn process_left_insert(
        &mut self,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&new)?;
        let matched = self.right_contains(&key);
        self.add_left_record(key, new.clone());
        if matched {
            fw.send(TableOperation::without_id(
                Operation::Insert { new },
                DEFAULT_PORT_HANDLE,
            ));
        }
        Ok(())
    }

    fn process_left_delete(
        &mut self,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.left_key(&old)?;
        let matched = self.right_contains(&key);
        let existed = self.remove_left_record(&key, &old);
        if existed && matched {
            fw.send(TableOperation::without_id(
                Operation::Delete { old },
                DEFAULT_PORT_HANDLE,
            ));
        }
        Ok(())
    }

    fn process_left_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let old_key = self.left_key(&old)?;
        let new_key = self.left_key(&new)?;
        let old_matched = self.right_contains(&old_key);
        let new_matched = self.right_contains(&new_key);
        let existed = self.remove_left_record(&old_key, &old);
        self.add_left_record(new_key, new.clone());

        if !existed {
            return Ok(());
        }

        match (old_matched, new_matched) {
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
        let key = self.right_key(&new)?;
        let count = self.right_counts.entry(key.clone()).or_default();
        let was_present = *count > 0;
        *count += 1;

        if !was_present {
            for record in self.matching_left_records(&key) {
                fw.send(TableOperation::without_id(
                    Operation::Insert { new: record },
                    DEFAULT_PORT_HANDLE,
                ));
            }
        }
        Ok(())
    }

    fn process_right_delete(
        &mut self,
        old: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let key = self.right_key(&old)?;
        let Some(count) = self.right_counts.get_mut(&key) else {
            return Ok(());
        };

        *count -= 1;
        if *count == 0 {
            self.right_counts.remove(&key);
            for record in self.matching_left_records(&key) {
                fw.send(TableOperation::without_id(
                    Operation::Delete { old: record },
                    DEFAULT_PORT_HANDLE,
                ));
            }
        }
        Ok(())
    }

    fn process_right_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        let old_key = self.right_key(&old)?;
        let new_key = self.right_key(&new)?;
        if old_key == new_key {
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
        match (op.port, op.op) {
            (LEFT_IN_SUBQUERY_PORT, Operation::Insert { new }) => {
                self.process_left_insert(new, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::Delete { old }) => {
                self.process_left_delete(old, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::Update { old, new }) => {
                self.process_left_update(old, new, fw)?
            }
            (LEFT_IN_SUBQUERY_PORT, Operation::BatchInsert { new }) => {
                for record in new {
                    self.process_left_insert(record, fw)?;
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
    use dozer_core::channels::ProcessorChannelForwarder;
    use dozer_core::node::Processor;
    use dozer_sql_expression::execution::Expression;
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    };

    use super::{InSubqueryProcessor, LEFT_IN_SUBQUERY_PORT, RIGHT_IN_SUBQUERY_PORT};

    struct TestForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn left_schema() -> Schema {
        Schema::default()
            .field(
                FieldDefinition::new(
                    "id".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "name".to_string(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone()
    }

    fn right_schema() -> Schema {
        Schema::default()
            .field(
                FieldDefinition::new(
                    "id".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone()
    }

    fn left_record(id: i64, name: &str) -> Record {
        Record::new(vec![Field::Int(id), Field::String(name.to_string())])
    }

    fn right_record(id: i64) -> Record {
        Record::new(vec![Field::Int(id)])
    }

    #[test]
    fn streams_left_rows_when_right_membership_changes() {
        let mut processor = InSubqueryProcessor::new(
            left_schema(),
            right_schema(),
            Expression::Column { index: 0 },
            Expression::Column { index: 0 },
        );
        let mut forwarder = TestForwarder { operations: vec![] };

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: right_record(1),
                    },
                    RIGHT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: left_record(1, "penelope"),
                    },
                    LEFT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: left_record(2, "jack"),
                    },
                    LEFT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: right_record(2),
                    },
                    RIGHT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation::without_id(
                    Operation::Update {
                        old: left_record(1, "penelope"),
                        new: left_record(3, "penelope"),
                    },
                    LEFT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();
        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: right_record(3),
                    },
                    RIGHT_IN_SUBQUERY_PORT,
                ),
                &mut forwarder,
            )
            .unwrap();

        let operations = forwarder
            .operations
            .into_iter()
            .map(|op| op.op)
            .collect::<Vec<_>>();
        assert_eq!(
            operations,
            vec![
                Operation::Insert {
                    new: left_record(1, "penelope")
                },
                Operation::Insert {
                    new: left_record(2, "jack")
                },
                Operation::Delete {
                    old: left_record(1, "penelope")
                },
                Operation::Insert {
                    new: left_record(3, "penelope")
                },
            ]
        );
    }
}
