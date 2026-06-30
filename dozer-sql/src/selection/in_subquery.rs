use std::collections::{hash_map, HashMap};
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
use crate::utils::record_hashtable_key::get_record_hash;

pub(crate) const LEFT_IN_SUBQUERY_PORT: PortHandle = 0;
pub(crate) const RIGHT_IN_SUBQUERY_PORT: PortHandle = 1;

#[derive(Debug)]
pub struct InSubqueryProcessorFactory {
    id: String,
    expr: SqlExpr,
    negated: bool,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
}

impl InSubqueryProcessorFactory {
    pub fn new(
        id: String,
        expr: SqlExpr,
        negated: bool,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            expr,
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
        validate_schemas(input_schemas).map(|(left, _)| left.clone())
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let (left_schema, right_schema) = validate_schemas(&input_schemas)?;
        let expr = ExpressionBuilder::new(left_schema.fields.len(), self.runtime.clone())
            .build(false, &self.expr, left_schema, &self.udfs)
            .await?;

        Ok(Box::new(InSubqueryProcessor::new(
            left_schema.clone(),
            right_schema.clone(),
            expr,
            self.negated,
        )?))
    }
}

fn validate_schemas(
    input_schemas: &HashMap<PortHandle, Schema>,
) -> Result<(&Schema, &Schema), BoxedError> {
    let left_schema = input_schemas
        .get(&LEFT_IN_SUBQUERY_PORT)
        .ok_or(PipelineError::InvalidPortHandle(LEFT_IN_SUBQUERY_PORT))?;
    let right_schema = input_schemas
        .get(&RIGHT_IN_SUBQUERY_PORT)
        .ok_or(PipelineError::InvalidPortHandle(RIGHT_IN_SUBQUERY_PORT))?;

    if right_schema.fields.len() != 1 {
        return Err(PipelineError::InvalidQuery(
            "IN subquery must return exactly one column".to_string(),
        )
        .into());
    }

    Ok((left_schema, right_schema))
}

#[derive(Debug)]
pub struct InSubqueryProcessor {
    left_schema: Schema,
    _right_schema: Schema,
    expr: Expression,
    negated: bool,
    left_rows: HashMap<Field, HashMap<u64, Vec<Record>>>,
    right_counts: HashMap<Field, usize>,
    right_null_count: usize,
}

impl InSubqueryProcessor {
    pub fn new(
        left_schema: Schema,
        right_schema: Schema,
        expr: Expression,
        negated: bool,
    ) -> Result<Self, PipelineError> {
        if right_schema.fields.len() != 1 {
            return Err(PipelineError::InvalidQuery(
                "IN subquery must return exactly one column".to_string(),
            ));
        }

        Ok(Self {
            left_schema,
            _right_schema: right_schema,
            expr,
            negated,
            left_rows: HashMap::new(),
            right_counts: HashMap::new(),
            right_null_count: 0,
        })
    }

    fn left_key(&mut self, record: &Record) -> Result<Option<Field>, PipelineError> {
        let key = self.expr.evaluate(record, &self.left_schema)?;
        Ok((key != Field::Null).then_some(key))
    }

    fn right_key(record: &Record) -> Result<Option<Field>, PipelineError> {
        let key = record.values.first().cloned().ok_or_else(|| {
            PipelineError::InvalidQuery("IN subquery record has no value".to_string())
        })?;
        Ok((key != Field::Null).then_some(key))
    }

    fn left_primary_key(&self, record: &Record) -> u64 {
        if self.left_schema.primary_index.is_empty() {
            get_record_hash(record.values.iter())
        } else {
            get_record_hash(
                self.left_schema
                    .primary_index
                    .iter()
                    .map(|idx| &record.values[*idx]),
            )
        }
    }

    fn right_has_key(&self, key: &Field) -> bool {
        self.right_counts.get(key).copied().unwrap_or_default() > 0
    }

    fn left_visible(&self, key: &Field) -> bool {
        if self.negated {
            self.right_null_count == 0 && !self.right_has_key(key)
        } else {
            self.right_has_key(key)
        }
    }

    fn insert_left(&mut self, key: Field, record: Record) {
        let primary_key = self.left_primary_key(&record);
        self.left_rows
            .entry(key)
            .or_default()
            .entry(primary_key)
            .or_default()
            .push(record);
    }

    fn remove_left(&mut self, key: &Field, record: &Record) {
        let primary_key = self.left_primary_key(record);
        if let hash_map::Entry::Occupied(mut by_key) = self.left_rows.entry(key.clone()) {
            if let hash_map::Entry::Occupied(mut records) = by_key.get_mut().entry(primary_key) {
                if let Some(pos) = records
                    .get()
                    .iter()
                    .rposition(|candidate| candidate == record)
                {
                    records.get_mut().remove(pos);
                }
                if records.get().is_empty() {
                    records.remove();
                }
            }
            if by_key.get().is_empty() {
                by_key.remove();
            }
        }
    }

    fn matching_left_rows(&self, key: &Field) -> Vec<Record> {
        self.left_rows
            .get(key)
            .into_iter()
            .flat_map(|records| records.values())
            .flatten()
            .cloned()
            .collect()
    }

    fn not_in_visible_left_rows(&self) -> Vec<Record> {
        self.left_rows
            .iter()
            .filter(|(key, _)| !self.right_has_key(key))
            .flat_map(|(_, records)| records.values())
            .flatten()
            .cloned()
            .collect()
    }

    fn send_records(records: Vec<Record>, visible: bool, fw: &mut dyn ProcessorChannelForwarder) {
        if records.is_empty() {
            return;
        }

        if visible {
            fw.send(TableOperation::without_id(
                Operation::BatchInsert { new: records },
                DEFAULT_PORT_HANDLE,
            ));
        } else {
            for old in records {
                fw.send(TableOperation::without_id(
                    Operation::Delete { old },
                    DEFAULT_PORT_HANDLE,
                ));
            }
        }
    }

    fn process_left(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { old } => {
                if let Some(key) = self.left_key(&old)? {
                    let visible = self.left_visible(&key);
                    self.remove_left(&key, &old);
                    if visible {
                        fw.send(TableOperation {
                            id: op.id,
                            op: Operation::Delete { old },
                            port: DEFAULT_PORT_HANDLE,
                        });
                    }
                }
            }
            Operation::Insert { new } => {
                if let Some(key) = self.left_key(&new)? {
                    let visible = self.left_visible(&key);
                    self.insert_left(key, new.clone());
                    if visible {
                        fw.send(TableOperation {
                            id: op.id,
                            op: Operation::Insert { new },
                            port: DEFAULT_PORT_HANDLE,
                        });
                    }
                }
            }
            Operation::Update { old, new } => {
                let old_key = self.left_key(&old)?;
                let new_key = self.left_key(&new)?;
                let old_visible = old_key
                    .as_ref()
                    .map(|key| self.left_visible(key))
                    .unwrap_or(false);
                let new_visible = new_key
                    .as_ref()
                    .map(|key| self.left_visible(key))
                    .unwrap_or(false);

                if let Some(key) = &old_key {
                    self.remove_left(key, &old);
                }
                if let Some(key) = new_key {
                    self.insert_left(key, new.clone());
                }

                let output = match (old_visible, new_visible) {
                    (true, true) => Some(Operation::Update { old, new }),
                    (true, false) => Some(Operation::Delete { old }),
                    (false, true) => Some(Operation::Insert { new }),
                    (false, false) => None,
                };
                if let Some(output_op) = output {
                    fw.send(TableOperation {
                        id: op.id,
                        op: output_op,
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::BatchInsert { new } => {
                let mut visible_records = vec![];
                for record in new {
                    if let Some(key) = self.left_key(&record)? {
                        let visible = self.left_visible(&key);
                        self.insert_left(key, record.clone());
                        if visible {
                            visible_records.push(record);
                        }
                    }
                }
                if !visible_records.is_empty() {
                    fw.send(TableOperation {
                        id: op.id,
                        op: Operation::BatchInsert {
                            new: visible_records,
                        },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
        }

        Ok(())
    }

    fn insert_right_key(&mut self, key: Field, fw: &mut dyn ProcessorChannelForwarder) {
        let count = self.right_counts.entry(key.clone()).or_default();
        *count += 1;
        if *count != 1 {
            return;
        }

        if self.negated {
            if self.right_null_count == 0 {
                Self::send_records(self.matching_left_rows(&key), false, fw);
            }
        } else {
            Self::send_records(self.matching_left_rows(&key), true, fw);
        }
    }

    fn delete_right_key(&mut self, key: Field, fw: &mut dyn ProcessorChannelForwarder) {
        let mut became_absent = false;
        if let hash_map::Entry::Occupied(mut count) = self.right_counts.entry(key.clone()) {
            if *count.get() == 0 {
                return;
            }
            *count.get_mut() -= 1;
            if *count.get() == 0 {
                count.remove();
                became_absent = true;
            }
        }

        if !became_absent {
            return;
        }

        if self.negated {
            if self.right_null_count == 0 {
                Self::send_records(self.matching_left_rows(&key), true, fw);
            }
        } else {
            Self::send_records(self.matching_left_rows(&key), false, fw);
        }
    }

    fn insert_right_null(&mut self, fw: &mut dyn ProcessorChannelForwarder) {
        if !self.negated {
            return;
        }

        self.right_null_count += 1;
        if self.right_null_count == 1 {
            Self::send_records(self.not_in_visible_left_rows(), false, fw);
        }
    }

    fn delete_right_null(&mut self, fw: &mut dyn ProcessorChannelForwarder) {
        if !self.negated {
            return;
        }

        if self.right_null_count == 0 {
            return;
        }

        self.right_null_count -= 1;
        if self.right_null_count == 0 {
            Self::send_records(self.not_in_visible_left_rows(), true, fw);
        }
    }

    fn insert_right(
        &mut self,
        record: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        match Self::right_key(&record)? {
            Some(key) => self.insert_right_key(key, fw),
            None => self.insert_right_null(fw),
        }
        Ok(())
    }

    fn delete_right(
        &mut self,
        record: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        match Self::right_key(&record)? {
            Some(key) => self.delete_right_key(key, fw),
            None => self.delete_right_null(fw),
        }
        Ok(())
    }

    fn process_right(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), PipelineError> {
        match op.op {
            Operation::Delete { old } => {
                self.delete_right(old, fw)?;
            }
            Operation::Insert { new } => {
                self.insert_right(new, fw)?;
            }
            Operation::Update { old, new } => {
                let old_key = Self::right_key(&old)?;
                let new_key = Self::right_key(&new)?;
                if old_key != new_key {
                    self.delete_right(old, fw)?;
                    self.insert_right(new, fw)?;
                }
            }
            Operation::BatchInsert { new } => {
                for record in new {
                    self.insert_right(record, fw)?;
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
            LEFT_IN_SUBQUERY_PORT => self.process_left(op, fw),
            RIGHT_IN_SUBQUERY_PORT => {
                self.process_right(op, fw)?;
                Ok(())
            }
            port => Err(PipelineError::InvalidPortHandle(port).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use dozer_core::channels::ProcessorChannelForwarder;
    use dozer_types::types::{FieldDefinition, FieldType, SourceDefinition};

    use super::*;

    struct TestForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn schema(fields: &[&str], primary_index: Vec<usize>) -> Schema {
        Schema {
            fields: fields
                .iter()
                .map(|name| {
                    FieldDefinition::new(
                        (*name).to_string(),
                        FieldType::Int,
                        false,
                        SourceDefinition::Dynamic,
                    )
                })
                .collect(),
            primary_index,
        }
    }

    fn processor() -> InSubqueryProcessor {
        InSubqueryProcessor::new(
            schema(&["id", "group_id"], vec![0]),
            schema(&["group_id"], vec![0]),
            Expression::Column { index: 1 },
            false,
        )
        .unwrap()
    }

    fn not_in_processor() -> InSubqueryProcessor {
        InSubqueryProcessor::new(
            schema(&["id", "group_id"], vec![0]),
            schema(&["group_id"], vec![0]),
            Expression::Column { index: 1 },
            true,
        )
        .unwrap()
    }

    fn record(values: &[i64]) -> Record {
        Record::new(values.iter().map(|value| Field::Int(*value)).collect())
    }

    fn op(port: PortHandle, op: Operation) -> TableOperation {
        TableOperation::without_id(op, port)
    }

    fn null_group_record(id: i64) -> Record {
        Record::new(vec![Field::Int(id), Field::Null])
    }

    fn right_null_record() -> Record {
        Record::new(vec![Field::Null])
    }

    fn drain_ops(fw: &mut TestForwarder) -> Vec<Operation> {
        fw.operations
            .drain(..)
            .map(|operation| operation.op)
            .collect()
    }

    #[test]
    fn right_membership_reveals_and_hides_stored_left_rows() {
        let mut processor = processor();
        let mut fw = TestForwarder { operations: vec![] };
        let left = record(&[1, 10]);

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut fw,
            )
            .unwrap();
        assert!(fw.operations.is_empty());

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            fw.operations,
            vec![op(
                DEFAULT_PORT_HANDLE,
                Operation::BatchInsert {
                    new: vec![left.clone()]
                }
            )]
        );

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete { old: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            fw.operations[1],
            op(DEFAULT_PORT_HANDLE, Operation::Delete { old: left })
        );
    }

    #[test]
    fn duplicate_right_rows_only_toggle_visibility_on_zero_crossings() {
        let mut processor = processor();
        let mut fw = TestForwarder { operations: vec![] };
        let left = record(&[1, 10]);

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete { old: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(fw.operations.len(), 1);

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete { old: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(fw.operations.len(), 2);
    }

    #[test]
    fn right_updates_toggle_existing_left_rows() {
        let mut processor = processor();
        let mut fw = TestForwarder { operations: vec![] };
        let left = record(&[1, 10]);

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[20]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert!(fw.operations.is_empty());

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Update {
                        old: record(&[20]),
                        new: record(&[10]),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            drain_ops(&mut fw),
            vec![Operation::BatchInsert {
                new: vec![left.clone()]
            }]
        );

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Update {
                        old: record(&[10]),
                        new: record(&[30]),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(drain_ops(&mut fw), vec![Operation::Delete { old: left }]);
    }

    #[test]
    fn null_keys_never_match_or_accumulate_for_in() {
        let mut processor = processor();
        let mut fw = TestForwarder { operations: vec![] };

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: null_group_record(1),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: right_null_record(),
                    },
                ),
                &mut fw,
            )
            .unwrap();

        assert!(fw.operations.is_empty());
        assert!(processor.left_rows.is_empty());
        assert_eq!(processor.right_null_count, 0);
    }

    #[test]
    fn left_updates_follow_dynamic_membership() {
        let mut processor = processor();
        let mut fw = TestForwarder { operations: vec![] };
        let hidden = record(&[1, 10]);
        let visible = record(&[1, 20]);

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[20]) },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: hidden.clone(),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Update {
                        old: hidden,
                        new: visible.clone(),
                    },
                ),
                &mut fw,
            )
            .unwrap();

        assert_eq!(
            fw.operations,
            vec![op(DEFAULT_PORT_HANDLE, Operation::Insert { new: visible })]
        );
    }

    #[test]
    fn not_in_hides_matching_keys_and_reveals_on_delete() {
        let mut processor = not_in_processor();
        let mut fw = TestForwarder { operations: vec![] };
        let left = record(&[1, 10]);

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: left.clone() },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            drain_ops(&mut fw),
            vec![Operation::Insert { new: left.clone() }]
        );

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert { new: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            drain_ops(&mut fw),
            vec![Operation::Delete { old: left.clone() }]
        );

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete { old: record(&[10]) },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            drain_ops(&mut fw),
            vec![Operation::BatchInsert { new: vec![left] }]
        );
    }

    #[test]
    fn not_in_right_null_hides_all_rows_until_last_null_is_deleted() {
        let mut processor = not_in_processor();
        let mut fw = TestForwarder { operations: vec![] };
        let left_a = record(&[1, 10]);
        let left_b = record(&[2, 20]);

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::BatchInsert {
                        new: vec![left_a.clone(), left_b.clone()],
                    },
                ),
                &mut fw,
            )
            .unwrap();
        assert_eq!(
            drain_ops(&mut fw),
            vec![Operation::BatchInsert {
                new: vec![left_a.clone(), left_b.clone()]
            }]
        );

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: right_null_record(),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        let mut deleted = drain_ops(&mut fw);
        assert_eq!(deleted.len(), 2);
        assert!(deleted.iter().any(|op| op
            == &Operation::Delete {
                old: left_a.clone()
            }));
        assert!(deleted.iter().any(|op| op
            == &Operation::Delete {
                old: left_b.clone()
            }));
        deleted.clear();

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: right_null_record(),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete {
                        old: right_null_record(),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        assert!(fw.operations.is_empty());

        processor
            .process(
                op(
                    RIGHT_IN_SUBQUERY_PORT,
                    Operation::Delete {
                        old: right_null_record(),
                    },
                ),
                &mut fw,
            )
            .unwrap();
        let inserted = drain_ops(&mut fw);
        assert_eq!(inserted.len(), 1);
        let Operation::BatchInsert { new } = &inserted[0] else {
            panic!("expected BatchInsert after deleting the last right NULL");
        };
        assert_eq!(new.len(), 2);
        assert!(new.contains(&left_a));
        assert!(new.contains(&left_b));
    }

    #[test]
    fn left_null_never_matches_not_in() {
        let mut processor = not_in_processor();
        let mut fw = TestForwarder { operations: vec![] };

        processor
            .process(
                op(
                    LEFT_IN_SUBQUERY_PORT,
                    Operation::Insert {
                        new: null_group_record(1),
                    },
                ),
                &mut fw,
            )
            .unwrap();

        assert!(fw.operations.is_empty());
        assert!(processor.left_rows.is_empty());
    }
}
