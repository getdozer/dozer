use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Lifetime, Operation, Record, TableOperation};

use crate::errors::PipelineError;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
}

impl ProductProcessor {
    pub fn new(_id: String, join_operator: JoinOperator) -> Self {
        Self { join_operator }
    }

    fn update_eviction_index(&mut self, lifetime: Lifetime) {
        self.join_operator.evict_index(&lifetime.reference);
    }

    fn evict_if_needed(&mut self, record: &Record) {
        if let Some(lifetime) = record.get_lifetime() {
            self.update_eviction_index(lifetime);
        }
    }

    fn process_batch_insert(
        &mut self,
        port: u16,
        records: Vec<Record>,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        for record in records {
            self.process(
                TableOperation::without_id(Operation::Insert { new: record }, port),
                fw,
            )?;
        }
        Ok(())
    }

    // Handle UPDATE operations on the outer table of an IN / NOT IN semi/anti-join
    // so (match, match) emits UPDATE, (no-match, match) emits INSERT, and
    // (match, no-match) emits DELETE per streaming WHERE semantics.
    fn process_semi_left_update(
        &mut self,
        old: Record,
        new: Record,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        self.evict_if_needed(&old);
        let old_records = self.join_operator.delete(JoinBranch::Left, &old, &old);
        let new_records = self
            .join_operator
            .insert(JoinBranch::Left, &new, &new)
            .map_err(PipelineError::JoinError)?;

        match (old_records.into_iter().next(), new_records.into_iter().next()) {
            (Some((_, old_rec)), Some((_, new_rec))) => {
                fw.send(TableOperation::without_id(
                    Operation::Update {
                        old: old_rec,
                        new: new_rec,
                    },
                    DEFAULT_PORT_HANDLE,
                ));
            }
            (None, Some((_, new_rec))) => {
                fw.send(TableOperation::without_id(
                    Operation::Insert { new: new_rec },
                    DEFAULT_PORT_HANDLE,
                ));
            }
            (Some((_, old_rec)), None) => {
                fw.send(TableOperation::without_id(
                    Operation::Delete { old: old_rec },
                    DEFAULT_PORT_HANDLE,
                ));
            }
            (None, None) => {}
        }
        Ok(())
    }

    fn evaluate_branch_op(
        &mut self,
        from_branch: JoinBranch,
        op: Operation,
    ) -> Result<Vec<(JoinAction, Record)>, BoxedError> {
        match op {
            Operation::Delete { old } => {
                self.evict_if_needed(&old);
                Ok(self.join_operator.delete(from_branch, &old, &old))
            }
            Operation::Insert { new } => {
                self.evict_if_needed(&new);
                Ok(self
                    .join_operator
                    .insert(from_branch, &new, &new)
                    .map_err(PipelineError::JoinError)?)
            }
            Operation::Update { old, new } => {
                self.evict_if_needed(&old);
                let mut old_records = self.join_operator.delete(from_branch, &old, &old);
                let new_records = self
                    .join_operator
                    .insert(from_branch, &new, &new)
                    .map_err(PipelineError::JoinError)?;
                old_records.extend(new_records);
                Ok(old_records)
            }
            Operation::BatchInsert { .. } => Ok(vec![]),
        }
    }
}

impl Processor for ProductProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        let from_branch = resolve_join_branch(op.port)?;
        if let Operation::BatchInsert { new } = op.op {
            return self.process_batch_insert(op.port, new, fw);
        }
        if self.join_operator.is_semi_or_anti() && matches!(from_branch, JoinBranch::Left) {
            if let Operation::Update { old, new } = op.op {
                return self.process_semi_left_update(old, new, fw);
            }
        }

        let records = self.evaluate_branch_op(from_branch, op.op)?;
        forward_join_records(records, fw);
        Ok(())
    }
}

fn resolve_join_branch(port: u16) -> Result<JoinBranch, PipelineError> {
    match port {
        0 => Ok(JoinBranch::Left),
        1 => Ok(JoinBranch::Right),
        _ => Err(PipelineError::InvalidPortHandle(port)),
    }
}

fn forward_join_records(
    records: Vec<(JoinAction, Record)>,
    fw: &mut dyn ProcessorChannelForwarder,
) {
    for (action, record) in records {
        let op = match action {
            JoinAction::Insert => Operation::Insert { new: record },
            JoinAction::Delete => Operation::Delete { old: record },
        };
        fw.send(TableOperation::without_id(op, DEFAULT_PORT_HANDLE));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dozer_core::{event::EventHub, node::ProcessorFactory};
    use dozer_sql_expression::builder::NameOrAlias;
    use dozer_sql_expression::sqlparser::ast::{
        Expr as SqlExpr, Ident, JoinOperator as SqlJoinOperator,
    };
    use dozer_types::types::{Field, FieldDefinition, Record, Schema};

    use crate::product::join::{
        factory::{LEFT_JOIN_PORT, RIGHT_JOIN_PORT},
        operator::JoinType,
    };
    use crate::{product::join::factory::JoinProcessorFactory, tests::utils::get_select};

    use super::*;

    struct TestChannelForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestChannelForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn create_schema(table_name: &'static str) -> Schema {
        let mut schema = Schema::new();
        schema
            .field(
                FieldDefinition {
                    name: "joinkey".into(),
                    typ: dozer_types::types::FieldType::UInt,
                    nullable: false,
                    source: dozer_types::types::SourceDefinition::Table {
                        connection: "test".into(),
                        name: table_name.into(),
                    },
                    description: None,
                },
                true,
            )
            .field(
                FieldDefinition {
                    name: "data".into(),
                    typ: dozer_types::types::FieldType::UInt,
                    nullable: false,
                    source: dozer_types::types::SourceDefinition::Table {
                        connection: "test".into(),
                        name: table_name.into(),
                    },
                    description: None,
                },
                false,
            );
        schema
    }

    fn create_single_col_schema(table_name: &'static str) -> Schema {
        let mut schema = Schema::new();
        schema.field(
            FieldDefinition {
                name: "joinkey".into(),
                typ: dozer_types::types::FieldType::UInt,
                nullable: false,
                source: dozer_types::types::SourceDefinition::Table {
                    connection: "test".into(),
                    name: table_name.into(),
                },
                description: None,
            },
            true,
        );
        schema
    }

    enum JoinSide {
        Left,
        Right,
    }

    struct Executor {
        processor: Box<dyn Processor>,
        forwarder: TestChannelForwarder,
    }

    impl Executor {
        async fn new(kind: JoinType) -> Self {
            let left_schema = create_schema("left");
            let right_schema = create_schema("right");

            let stmt = get_select(
                "SELECT left.joinkey FROM left INNER JOIN right ON left.joinkey = right.joinkey",
            )
            .unwrap();
            let join = &stmt.from[0].joins[0];
            let SqlJoinOperator::Inner(constraint) = join.join_operator.clone() else {
                unreachable!()
            };
            let join_op = match kind {
                JoinType::Inner => SqlJoinOperator::Inner(constraint),
                JoinType::LeftOuter => SqlJoinOperator::LeftOuter(constraint),
                JoinType::RightOuter => SqlJoinOperator::RightOuter(constraint),
                JoinType::LeftSemi | JoinType::LeftAnti => unreachable!(),
            };
            let factory = JoinProcessorFactory::new(
                "test".into(),
                Some(NameOrAlias("left".into(), None)),
                Some(NameOrAlias("right".into(), None)),
                join_op,
                false,
            );

            let schemas = [
                (LEFT_JOIN_PORT, left_schema),
                (RIGHT_JOIN_PORT, right_schema),
            ]
            .into_iter()
            .collect();
            let processor = factory
                .build(schemas, HashMap::new(), EventHub::new(1))
                .await
                .unwrap();

            Executor {
                processor,
                forwarder: TestChannelForwarder { operations: vec![] },
            }
        }

        async fn new_in_subquery(negated: bool) -> Self {
            let left_schema = create_schema("left");
            let right_schema = create_single_col_schema("right");
            let factory = JoinProcessorFactory::new_in_subquery(
                "test_in".into(),
                SqlExpr::Identifier(Ident::new("joinkey")),
                negated,
                false,
            );
            let schemas = [
                (LEFT_JOIN_PORT, left_schema),
                (RIGHT_JOIN_PORT, right_schema),
            ]
            .into_iter()
            .collect();
            let processor = factory
                .build(schemas, HashMap::new(), EventHub::new(1))
                .await
                .unwrap();
            Executor {
                processor,
                forwarder: TestChannelForwarder { operations: vec![] },
            }
        }

        fn do_op(&mut self, operation: Operation, side: JoinSide) -> Vec<Operation> {
            let port = match side {
                JoinSide::Left => LEFT_JOIN_PORT,
                JoinSide::Right => RIGHT_JOIN_PORT,
            };
            self.processor
                .process(
                    TableOperation::without_id(operation, port),
                    &mut self.forwarder,
                )
                .unwrap();
            let output_ops = self.forwarder.operations.clone();
            self.forwarder.operations.clear();
            output_ops.into_iter().map(|op| op.op).collect()
        }

        fn insert(&mut self, side: JoinSide, values: &[Field]) -> (Record, Vec<Operation>) {
            let record = Record::new(values.to_vec());
            let op = Operation::Insert {
                new: record.clone(),
            };
            (record, self.do_op(op, side))
        }

        fn update(
            &mut self,
            side: JoinSide,
            old: Record,
            new: &[Field],
        ) -> (Record, Vec<Operation>) {
            let record = Record::new(new.to_vec());
            let op = Operation::Update {
                old,
                new: record.clone(),
            };
            (record, self.do_op(op, side))
        }

        fn delete(&mut self, side: JoinSide, old: Record) -> Vec<Operation> {
            let op = Operation::Delete { old };
            self.do_op(op, side)
        }
    }

    fn join_record(left: Record, right: Record) -> Record {
        let mut values = left.values;
        values.extend(right.values);
        Record::new(values)
    }

    #[tokio::test]
    async fn test_inner_join() {
        let mut exec = Executor::new(JoinType::Inner).await;

        let (left_record, ops) = exec.insert(JoinSide::Left, &[Field::UInt(0), Field::UInt(1)]);
        assert_eq!(ops, &[]);

        let (right_record, ops) = exec.insert(JoinSide::Right, &[Field::UInt(0), Field::UInt(2)]);
        assert_eq!(
            ops,
            &[Operation::Insert {
                new: join_record(left_record.clone(), right_record.clone())
            }]
        );
        let (new_left_record, ops) = exec.update(
            JoinSide::Left,
            left_record.clone(),
            &[Field::UInt(0), Field::UInt(2)],
        );
        assert_eq!(
            ops,
            &[
                Operation::Delete {
                    old: join_record(left_record.clone(), right_record.clone())
                },
                Operation::Insert {
                    new: join_record(new_left_record.clone(), right_record.clone())
                }
            ]
        );

        assert_eq!(
            exec.delete(JoinSide::Right, right_record.clone()),
            &[Operation::Delete {
                old: join_record(new_left_record, right_record)
            }]
        );
    }

    #[tokio::test]
    async fn test_left_outer_join() {
        let mut exec = Executor::new(JoinType::LeftOuter).await;
        let null_record = Record::new(vec![Field::Null, Field::Null]);

        let (left_record, ops) = exec.insert(JoinSide::Left, &[Field::UInt(0), Field::UInt(1)]);
        assert_eq!(
            ops,
            &[Operation::Insert {
                new: join_record(left_record.clone(), null_record.clone())
            }]
        );

        let (right_record, ops) = exec.insert(JoinSide::Right, &[Field::UInt(0), Field::UInt(2)]);
        assert_eq!(
            ops,
            &[
                Operation::Delete {
                    old: join_record(left_record.clone(), null_record.clone()),
                },
                Operation::Insert {
                    new: join_record(left_record.clone(), right_record.clone())
                }
            ]
        );

        assert_eq!(
            exec.delete(JoinSide::Right, right_record.clone()),
            &[
                Operation::Delete {
                    old: join_record(left_record.clone(), right_record)
                },
                Operation::Insert {
                    new: join_record(left_record, null_record)
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_right_outer_join() {
        let mut exec = Executor::new(JoinType::RightOuter).await;
        let null_record = Record::new(vec![Field::Null, Field::Null]);

        let (_, ops) = exec.insert(JoinSide::Left, &[Field::UInt(9), Field::UInt(1)]);
        assert_eq!(ops, &[]);

        let (right_record, ops) = exec.insert(JoinSide::Right, &[Field::UInt(0), Field::UInt(2)]);
        assert_eq!(
            ops,
            &[Operation::Insert {
                new: join_record(null_record, right_record)
            }]
        );
    }

    #[tokio::test]
    async fn test_in_subquery_semi_join_streaming_ops() {
        let mut exec = Executor::new_in_subquery(false).await;

        let (left_rec, ops) = exec.insert(JoinSide::Left, &[Field::UInt(10), Field::UInt(100)]);
        assert_eq!(ops, &[]);

        let (right_rec, ops) = exec.insert(JoinSide::Right, &[Field::UInt(10)]);
        assert_eq!(ops, &[Operation::Insert { new: left_rec.clone() }]);

        // Updating left record while still matching emits Operation::Update
        let (updated_left, ops) =
            exec.update(JoinSide::Left, left_rec.clone(), &[Field::UInt(10), Field::UInt(200)]);
        assert_eq!(
            ops,
            &[Operation::Update {
                old: left_rec,
                new: updated_left.clone()
            }]
        );

        // Updating left record to non-matching key emits Operation::Delete
        let (unmatched_left, ops) = exec.update(
            JoinSide::Left,
            updated_left.clone(),
            &[Field::UInt(20), Field::UInt(200)],
        );
        assert_eq!(ops, &[Operation::Delete { old: updated_left }]);

        // Updating left record back to matching key emits Operation::Insert
        let (rematched_left, ops) = exec.update(
            JoinSide::Left,
            unmatched_left,
            &[Field::UInt(10), Field::UInt(300)],
        );
        assert_eq!(
            ops,
            &[Operation::Insert {
                new: rematched_left.clone()
            }]
        );

        // Deleting inner subquery value emits Operation::Delete for parent record
        assert_eq!(
            exec.delete(JoinSide::Right, right_rec),
            &[Operation::Delete { old: rematched_left }]
        );
    }
}
