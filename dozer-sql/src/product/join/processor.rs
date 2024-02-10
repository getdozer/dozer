use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::dozer_log::storage::Object;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_tracing::Labels;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Lifetime, Operation, TableOperation};
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
    increment_counter,
};

use crate::errors::PipelineError;

use super::operator::{JoinAction, JoinBranch, JoinOperator};

#[derive(Debug)]
pub struct ProductProcessor {
    join_operator: JoinOperator,
    labels: Labels,
}

const LEFT_LOOKUP_SIZE: &str = "product.left_lookup_size";
const RIGHT_LOOKUP_SIZE: &str = "product.right_lookup_size";
const UNSATISFIED_JOINS: &str = "product.unsatisfied_joins";
const IN_OPS: &str = "product.in_ops";
const OUT_OPS: &str = "product.out_ops";
const LATENCY: &str = "product.latency";

impl ProductProcessor {
    pub fn new(id: String, join_operator: JoinOperator) -> Self {
        describe_gauge!(
            LEFT_LOOKUP_SIZE,
            "Total number of items in the left lookup table"
        );
        describe_gauge!(
            RIGHT_LOOKUP_SIZE,
            "Total number of items in the right lookup table"
        );
        describe_counter!(
            UNSATISFIED_JOINS,
            "Operations not matching the Join condition"
        );
        describe_counter!(
            IN_OPS,
            "Number of records received by the product processor"
        );
        describe_counter!(
            OUT_OPS,
            "Number of records forwarded by the product processor"
        );

        describe_histogram!(LATENCY, "Processing latency");

        let mut labels = Labels::empty();
        labels.push("pid", id);
        Self {
            join_operator,
            labels,
        }
    }

    fn update_eviction_index(&mut self, lifetime: Lifetime) {
        self.join_operator.evict_index(&lifetime.reference);
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
        let from_branch = match op.port {
            0 => JoinBranch::Left,
            1 => JoinBranch::Right,
            _ => return Err(PipelineError::InvalidPortHandle(op.port).into()),
        };

        let now = std::time::Instant::now();
        let records = match op.op {
            Operation::Delete { old } => {
                if let Some(lifetime) = old.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                self.join_operator.delete(from_branch, &old, &old)
            }
            Operation::Insert { new } => {
                if let Some(lifetime) = new.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                self.join_operator
                    .insert(from_branch, &new, &new)
                    .map_err(PipelineError::JoinError)?
            }
            Operation::Update { old, new } => {
                if let Some(lifetime) = old.get_lifetime() {
                    self.update_eviction_index(lifetime);
                }

                let mut old_records = self.join_operator.delete(from_branch, &old, &old);

                let new_records = self
                    .join_operator
                    .insert(from_branch, &new, &new)
                    .map_err(PipelineError::JoinError)?;

                old_records.extend(new_records);
                old_records
            }
            Operation::BatchInsert { new } => {
                for record in &new {
                    self.process(
                        TableOperation::without_id(
                            Operation::Insert {
                                new: record.clone(),
                            },
                            op.port,
                        ),
                        fw,
                    )?;
                }
                return Ok(());
            }
        };

        let elapsed = now.elapsed();
        histogram!(LATENCY, elapsed, self.labels.clone());
        increment_counter!(IN_OPS, self.labels.clone());

        counter!(OUT_OPS, records.len() as u64, self.labels.clone());

        gauge!(
            LEFT_LOOKUP_SIZE,
            self.join_operator.left_lookup_size() as f64,
            self.labels.clone()
        );
        gauge!(
            RIGHT_LOOKUP_SIZE,
            self.join_operator.right_lookup_size() as f64,
            self.labels.clone()
        );

        if records.is_empty() {
            increment_counter!(UNSATISFIED_JOINS, self.labels.clone());
        }

        for (action, record) in records {
            match action {
                JoinAction::Insert => {
                    fw.send(TableOperation::without_id(
                        Operation::Insert { new: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
                JoinAction::Delete => {
                    fw.send(TableOperation::without_id(
                        Operation::Delete { old: record },
                        DEFAULT_PORT_HANDLE,
                    ));
                }
            }
        }

        Ok(())
    }

    fn serialize(&mut self, object: Object) -> Result<(), BoxedError> {
        self.join_operator.serialize(object).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dozer_core::node::ProcessorFactory;
    use dozer_sql_expression::builder::NameOrAlias;
    use dozer_sql_expression::sqlparser::ast::JoinOperator as SqlJoinOperator;
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
                },
                false,
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
            let join_op = join.join_operator.clone();
            let SqlJoinOperator::Inner(constraint) = join_op else {
                unreachable!()
            };
            let join_op = match kind {
                JoinType::Inner => SqlJoinOperator::Inner(constraint),
                JoinType::LeftOuter => SqlJoinOperator::LeftOuter(constraint),
                JoinType::RightOuter => SqlJoinOperator::RightOuter(constraint),
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
            let processor = factory.build(schemas, HashMap::new(), None).await.unwrap();

            let forwarder = TestChannelForwarder { operations: vec![] };
            Executor {
                processor,
                forwarder,
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
                old: join_record(new_left_record.clone(), right_record.clone())
            },]
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
            &[
                Operation::Delete {
                    old: join_record(new_left_record.clone(), right_record.clone())
                },
                Operation::Insert {
                    new: join_record(new_left_record.clone(), null_record.clone(),)
                },
            ]
        );
        let (right_record, _) = exec.insert(JoinSide::Right, &[Field::UInt(0), Field::UInt(2)]);

        assert_eq!(
            exec.delete(JoinSide::Left, new_left_record.clone()),
            &[Operation::Delete {
                old: join_record(new_left_record.clone(), right_record.clone())
            },]
        );
    }

    #[tokio::test]
    async fn test_right_outer_join() {
        let mut exec = Executor::new(JoinType::RightOuter).await;

        let null_record = Record::new(vec![Field::Null, Field::Null]);

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
                    new: join_record(null_record.clone(), right_record.clone())
                },
                Operation::Delete {
                    old: join_record(null_record.clone(), right_record.clone())
                },
                Operation::Insert {
                    new: join_record(new_left_record.clone(), right_record.clone())
                }
            ]
        );

        assert_eq!(
            exec.delete(JoinSide::Left, right_record.clone()),
            &[
                Operation::Delete {
                    old: join_record(new_left_record.clone(), right_record.clone())
                },
                Operation::Insert {
                    new: join_record(null_record.clone(), right_record.clone(),)
                },
            ]
        );
        let (new_left_record, _) = exec.insert(JoinSide::Left, &[Field::UInt(0), Field::UInt(2)]);

        assert_eq!(
            exec.delete(JoinSide::Right, new_left_record.clone()),
            &[Operation::Delete {
                old: join_record(new_left_record.clone(), right_record.clone())
            },]
        );
    }
}
