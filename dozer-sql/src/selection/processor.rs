use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::epoch::Epoch;
use dozer_core::node::Processor;
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_sql_expression::execution::Expression;
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{Field, Operation, Record, Schema, TableOperation};

use crate::errors::PipelineError;

#[derive(Debug)]
pub struct SelectionProcessor {
    expression: Expression,
    input_schema: Schema,
    internal_fields: usize,
}

impl SelectionProcessor {
    pub fn new(input_schema: Schema, expression: Expression) -> Result<Self, PipelineError> {
        Ok(Self {
            input_schema,
            expression,
            internal_fields: 0,
        })
    }

    pub(crate) fn with_internal_fields(mut self, count: usize) -> Self {
        self.internal_fields = count;
        self
    }

    fn project(&self, mut record: Record) -> Record {
        record
            .values
            .truncate(record.values.len() - self.internal_fields);
        record
    }

    fn filter(&mut self, record: &Record) -> Result<bool, PipelineError> {
        Ok(self.expression.evaluate(record, &self.input_schema)? == Field::Boolean(true))
    }
}

impl Processor for SelectionProcessor {
    fn commit(&self, _epoch: &Epoch) -> Result<(), BoxedError> {
        Ok(())
    }

    fn process(
        &mut self,
        op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { old } => {
                if self.filter(&old)? {
                    fw.send(TableOperation {
                        id: op.id,
                        op: Operation::Delete {
                            old: self.project(old),
                        },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::Insert { new } => {
                if self.filter(&new)? {
                    fw.send(TableOperation {
                        id: op.id,
                        op: Operation::Insert {
                            new: self.project(new),
                        },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
            Operation::Update { old, new } => {
                let old_fulfilled = self.filter(&old)?;
                let new_fulfilled = self.filter(&new)?;
                let changed = old != new;
                let old = self.project(old);
                let new = self.project(new);
                // A mark changed, but the projected row and compound predicate
                // may be unchanged (for example, the other arm of an OR is true).
                if self.internal_fields > 0
                    && changed
                    && old_fulfilled == new_fulfilled
                    && old == new
                {
                    return Ok(());
                }
                match (old_fulfilled, new_fulfilled) {
                    (true, true) => {
                        // both records fulfills the WHERE condition, forward the operation
                        fw.send(TableOperation {
                            id: op.id,
                            op: Operation::Update { old, new },
                            port: DEFAULT_PORT_HANDLE,
                        });
                    }
                    (true, false) => {
                        // the old record fulfills the WHERE condition while then new one doesn't, forward a delete operation
                        fw.send(TableOperation {
                            id: op.id,
                            op: Operation::Delete { old },
                            port: DEFAULT_PORT_HANDLE,
                        });
                    }
                    (false, true) => {
                        // the old record doesn't fulfill the WHERE condition while then new one does, forward an insert operation
                        fw.send(TableOperation {
                            id: op.id,
                            op: Operation::Insert { new },
                            port: DEFAULT_PORT_HANDLE,
                        });
                    }
                    (false, false) => {
                        // both records doesn't fulfill the WHERE condition, don't forward the operation
                    }
                }
            }
            Operation::BatchInsert { new } => {
                let records = new
                    .into_iter()
                    .filter_map(|record| {
                        self.filter(&record)
                            .map(|fulfilled| {
                                if fulfilled {
                                    Some(self.project(record))
                                } else {
                                    None
                                }
                            })
                            .transpose()
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !records.is_empty() {
                    fw.send(TableOperation {
                        id: op.id,
                        op: Operation::BatchInsert { new: records },
                        port: DEFAULT_PORT_HANDLE,
                    });
                }
            }
        }
        Ok(())
    }
}
