use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::checkpoint::serialize::Cursor;
use dozer_core::dozer_log::storage::Object;
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
}

impl SelectionProcessor {
    pub fn new(
        input_schema: Schema,
        mut expression: Expression,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Self, PipelineError> {
        if let Some(data) = checkpoint_data {
            let mut cursor = Cursor::new(&data);
            expression.deserialize_state(&mut cursor)?;
        }

        Ok(Self {
            input_schema,
            expression,
        })
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
        mut op: TableOperation,
        fw: &mut dyn ProcessorChannelForwarder,
    ) -> Result<(), BoxedError> {
        match op.op {
            Operation::Delete { ref old } => {
                if self.filter(old)? {
                    op.port = DEFAULT_PORT_HANDLE;
                    fw.send(op);
                }
            }
            Operation::Insert { ref new } => {
                if self.filter(new)? {
                    op.port = DEFAULT_PORT_HANDLE;
                    fw.send(op);
                }
            }
            Operation::Update { old, new } => {
                let old_fulfilled = self.filter(&old)?;
                let new_fulfilled = self.filter(&new)?;
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
                            .map(|fulfilled| if fulfilled { Some(record) } else { None })
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

    fn serialize(&mut self, mut object: Object) -> Result<(), BoxedError> {
        self.expression.serialize_state(&mut object)?;
        Ok(())
    }
}
