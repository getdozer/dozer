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
}

impl SelectionProcessor {
    pub fn new(input_schema: Schema, expression: Expression) -> Result<Self, PipelineError> {
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
}

#[cfg(test)]
mod tests {
    use super::SelectionProcessor;
    use crate::tests::utils::{create_test_runtime, get_select};
    use dozer_core::channels::ProcessorChannelForwarder;
    use dozer_core::node::Processor;
    use dozer_core::DEFAULT_PORT_HANDLE;
    use dozer_sql_expression::builder::ExpressionBuilder;
    use dozer_types::types::{
        Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition,
        TableOperation,
    };

    #[derive(Default)]
    struct TestForwarder {
        operations: Vec<TableOperation>,
    }

    impl ProcessorChannelForwarder for TestForwarder {
        fn send(&mut self, op: TableOperation) {
            self.operations.push(op);
        }
    }

    fn build_schema() -> Schema {
        Schema::default()
            .field(
                FieldDefinition::new(
                    String::from("country"),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone()
    }

    fn build_processor(sql: &str) -> SelectionProcessor {
        let schema = build_schema();
        let select = get_select(sql).expect("valid SQL");
        let selection = select.selection.expect("WHERE clause is required");
        let runtime = create_test_runtime();
        let mut builder = ExpressionBuilder::new(schema.fields.len(), runtime.clone());
        let expression = runtime
            .block_on(builder.build(false, &selection, &schema, &[]))
            .expect("selection expression should build");

        SelectionProcessor::new(schema, expression).expect("selection processor")
    }

    fn record(country: &str) -> Record {
        Record::new(vec![Field::String(country.to_string())])
    }

    #[test]
    fn in_list_filters_records_and_updates() {
        let mut processor =
            build_processor("SELECT * FROM users WHERE country IN ('Italy', 'France')");
        let mut fw = TestForwarder::default();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: record("Italy"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: record("Germany"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Update {
                        old: record("Spain"),
                        new: record("France"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Update {
                        old: record("Italy"),
                        new: record("Germany"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        assert_eq!(fw.operations.len(), 3);
        assert!(matches!(fw.operations[0].op, Operation::Insert { .. }));
        assert!(matches!(fw.operations[1].op, Operation::Insert { .. }));
        assert!(matches!(fw.operations[2].op, Operation::Delete { .. }));
        assert!(fw
            .operations
            .iter()
            .all(|op| op.port == DEFAULT_PORT_HANDLE));
    }

    #[test]
    fn not_in_list_negates_the_predicate() {
        let mut processor =
            build_processor("SELECT * FROM users WHERE country NOT IN ('Italy', 'France')");
        let mut fw = TestForwarder::default();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: record("Italy"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        processor
            .process(
                TableOperation::without_id(
                    Operation::Insert {
                        new: record("Germany"),
                    },
                    DEFAULT_PORT_HANDLE,
                ),
                &mut fw,
            )
            .unwrap();

        assert_eq!(fw.operations.len(), 1);
        assert!(matches!(fw.operations[0].op, Operation::Insert { .. }));
    }
}
