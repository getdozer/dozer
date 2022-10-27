use super::common::column_index;
use super::projection_builder::ProjectionBuilder;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::core::channels::ProcessorChannelForwarder;
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::{Processor, ProcessorFactory};
use dozer_types::core::state::{StateStore, StateStoreOptions};
use dozer_types::errors::execution::ExecutionError;
use dozer_types::errors::execution::ExecutionError::InternalError;
use dozer_types::errors::pipeline::PipelineError;
use dozer_types::types::{FieldDefinition, Operation, Record, Schema};
use log::info;
use sqlparser::ast::SelectItem;
use std::collections::HashMap;

pub struct ProjectionProcessorFactory {
    statement: Vec<SelectItem>,
}

impl ProjectionProcessorFactory {
    /// Creates a new [`ProjectionProcessorFactory`].
    pub fn new(statement: Vec<SelectItem>) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory for ProjectionProcessorFactory {
    fn get_state_store_opts(&self) -> Option<StateStoreOptions> {
        None
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(ProjectionProcessor {
            statement: self.statement.clone(),
            input_schema: Schema::empty(),
            expressions: vec![],
            builder: ProjectionBuilder {},
        })
    }
}

pub struct ProjectionProcessor {
    statement: Vec<SelectItem>,
    input_schema: Schema,
    expressions: Vec<(String, Expression)>,
    builder: ProjectionBuilder,
}

impl ProjectionProcessor {
    fn build_projection(
        &self,
        statement: Vec<SelectItem>,
        input_schema: &Schema,
    ) -> Result<Vec<(String, Expression)>, PipelineError> {
        self.builder.build_projection(&statement, input_schema)
    }

    fn build_output_schema(
        &mut self,
        input_schema: &Schema,
        expressions: Vec<(String, Expression)>,
    ) -> Result<Schema, ExecutionError> {
        self.input_schema = input_schema.clone();
        let mut output_schema = input_schema.clone();

        for e in expressions.iter() {
            let field_name = e.0.clone();
            let field_type = e.1.get_type(input_schema);
            let field_nullable = true;

            if column_index(&field_name, &output_schema).is_err() {
                output_schema.fields.push(FieldDefinition::new(
                    field_name,
                    field_type,
                    field_nullable,
                ));
            }
        }

        Ok(output_schema)
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];

        for field in record.values.iter() {
            results.push(field.clone());
        }

        for expr in &self.expressions {
            results.push(
                expr.1
                    .evaluate(record)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Delete {
            old: Record::new(None, results),
        })
    }

    fn insert(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];
        for field in record.values.clone() {
            results.push(field);
        }

        for expr in self.expressions.clone() {
            if let Ok(idx) = column_index(&expr.0, &self.input_schema) {
                let _ = std::mem::replace(&mut results[idx], expr.1.evaluate(record)?);
            } else {
                results.push(
                    expr.1
                        .evaluate(record)
                        .map_err(|e| InternalError(Box::new(e)))?,
                );
            }
        }
        Ok(Operation::Insert {
            new: Record::new(None, results),
        })
    }

    fn update(&self, old: &Record, new: &Record) -> Result<Operation, ExecutionError> {
        let mut old_results = vec![];
        let mut new_results = vec![];

        for field in old.values.iter() {
            old_results.push(field.clone());
        }

        for field in new.values.iter() {
            new_results.push(field.clone());
        }

        for expr in &self.expressions {
            old_results.push(
                expr.1
                    .evaluate(old)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
            new_results.push(
                expr.1
                    .evaluate(new)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }

        Ok(Operation::Update {
            old: Record::new(None, old_results),
            new: Record::new(None, new_results),
        })
    }
}

impl Processor for ProjectionProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let input_schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap();
        let expressions = self.build_projection(self.statement.clone(), input_schema)?;
        self.expressions = expressions.clone();
        self.build_output_schema(input_schema, expressions)
    }

    fn init<'a>(&'_ mut self, _: &mut dyn StateStore) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Projection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state_store: &mut dyn StateStore,
    ) -> Result<(), ExecutionError> {
        let _ = match op {
            Operation::Delete { ref old } => fw.send(self.delete(old)?, DEFAULT_PORT_HANDLE),
            Operation::Insert { ref new } => fw.send(self.insert(new)?, DEFAULT_PORT_HANDLE),
            Operation::Update { ref old, ref new } => {
                fw.send(self.update(old, new)?, DEFAULT_PORT_HANDLE)
            }
            Operation::SchemaUpdate { schema: _ } => Ok(()),
        };
        Ok(())
    }
}
