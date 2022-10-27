use super::projection_builder::ProjectionBuilder;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_types::core::channels::ProcessorChannelForwarder;
use dozer_types::core::node::PortHandle;
use dozer_types::core::node::{Processor, ProcessorFactory};
use dozer_types::errors::execution::ExecutionError;
use dozer_types::errors::execution::ExecutionError::InternalError;
use dozer_types::types::{FieldDefinition, Operation, Record, Schema};
use log::info;
use rocksdb::DB;
use sqlparser::ast::SelectItem;
use std::collections::HashMap;
use std::sync::Arc;

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
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn build(&self) -> Box<dyn Processor> {
        Box::new(ProjectionProcessor {
            statement: self.statement.clone(),
            expressions: vec![],
            builder: ProjectionBuilder {},
        })
    }
}

pub struct ProjectionProcessor {
    statement: Vec<SelectItem>,
    expressions: Vec<Expression>,
    builder: ProjectionBuilder,
}

impl ProjectionProcessor {
    fn build_projection(
        &self,
        statement: Vec<SelectItem>,
        schema: &Schema,
    ) -> Result<(Vec<Expression>, Vec<String>), ExecutionError> {
        self.builder
            .build_projection(&statement, schema)
            .map_err(|e| InternalError(Box::new(e)))
    }

    fn build_output_schema(
        &self,
        input_schema: &Schema,
        names: &[String],
    ) -> Result<Schema, ExecutionError> {
        let mut output_schema = Schema::empty();

        for (counter, e) in self.expressions.iter().enumerate() {
            let field_name = names.get(counter).unwrap().clone();
            let field_type = e.get_type(input_schema);
            let field_nullable = true;
            output_schema
                .fields
                .push(FieldDefinition::new(field_name, field_type, field_nullable));
        }

        Ok(output_schema)
    }

    fn delete(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];
        for expr in &self.expressions {
            results.push(
                expr.evaluate(record)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Delete {
            old: Record::new(None, results),
        })
    }

    fn insert(&mut self, record: &Record) -> Result<Operation, ExecutionError> {
        let mut results = vec![];
        for expr in &self.expressions {
            results.push(
                expr.evaluate(record)
                    .map_err(|e| InternalError(Box::new(e)))?,
            );
        }
        Ok(Operation::Insert {
            new: Record::new(None, results),
        })
    }

    fn update(&self, old: &Record, new: &Record) -> Result<Operation, ExecutionError> {
        let mut old_results = vec![];
        let mut new_results = vec![];
        for expr in &self.expressions {
            old_results.push(expr.evaluate(old).map_err(|e| InternalError(Box::new(e)))?);
            new_results.push(expr.evaluate(new).map_err(|e| InternalError(Box::new(e)))?);
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
        let (expressions, names) = self.build_projection(self.statement.clone(), input_schema)?;
        self.expressions = expressions;
        self.build_output_schema(input_schema, &names)
    }

    fn init<'a>(&'_ mut self, _: Arc<DB>) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Projection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _db: &DB,
    ) -> Result<(), ExecutionError> {
        let _ = match op {
            Operation::Delete { ref old } => fw.send(self.delete(old)?, DEFAULT_PORT_HANDLE),
            Operation::Insert { ref new } => fw.send(self.insert(new)?, DEFAULT_PORT_HANDLE),
            Operation::Update { ref old, ref new } => {
                fw.send(self.update(old, new)?, DEFAULT_PORT_HANDLE)
            }
        };
        Ok(())
    }
}
