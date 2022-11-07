use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::builder::{column_index, ExpressionBuilder, ExpressionType};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::errors::ExecutionError::InternalError;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::storage::lmdb_sys::Transaction;
use dozer_types::errors::pipeline::PipelineError::InvalidExpression;
use dozer_types::errors::pipeline::PipelineError::InvalidOperator;
use dozer_types::internal_err;
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
    fn is_stateful(&self) -> bool {
        false
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
            builder: ExpressionBuilder {},
        })
    }
}

pub struct ProjectionProcessor {
    statement: Vec<SelectItem>,
    input_schema: Schema,
    expressions: Vec<(String, Expression)>,
    builder: ExpressionBuilder,
}

impl ProjectionProcessor {
    fn build_projection(
        &self,
        statement: Vec<SelectItem>,
        input_schema: &Schema,
    ) -> Result<Vec<(String, Expression)>, PipelineError> {
        let expressions = statement
            .iter()
            .map(|item| self.parse_sql_select_item(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()?;

        Ok(expressions)
    }

    fn parse_sql_select_item(
        &self,
        sql: &SelectItem,
        schema: &Schema,
    ) -> Result<(String, Expression), PipelineError> {
        match sql {
            SelectItem::UnnamedExpr(sql_expr) => {
                match self.builder.parse_sql_expression(
                    &ExpressionType::PreAggregation,
                    sql_expr,
                    schema,
                ) {
                    Ok(expr) => Ok((sql_expr.to_string(), *expr.0)),
                    Err(error) => Err(error),
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                Err(InvalidExpression(format!("{}:{}", expr, alias)))
            }
            SelectItem::Wildcard => Err(InvalidOperator("*".to_string())),
            SelectItem::QualifiedWildcard(ref object_name) => {
                Err(InvalidOperator(object_name.to_string()))
            }
        }
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
                let _ =
                    std::mem::replace(&mut results[idx], internal_err!(expr.1.evaluate(record))?);
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
        let expressions =
            internal_err!(self.build_projection(self.statement.clone(), input_schema))?;
        self.expressions = expressions.clone();
        self.build_output_schema(input_schema, expressions)
    }

    fn init<'a>(&'_ mut self, _state: Option<&mut Transaction>) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Projection Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state: Option<&mut Transaction>,
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
