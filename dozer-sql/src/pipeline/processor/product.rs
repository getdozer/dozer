use crate::pipeline::expression::builder::ExpressionBuilder;
use crate::pipeline::expression::execution::Expression;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::mt_executor::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::PortHandle;
use dozer_core::dag::node::{Processor, ProcessorFactory};
use dozer_core::storage::lmdb_sys::Transaction;
use dozer_types::types::{Field, Operation, Schema};
use log::info;
use sqlparser::ast::TableWithJoins;
use std::collections::HashMap;

pub struct ProductProcessorFactory {
    statement: Vec<TableWithJoins>,
}

impl ProductProcessorFactory {
    /// Creates a new [`ProductProcessorFactory`].
    pub fn new(statement: Vec<TableWithJoins>) -> Self {
        Self { statement }
    }
}

impl ProcessorFactory for ProductProcessorFactory {
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
        Box::new(ProductProcessor {
            statement: self.statement.clone(),
            expression: Box::new(Expression::Literal(Field::Boolean(true))),
            builder: ExpressionBuilder {},
        })
    }
}

pub struct ProductProcessor {
    statement: Vec<TableWithJoins>,
    expression: Box<Expression>,
    builder: ExpressionBuilder,
}

impl ProductProcessor {
    fn build_expression(
        &self,
        sql_expression: &[TableWithJoins],
        input_schema: &Schema,
    ) -> Result<Box<Expression>, ExecutionError> {
        let expressions = sql_expression
            .iter()
            .map(|item| self.parse_table_with_join(item, input_schema))
            .collect::<Result<Vec<(String, Expression)>, PipelineError>>()?;

        Ok(expressions)
    }

    fn delete(&self, record: &dozer_types::types::Record) -> Operation {
        Operation::Delete {
            old: record.clone(),
        }
    }

    fn insert(&self, record: &dozer_types::types::Record) -> Operation {
        Operation::Insert {
            new: record.clone(),
        }
    }
}

impl StatefulProcessor for ProductProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let schema = input_schemas.get(&DEFAULT_PORT_HANDLE).unwrap();
        self.expression = self.build_expression(&self.statement, schema)?;
        Ok(schema.clone())
    }

    fn init<'a>(&'_ mut self, _state: Option<&mut Transaction>) -> Result<(), ExecutionError> {
        info!("{:?}", "Initialising Product Processor");
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &dyn ProcessorChannelForwarder,
        _state: Option<&mut Transaction>,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                let _ = fw.send(op, DEFAULT_PORT_HANDLE);
            }
            Operation::Insert { ref new } => {
                let _ = fw.send(op, DEFAULT_PORT_HANDLE);
            }
            Operation::Update { ref old, ref new } => {
                let _ = fw.send(op, DEFAULT_PORT_HANDLE);
            }
        }
        Ok(())
    }
}
