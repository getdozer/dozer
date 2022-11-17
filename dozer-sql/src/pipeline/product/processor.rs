use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::{PortHandle, StatefulProcessor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Database, Environment, RwTransaction};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Schema};
use sqlparser::ast::TableWithJoins;
use std::collections::HashMap;

use dozer_core::dag::errors::ExecutionError::InternalError;

pub struct TableOperation {
    relation: i16,
    join: Vec<JoinOperation>,
}

pub struct JoinOperation {
    relation: i16,
    constraint: Expression,
}

/// Cartesian Product Processor
pub struct ProductProcessor {
    /// Parsed FROM Statement
    statement: TableWithJoins,

    /// Database to store the state
    db: Option<Database>,
}

impl ProductProcessor {
    /// Creates a new [`ProductProcessor`].
    pub fn new(statement: TableWithJoins) -> Self {
        Self {
            statement,
            db: None,
        }
    }

    fn init_store(&mut self, txn: &mut dyn Environment) -> Result<(), PipelineError> {
        self.db = Some(txn.open_database("aggr", false)?);
        Ok(())
    }

    fn build_expression(
        &self,
        sql_expression: &TableWithJoins,
        input_schema: &Schema,
    ) -> Result<Box<Expression>, ExecutionError> {
        // let expressions = sql_expression
        //     .iter()
        //     .map(|item| self.parse_table_with_join(item, input_schema))
        //     .collect::<Result<Vec<(String, Expression)>, PipelineError>>()?;

        // Ok(expressions)
        todo!()
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

    fn join_schemas(
        &self,
        input_schemas: &&HashMap<u16, Schema>,
    ) -> Result<Schema, ExecutionError> {
        todo!()
    }
}

impl StatefulProcessor for ProductProcessor {
    fn update_schema(
        &mut self,
        output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let output_schema = self.join_schemas(&input_schemas)?;
        Ok(output_schema)
    }

    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(state))
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
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
