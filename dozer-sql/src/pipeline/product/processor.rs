use crate::pipeline::errors::PipelineError;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::{PortHandle, StatefulProcessor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Environment, RwTransaction};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Record, Schema};
use sqlparser::ast::TableWithJoins;
use std::collections::HashMap;

use dozer_core::dag::errors::ExecutionError::InternalError;

use super::factory::{build_join_chain, get_input_tables};
use super::join::JoinTable;

/// Cartesian Product Processor
pub struct ProductProcessor {
    /// Parsed FROM Statement
    statement: TableWithJoins,

    /// List of input ports by table name
    input_tables: Vec<String>,

    /// Join operations
    join_tables: HashMap<PortHandle, JoinTable>,
}

impl ProductProcessor {
    /// Creates a new [`ProductProcessor`].
    pub fn new(statement: TableWithJoins) -> Self {
        Self {
            statement: statement.clone(),
            input_tables: get_input_tables(&statement).unwrap(),
            join_tables: HashMap::new(),
        }
    }

    fn init_store(&mut self, env: &mut dyn Environment) -> Result<(), PipelineError> {
        self.join_tables = build_join_chain(&self.statement, env).unwrap();

        Ok(())
    }

    fn delete(
        &self,
        _from_port: PortHandle,
        record: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Operation {
        Operation::Delete {
            old: record.clone(),
        }
    }

    fn insert(
        &self,
        _from_port: PortHandle,
        record: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Operation {
        Operation::Insert {
            new: record.clone(),
        }
    }

    fn update(
        &self,
        _from_port: PortHandle,
        old: &Record,
        new: &Record,
        _txn: &mut dyn RwTransaction,
        _reader: &HashMap<PortHandle, RecordReader>,
    ) -> Operation {
        Operation::Update {
            old: old.clone(),
            new: new.clone(),
        }
    }

    fn get_output_schema(
        &self,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let mut output_schema = Schema::empty();
        for (port, table) in self.input_tables.iter().enumerate() {
            if let Some(current_schema) = input_schemas.get(&(port as PortHandle)) {
                output_schema = append_schema(output_schema, table, current_schema);
            } else {
                return Err(ExecutionError::InvalidPortHandle(port as PortHandle));
            }
        }

        Ok(output_schema)
    }
}

fn append_schema(mut output_schema: Schema, table: &String, current_schema: &Schema) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        let mut name = String::from(table);
        name.push('.');
        name.push_str(&field.name);
        field.name = name;
        output_schema.fields.push(field);
    }

    output_schema
}

impl StatefulProcessor for ProductProcessor {
    fn update_schema(
        &mut self,
        _output_port: PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, ExecutionError> {
        let output_schema = self.get_output_schema(input_schemas)?;
        Ok(output_schema)
    }

    fn init(&mut self, state: &mut dyn Environment) -> Result<(), ExecutionError> {
        internal_err!(self.init_store(state))
    }

    fn process(
        &mut self,
        from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<(), ExecutionError> {
        match op {
            Operation::Delete { ref old } => {
                let _ = fw.send(
                    self.delete(from_port, old, txn, reader),
                    DEFAULT_PORT_HANDLE,
                );
            }
            Operation::Insert { ref new } => {
                let _ = fw.send(
                    self.insert(from_port, new, txn, reader),
                    DEFAULT_PORT_HANDLE,
                );
            }
            Operation::Update { ref old, ref new } => {
                let _ = fw.send(
                    self.update(from_port, old, new, txn, reader),
                    DEFAULT_PORT_HANDLE,
                );
            }
        }
        Ok(())
    }
}
