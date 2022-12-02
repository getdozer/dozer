use crate::pipeline::errors::PipelineError;
use dozer_core::dag::channels::ProcessorChannelForwarder;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::{PortHandle, Processor};
use dozer_core::dag::record_store::RecordReader;
use dozer_core::storage::common::{Database, Environment, RwTransaction};
use dozer_types::internal_err;
use dozer_types::types::{Operation, Record, Schema};
use sqlparser::ast::TableWithJoins;
use std::collections::HashMap;

use dozer_core::dag::errors::ExecutionError::InternalError;

use super::factory::{build_join_chain, get_input_tables};
use super::join::{JoinExecutor, JoinTable};

/// Cartesian Product Processor
pub struct ProductProcessor {
    /// Parsed FROM Statement
    statement: TableWithJoins,

    /// List of input ports by table name
    input_tables: Vec<String>,

    /// Join operations
    join_tables: HashMap<PortHandle, JoinTable>,

    /// Database to store Join indexes
    db: Option<Database>,
}

impl ProductProcessor {
    /// Creates a new [`ProductProcessor`].
    pub fn new(statement: TableWithJoins) -> Self {
        Self {
            statement: statement.clone(),
            input_tables: get_input_tables(&statement).unwrap(),
            join_tables: HashMap::new(),
            db: None,
        }
    }

    fn init_store(&mut self, env: &mut dyn Environment) -> Result<(), PipelineError> {
        self.db = Some(env.open_database("product", true)?);
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
        from_port: PortHandle,
        record: &Record,
        txn: &mut dyn RwTransaction,
        reader: &HashMap<PortHandle, RecordReader>,
    ) -> Result<Vec<Record>, ExecutionError> {
        if let Some(input_table) = self.join_tables.get(&from_port) {
            let mut output_records = vec![record.clone()];

            if let Some(database) = &self.db {
                if let Some(left_join) = &input_table.left {
                    output_records = left_join.execute(
                        output_records,
                        database,
                        txn,
                        reader,
                        &self.join_tables,
                    )?;
                }

                if let Some(right_join) = &input_table.right {
                    output_records = right_join.execute(
                        output_records,
                        database,
                        txn,
                        reader,
                        &self.join_tables,
                    )?;
                }
            }

            return Ok(output_records);
        }

        Err(ExecutionError::MissingNodeInput(format!(
            "Cannot load data from Port {}",
            from_port
        )))
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
        &mut self,
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

        if let Ok(join_tables) = build_join_chain(&self.statement) {
            self.join_tables = join_tables;
        } else {
            return Err(ExecutionError::InvalidOperation(
                "Unable to build Join".to_string(),
            ));
        }

        Ok(output_schema)
    }

    // fn merge(&self, _left_records: &[Record], _right_records: &[Record]) -> Vec<Record> {
    //     todo!()
    // }
}

fn append_schema(mut output_schema: Schema, _table: &str, current_schema: &Schema) -> Schema {
    for mut field in current_schema.clone().fields.into_iter() {
        let mut name = String::from(""); //String::from(table);
                                         //name.push('.');
        name.push_str(&field.name);
        field.name = name;
        output_schema.fields.push(field);
    }

    output_schema
}

impl Processor for ProductProcessor {
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

    fn commit(&self, _tx: &mut dyn RwTransaction) -> Result<(), ExecutionError> {
        Ok(())
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
                let records = self.insert(from_port, new, txn, reader)?;

                for record in records.into_iter() {
                    let _ = fw.send(Operation::Insert { new: record }, DEFAULT_PORT_HANDLE);
                }
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
