use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::Schema;

use crate::pipeline::builder::IndexedTableWithJoins;
use crate::pipeline::expression::builder::NameOrAlias;
use crate::pipeline::{builder::SchemaSQLContext, expression::builder::extend_schema_source_def};

use super::processor::TableProcessor;

#[derive(Debug)]
pub struct TableProcessorFactory {
    input_tables: IndexedTableWithJoins,
}

impl TableProcessorFactory {
    pub fn new(input_tables: IndexedTableWithJoins) -> Self {
        Self { input_tables }
    }
}

impl ProcessorFactory<SchemaSQLContext> for TableProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        vec![OutputPortDef::new(
            DEFAULT_PORT_HANDLE,
            OutputPortType::Stateless,
        )]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        if let Some((input_schema, query_context)) = input_schemas.get(&DEFAULT_PORT_HANDLE) {
            let input_names = get_input_names(&self.input_tables);
            if input_names.len() != 1 {
                return Err(ExecutionError::InternalError(
                    "Invalid Input".to_string().into(),
                ));
            }
            let table = input_names[0].clone();

            let extended_input_schema = extend_schema_source_def(input_schema, &table);
            Ok((extended_input_schema, query_context.clone()))
        } else {
            Err(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))
        }
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _txn: &mut LmdbExclusiveTransaction,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        Ok(Box::new(TableProcessor::new()))
    }
}

pub fn get_input_names(input_tables: &IndexedTableWithJoins) -> Vec<NameOrAlias> {
    let mut input_names = vec![];
    input_names.push(input_tables.relation.0.clone());

    for join in &input_tables.joins {
        input_names.push(join.0.clone());
    }
    input_names
}
