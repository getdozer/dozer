use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    storage::lmdb_storage::LmdbExclusiveTransaction,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::Schema;
use sqlparser::ast::TableFactor;

use crate::pipeline::{builder::SchemaSQLContext, expression::builder::extend_schema_source_def};
use crate::pipeline::{
    expression::builder::NameOrAlias, window::builder::string_from_sql_object_name,
};

use super::processor::TableProcessor;

#[derive(Debug)]
pub struct TableProcessorFactory {
    relation: TableFactor,
}

impl TableProcessorFactory {
    pub fn new(relation: TableFactor) -> Self {
        Self { relation }
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
            let table = get_name_or_alias(&self.relation)?;
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

pub fn get_name_or_alias(relation: &TableFactor) -> Result<NameOrAlias, ExecutionError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = string_from_sql_object_name(name);
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias(table_name, Some(alias)));
            }
            Ok(NameOrAlias(table_name, None))
        }
        _ => todo!(),
    }
}
