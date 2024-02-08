use std::collections::HashMap;

use dozer_core::{
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::{
    builder::{extend_schema_source_def, NameOrAlias},
    sqlparser::ast::TableFactor,
};
use dozer_types::{errors::internal::BoxedError, tonic::async_trait, types::Schema};

use crate::errors::{PipelineError, ProductError};
use crate::window::builder::string_from_sql_object_name;

use super::processor::TableProcessor;

#[derive(Debug)]
pub struct TableProcessorFactory {
    id: String,
    relation: TableFactor,
}

impl TableProcessorFactory {
    pub fn new(id: String, relation: TableFactor) -> Self {
        Self { id, relation }
    }
}

#[async_trait]
impl ProcessorFactory for TableProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Table".to_string()
    }

    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        if let Some(input_schema) = input_schemas.get(&DEFAULT_PORT_HANDLE) {
            let table = get_name_or_alias(&self.relation)?;
            let extended_input_schema = extend_schema_source_def(input_schema, &table);
            Ok(extended_input_schema)
        } else {
            Err(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE).into())
        }
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(TableProcessor::new(
            self.id.clone(),
            checkpoint_data,
        )))
    }
}

pub fn get_name_or_alias(relation: &TableFactor) -> Result<NameOrAlias, PipelineError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let table_name = string_from_sql_object_name(name);
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias(table_name, Some(alias)));
            }
            Ok(NameOrAlias(table_name, None))
        }
        TableFactor::Derived { alias, .. } => {
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias("dozer_derived".to_string(), Some(alias)));
            }
            Ok(NameOrAlias("dozer_derived".to_string(), None))
        }
        TableFactor::TableFunction { .. } => Err(PipelineError::ProductError(
            ProductError::UnsupportedTableFunction,
        )),
        TableFactor::UNNEST { .. } => {
            Err(PipelineError::ProductError(ProductError::UnsupportedUnnest))
        }
        TableFactor::NestedJoin { alias, .. } => {
            if let Some(table_alias) = alias {
                let alias = table_alias.name.value.clone();
                return Ok(NameOrAlias("dozer_nested".to_string(), Some(alias)));
            }
            Ok(NameOrAlias("dozer_nested".to_string(), None))
        }
        TableFactor::Pivot { .. } => {
            Err(PipelineError::ProductError(ProductError::UnsupportedPivot))
        }
    }
}
