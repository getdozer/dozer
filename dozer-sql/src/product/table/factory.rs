use std::collections::HashMap;

use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::builder::{extend_schema_source_def, NameOrAlias};
use dozer_types::{errors::internal::BoxedError, tonic::async_trait, types::Schema};

use crate::errors::PipelineError;

use super::processor::TableProcessor;

#[derive(Debug)]
pub struct TableProcessorFactory {
    id: String,
    table: NameOrAlias,
}

impl TableProcessorFactory {
    pub fn new(id: String, table: NameOrAlias) -> Self {
        Self { id, table }
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
            let extended_input_schema = extend_schema_source_def(input_schema, &self.table);
            Ok(extended_input_schema)
        } else {
            Err(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE).into())
        }
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(TableProcessor::new(self.id.clone())))
    }
}
