use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::Schema;

use crate::pipeline::{
    builder::SchemaSQLContext,
    errors::{PipelineError, WindowError},
    pipeline_builder::from_builder::TableOperatorDescriptor,
};

use super::{
    builder::{window_from_table_operator, window_source_name},
    processor::WindowProcessor,
};

#[derive(Debug)]
pub struct WindowProcessorFactory {
    table: TableOperatorDescriptor,
}

impl WindowProcessorFactory {
    pub fn new(table: TableOperatorDescriptor) -> Self {
        Self { table }
    }

    pub(crate) fn get_source_name(&self) -> Result<String, PipelineError> {
        window_source_name(&self.table).map_err(PipelineError::WindowError)
    }
}

impl ProcessorFactory<SchemaSQLContext> for WindowProcessorFactory {
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
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        let output_schema = match window_from_table_operator(&self.table, &input_schema.0)
            .map_err(|e| ExecutionError::WindowProcessorFactoryError(Box::new(e)))?
        {
            Some(window) => window
                .get_output_schema(&input_schema.0)
                .map_err(|e| ExecutionError::WindowProcessorFactoryError(Box::new(e)))?,
            None => {
                return Err(ExecutionError::WindowProcessorFactoryError(Box::new(
                    WindowError::InvalidWindow(),
                )))
            }
        };

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        match window_from_table_operator(&self.table, &input_schema)
            .map_err(|e| ExecutionError::WindowProcessorFactoryError(Box::new(e)))?
        {
            Some(window) => Ok(Box::new(WindowProcessor::new(window))),
            None => Err(ExecutionError::WindowProcessorFactoryError(Box::new(
                WindowError::InvalidWindow(),
            ))),
        }
    }
}
