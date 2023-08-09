use std::collections::HashMap;

use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    processor_record::ProcessorRecordStore,
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{errors::internal::BoxedError, types::Schema};

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
    id: String,
    table: TableOperatorDescriptor,
}

impl WindowProcessorFactory {
    pub fn new(id: String, table: TableOperatorDescriptor) -> Self {
        Self { id, table }
    }

    pub(crate) fn get_source_name(&self) -> Result<String, PipelineError> {
        window_source_name(&self.table).map_err(PipelineError::WindowError)
    }
}

impl ProcessorFactory<SchemaSQLContext> for WindowProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Window".to_string()
    }

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
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        let output_schema = match window_from_table_operator(&self.table, &input_schema.0)
            .map_err(PipelineError::WindowError)?
        {
            Some(window) => window
                .get_output_schema(&input_schema.0)
                .map_err(PipelineError::WindowError)?,
            None => return Err(PipelineError::WindowError(WindowError::InvalidWindow()).into()),
        };

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        match window_from_table_operator(&self.table, &input_schema)
            .map_err(PipelineError::WindowError)?
        {
            Some(window) => Ok(Box::new(WindowProcessor::new(self.id.clone(), window))),
            None => Err(PipelineError::WindowError(WindowError::InvalidWindow()).into()),
        }
    }
}
