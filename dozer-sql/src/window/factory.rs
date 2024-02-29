use std::collections::HashMap;

use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{errors::internal::BoxedError, tonic::async_trait, types::Schema};

use crate::{
    builder::TableOperatorDescriptor,
    errors::{PipelineError, WindowError},
};

use super::{builder::window_from_table_operator, processor::WindowProcessor};

#[derive(Debug)]
pub struct WindowProcessorFactory {
    id: String,
    table: TableOperatorDescriptor,
}

impl WindowProcessorFactory {
    pub fn new(id: String, table: TableOperatorDescriptor) -> Self {
        Self { id, table }
    }
}

#[async_trait]
impl ProcessorFactory for WindowProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Window".to_string()
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
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InternalError(
                "Invalid Window".to_string().into(),
            ))?
            .clone();

        let output_schema = match window_from_table_operator(&self.table, &input_schema)
            .map_err(PipelineError::WindowError)?
        {
            Some(window) => window
                .get_output_schema(&input_schema)
                .map_err(PipelineError::WindowError)?,
            None => return Err(PipelineError::WindowError(WindowError::InvalidWindow()).into()),
        };

        Ok(output_schema)
    }

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _output_schemas: HashMap<PortHandle, dozer_types::types::Schema>,
        _event_hub: EventHub,
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
