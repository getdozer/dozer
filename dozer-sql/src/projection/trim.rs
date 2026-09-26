use std::collections::HashMap;

use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::execution::Expression;
use dozer_types::{errors::internal::BoxedError, tonic::async_trait, types::Schema};

use crate::errors::PipelineError;

use super::processor::ProjectionProcessor;

/// Restore the input schema before wildcard expansion or aggregation.
#[derive(Debug)]
pub(crate) struct TrimMembershipColumns {
    pub id: String,
    pub columns: Vec<String>,
}

impl TrimMembershipColumns {
    fn width(&self, schema: &Schema) -> Result<usize, PipelineError> {
        let width = schema.fields.len().checked_sub(self.columns.len());
        match width {
            Some(width)
                if schema.fields[width..]
                    .iter()
                    .map(|field| &field.name)
                    .eq(self.columns.iter()) =>
            {
                Ok(width)
            }
            _ => Err(PipelineError::InvalidQuery(
                "IN membership columns do not match the planned schema".into(),
            )),
        }
    }
}

#[async_trait]
impl ProcessorFactory for TrimMembershipColumns {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "MembershipProjection".into()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _port: &PortHandle,
        inputs: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let mut schema = inputs
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?
            .clone();
        let width = self.width(&schema)?;
        schema.fields.truncate(width);
        Ok(schema)
    }

    async fn build(
        &self,
        inputs: HashMap<PortHandle, Schema>,
        _outputs: HashMap<PortHandle, Schema>,
        _events: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = inputs
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        let expressions = (0..self.width(schema)?)
            .map(|index| Expression::Column { index })
            .collect();
        Ok(Box::new(ProjectionProcessor::new(
            schema.clone(),
            expressions,
        )?))
    }
}
