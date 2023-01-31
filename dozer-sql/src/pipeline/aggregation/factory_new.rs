use crate::pipeline::aggregation::processor_new::AggregationProcessor;
use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::Expression;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory,
};
use dozer_types::types::Schema;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AggregationProcessorFactory {
    dimensions: Vec<Expression>,
    measures: Vec<Expression>,
    projections: Vec<Expression>,
    input_schema: Schema,
    aggregation_schema: Schema,
    projection_schema: Schema,
    stateful: bool,
}

impl AggregationProcessorFactory {
    pub fn new(
        dimensions: Vec<Expression>,
        measures: Vec<Expression>,
        projections: Vec<Expression>,
        input_schema: Schema,
        aggregation_schema: Schema,
        projection_schema: Schema,
        stateful: bool,
    ) -> Self {
        Self {
            dimensions,
            measures,
            projections,
            input_schema,
            aggregation_schema,
            projection_schema,
            stateful,
        }
    }
}

impl ProcessorFactory<SchemaSQLContext> for AggregationProcessorFactory {
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<OutputPortDef> {
        if self.stateful {
            vec![OutputPortDef::new(
                DEFAULT_PORT_HANDLE,
                OutputPortType::StatefulWithPrimaryKeyLookup {
                    retr_old_records_for_deletes: true,
                    retr_old_records_for_updates: true,
                },
            )]
        } else {
            vec![OutputPortDef::new(
                DEFAULT_PORT_HANDLE,
                OutputPortType::Stateless,
            )]
        }
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), ExecutionError> {
        let (input_schema, ctx) = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        Ok((self.projection_schema.clone(), ctx.clone()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let processor = AggregationProcessor::new(
            self.dimensions.clone(),
            self.measures.clone(),
            self.projections.clone(),
            input_schema.clone(),
            self.aggregation_schema.clone(),
        )
        .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;

        Ok(Box::new(processor))
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
