use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::planner::projection::CommonPlanner;
use crate::pipeline::projection::processor::ProjectionProcessor;
use crate::pipeline::{aggregation::processor::AggregationProcessor, errors::PipelineError};
use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::types::Schema;
use sqlparser::ast::Select;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AggregationProcessorFactory {
    id: String,
    projection: Select,
    _stateful: bool,
    enable_probabilistic_optimizations: bool,
    udfs: Vec<UdfConfig>,
}

impl AggregationProcessorFactory {
    pub fn new(
        id: String,
        projection: Select,
        stateful: bool,
        enable_probabilistic_optimizations: bool,
        udfs: Vec<UdfConfig>,
    ) -> Self {
        Self {
            id,
            projection,
            _stateful: stateful,
            enable_probabilistic_optimizations,
            udfs,
        }
    }

    fn get_planner(&self, input_schema: Schema) -> Result<CommonPlanner, PipelineError> {
        let mut projection_planner = CommonPlanner::new(input_schema, self.udfs.clone());
        projection_planner.plan(self.projection.clone())?;
        Ok(projection_planner)
    }
}

impl ProcessorFactory<SchemaSQLContext> for AggregationProcessorFactory {
    fn type_name(&self) -> String {
        "Aggregation".to_string()
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
        let (input_schema, ctx) = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone())?;
        Ok((planner.post_projection_schema, ctx.clone()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone())?;

        let is_projection = planner.aggregation_output.is_empty() && planner.groupby.is_empty();
        let processor: Box<dyn Processor> = if is_projection {
            Box::new(ProjectionProcessor::new(
                input_schema.clone(),
                planner.projection_output,
            ))
        } else {
            Box::new(AggregationProcessor::new(
                self.id.clone(),
                planner.groupby,
                planner.aggregation_output,
                planner.projection_output,
                planner.having,
                input_schema.clone(),
                planner.post_aggregation_schema,
                self.enable_probabilistic_optimizations,
            )?)
        };
        Ok(processor)
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}
