use crate::pipeline::aggregation::processor::AggregationProcessor;
use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::planner::projection::CommonPlanner;
use crate::pipeline::projection::processor::ProjectionProcessor;
use dozer_core::dag::errors::ExecutionError;
use dozer_core::dag::node::{
    OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory,
};
use dozer_core::dag::DEFAULT_PORT_HANDLE;
use dozer_types::types::Schema;
use sqlparser::ast::Select;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AggregationProcessorFactory {
    projection: Select,
    stateful: bool,
}

impl AggregationProcessorFactory {
    pub fn new(projection: Select, stateful: bool) -> Self {
        Self {
            projection,
            stateful,
        }
    }

    fn get_planner(&self, input_schema: Schema) -> Result<CommonPlanner, ExecutionError> {
        let mut projection_planner = CommonPlanner::new(input_schema);
        projection_planner
            .plan(self.projection.clone())
            .map_err(|e| ExecutionError::InternalError(Box::new(e)))?;
        Ok(projection_planner)
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

        let planner = self.get_planner(input_schema.clone())?;
        Ok((planner.post_projection_schema, ctx.clone()))
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, ExecutionError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone())?;

        let processor: Box<dyn Processor> = match planner.aggregation_output.len() {
            0 => Box::new(ProjectionProcessor::new(
                input_schema.clone(),
                planner.projection_output,
            )),
            _ => Box::new(
                AggregationProcessor::new(
                    planner.groupby,
                    planner.aggregation_output,
                    planner.projection_output,
                    input_schema.clone(),
                    planner.post_aggregation_schema.clone(),
                )
                .map_err(|e| ExecutionError::InternalError(Box::new(e)))?,
            ),
        };

        Ok(processor)
    }

    fn prepare(
        &self,
        _input_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
        _output_schemas: HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(), ExecutionError> {
        Ok(())
    }
}
