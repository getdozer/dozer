use crate::planner::projection::CommonPlanner;
use crate::projection::processor::ProjectionProcessor;
use crate::{aggregation::processor::AggregationProcessor, errors::PipelineError};
use dozer_core::{
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_recordstore::ProcessorRecordStoreDeserializer;
use dozer_sql_expression::sqlparser::ast::Select;
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::parking_lot::Mutex;
use dozer_types::types::Schema;
use std::collections::HashMap;

#[derive(Debug)]
pub struct AggregationProcessorFactory {
    id: String,
    projection: Select,
    _stateful: bool,
    enable_probabilistic_optimizations: bool,
    udfs: Vec<UdfConfig>,

    /// Type name can only be determined after schema propagation.
    type_name: Mutex<Option<String>>,
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
            type_name: Mutex::new(None),
        }
    }

    fn get_planner(&self, input_schema: Schema) -> Result<CommonPlanner, PipelineError> {
        let mut projection_planner = CommonPlanner::new(input_schema, self.udfs.as_slice());
        projection_planner.plan(self.projection.clone())?;
        Ok(projection_planner)
    }
}

impl ProcessorFactory for AggregationProcessorFactory {
    fn type_name(&self) -> String {
        self.type_name
            .lock()
            .as_deref()
            .unwrap_or("Aggregation")
            .to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone())?;

        *self.type_name.lock() = Some(
            if is_projection(&planner) {
                "Projection"
            } else {
                "Aggregation"
            }
            .to_string(),
        );

        Ok(planner.post_projection_schema)
    }

    fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStoreDeserializer,
        checkpoint_data: Option<Vec<u8>>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone())?;

        let processor: Box<dyn Processor> = if is_projection(&planner) {
            Box::new(ProjectionProcessor::new(
                input_schema.clone(),
                planner.projection_output,
                checkpoint_data,
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
                checkpoint_data,
            )?)
        };
        Ok(processor)
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn is_projection(planner: &CommonPlanner) -> bool {
    planner.aggregation_output.is_empty() && planner.groupby.is_empty()
}
