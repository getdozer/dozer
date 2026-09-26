use crate::planner::projection::CommonPlanner;
use crate::projection::processor::ProjectionProcessor;
use crate::selection::replay::ReplayRequirement;
use crate::{aggregation::processor::AggregationProcessor, errors::PipelineError};
use dozer_core::event::EventHub;
use dozer_core::{
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::execution::Expression;
use dozer_sql_expression::sqlparser::ast::{Expr, SelectItem};
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::udf_config::UdfConfig;
use dozer_types::parking_lot::Mutex;
use dozer_types::tonic::async_trait;
use dozer_types::types::Schema;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[derive(Debug)]
pub struct AggregationProcessorFactory {
    id: String,
    projection: Vec<SelectItem>,
    group_by: Vec<Expr>,
    having: Option<Expr>,
    enable_probabilistic_optimizations: bool,
    udfs: Vec<UdfConfig>,
    runtime: Arc<Runtime>,
    replay: ReplayRequirement,

    /// Type name can only be determined after schema propagation.
    type_name: Mutex<Option<String>>,
}

impl AggregationProcessorFactory {
    pub fn new(
        id: String,
        projection: Vec<SelectItem>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        enable_probabilistic_optimizations: bool,
        udfs: Vec<UdfConfig>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            id,
            projection,
            group_by,
            having,
            enable_probabilistic_optimizations,
            udfs,
            runtime,
            replay: ReplayRequirement::default(),
            type_name: Mutex::new(None),
        }
    }

    pub(crate) fn replay_requirement(&self) -> ReplayRequirement {
        self.replay.clone()
    }

    async fn get_planner(&self, input_schema: Schema) -> Result<CommonPlanner, PipelineError> {
        let mut projection_planner =
            CommonPlanner::new(input_schema, self.udfs.as_slice(), self.runtime.clone());
        projection_planner
            .plan(
                self.projection.clone(),
                self.group_by.clone(),
                self.having.clone(),
            )
            .await?;
        if self.replay.is_required()
            && !projection_planner.aggregation_output.is_empty()
            && projection_planner.groupby.is_empty()
        {
            // The existing aggregate processor emits no row for empty input;
            // global COUNT/SUM cannot supply SQL's empty-set row to membership.
            return Err(PipelineError::InvalidQuery(
                "IN subqueries do not support aggregates without GROUP BY".into(),
            ));
        }
        for expression in projection_planner
            .projection_output
            .iter()
            .chain(projection_planner.groupby.iter())
            .chain(projection_planner.having.iter())
        {
            self.replay.validate(expression)?;
        }
        for aggregate in &projection_planner.aggregation_output {
            if let Expression::AggregateFunction { args, .. } = aggregate {
                for argument in args {
                    self.replay.validate(argument)?;
                }
            } else {
                self.replay.validate(aggregate)?;
            }
        }
        Ok(projection_planner)
    }
}

#[async_trait]
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

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone()).await?;

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

    async fn build(
        &self,
        input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let input_schema = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;

        let planner = self.get_planner(input_schema.clone()).await?;

        let processor: Box<dyn Processor> = if is_projection(&planner) {
            Box::new(ProjectionProcessor::new(
                input_schema.clone(),
                planner.projection_output,
            )?)
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

fn is_projection(planner: &CommonPlanner) -> bool {
    planner.aggregation_output.is_empty() && planner.groupby.is_empty()
}
