//! Streaming `WHERE value IN (SELECT ...)` and `NOT IN`.
//!
//! The outer stream and each uncorrelated subquery feed a stateful selection
//! processor. Inner duplicates are reference-counted; an outer row is emitted or
//! retracted only when its predicate changes selectedness. Predicates use SQL
//! three-valued logic: an empty subquery makes `IN` false, a match is true, and an
//! unmatched comparison involving NULL is unknown. Only true selects a row.
//!
//! The subquery must be written as `IN (SELECT ...)` (or `IN (WITH ... SELECT ...)`).
//! Correlated or lateral subqueries, row-valued membership and recursive CTEs are
//! rejected. State is in memory. Membership lookup hashes a canonical form of the
//! value (all integer widths, and string/text, share a key); only candidates of a
//! different type class fall back to the generic equality operator.
//!
//! `tests/dynamic_in.rs` runs the SQL builder, DAG and a collecting sink end to end.
use std::{collections::HashMap, sync::Arc};

use dozer_core::{
    event::EventHub,
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::{
    errors::internal::BoxedError, models::udf_config::UdfConfig, tonic::async_trait, types::Schema,
};
use tokio::runtime::Runtime;

use crate::errors::PipelineError;
use plan::{require_boolean, Plan};
use processor::DynamicSelectionProcessor;

pub(crate) mod plan;
mod processor;

pub(crate) fn membership_port(index: usize) -> Result<PortHandle, PipelineError> {
    index
        .checked_add(1)
        .and_then(|port| PortHandle::try_from(port).ok())
        .filter(|port| *port != DEFAULT_PORT_HANDLE)
        .ok_or_else(|| PipelineError::InvalidQuery("Too many IN subqueries in one filter".into()))
}

#[derive(Debug)]
pub(crate) struct DynamicSelectionFactory {
    pub id: String,
    pub plan: Plan,
    pub input_count: PortHandle,
    pub udfs: Vec<UdfConfig>,
    pub runtime: Arc<Runtime>,
}

#[async_trait]
impl ProcessorFactory for DynamicSelectionFactory {
    fn id(&self) -> String {
        self.id.clone()
    }
    fn type_name(&self) -> String {
        "DynamicSelection".into()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        std::iter::once(DEFAULT_PORT_HANDLE)
            .chain((1..=self.input_count).filter(|port| *port != DEFAULT_PORT_HANDLE))
            .collect()
    }
    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        output: &PortHandle,
        inputs: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        if *output != DEFAULT_PORT_HANDLE {
            return Err(PipelineError::InvalidPortHandle(*output).into());
        }
        if self.input_count == DEFAULT_PORT_HANDLE {
            return Err(
                PipelineError::InvalidQuery("Too many IN subqueries in one filter".into()).into(),
            );
        }
        if let Some(port) = inputs
            .keys()
            .find(|port| **port != DEFAULT_PORT_HANDLE && !(1..=self.input_count).contains(port))
        {
            return Err(PipelineError::InvalidPortHandle(*port).into());
        }
        let schema = inputs
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(PipelineError::InvalidPortHandle(DEFAULT_PORT_HANDLE))?;
        for port in 1..=self.input_count {
            let inner = inputs
                .get(&port)
                .ok_or(PipelineError::InvalidPortHandle(port))?;
            if inner.fields.len() != 1 {
                return Err(PipelineError::InvalidQuery(
                    "IN subquery must return exactly one column".into(),
                )
                .into());
            }
        }
        Ok(schema.clone())
    }

    async fn build(
        &self,
        inputs: HashMap<PortHandle, Schema>,
        _: HashMap<PortHandle, Schema>,
        _: EventHub,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        let schema = self
            .get_output_schema(&DEFAULT_PORT_HANDLE, &inputs)
            .await?;
        let predicate = self
            .plan
            .compile(&schema, &inputs, &self.udfs, self.runtime.clone())
            .await?;
        require_boolean(&predicate)?;
        Ok(Box::new(DynamicSelectionProcessor::new(
            predicate.predicate,
            schema,
            usize::from(self.input_count),
        )))
    }
}

#[cfg(test)]
mod tests;
