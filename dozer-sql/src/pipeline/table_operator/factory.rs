use std::collections::HashMap;

use dozer_core::{
    errors::ExecutionError,
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::types::{DozerDuration, Schema, TimeUnit};

use crate::pipeline::{
    builder::SchemaSQLContext, errors::TableOperatorError,
    pipeline_builder::from_builder::TableOperatorDescriptor,
    window::builder::string_from_sql_object_name,
};

use super::{
    lifetime::LifetimeTableOperator,
    operator::{TableOperator, TableOperatorType},
    processor::TableProcessor,
};

#[derive(Debug)]
pub struct TableProcessorFactory {
    table: TableOperatorDescriptor,
}

impl TableProcessorFactory {
    pub fn new(table: TableOperatorDescriptor) -> Self {
        Self { table }
    }
}

impl ProcessorFactory<SchemaSQLContext> for TableProcessorFactory {
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
        let (input_schema, _) = input_schemas
            .get(&DEFAULT_PORT_HANDLE)
            .ok_or(ExecutionError::InternalError(
                "Invalid Table Operator".to_string().into(),
            ))?
            .clone();

        let output_schema = match operator_from_descriptor(&self.table, &input_schema)
            .map_err(|e| ExecutionError::TableProcessorError(Box::new(e)))?
        {
            Some(operator) => operator
                .get_output_schema(&input_schema)
                .map_err(|e| ExecutionError::WindowProcessorFactoryError(Box::new(e)))?,
            None => {
                return Err(ExecutionError::TableProcessorError(Box::new(
                    TableOperatorError::InternalError("Invalid Table Operator".into()),
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

        match operator_from_descriptor(&self.table, &input_schema)
            .map_err(|e| ExecutionError::TableProcessorError(Box::new(e)))?
        {
            Some(operator) => Ok(Box::new(TableProcessor::new(operator))),
            None => Err(ExecutionError::TableProcessorError(Box::new(
                TableOperatorError::InternalError("Invalid Table Operator".into()),
            ))),
        }
    }
}

pub(crate) fn operator_from_descriptor(
    descriptor: &TableOperatorDescriptor,
    _schema: &Schema,
) -> Result<Option<TableOperatorType>, ExecutionError> {
    let function_name = string_from_sql_object_name(&descriptor.name);

    if function_name.to_uppercase() == "TTL" {
        let operator = LifetimeTableOperator::new(
            None,
            DozerDuration(
                std::time::Duration::from_nanos(0_u64),
                TimeUnit::Nanoseconds,
            ),
        );

        Ok(Some(operator.into()))
    } else {
        Err(ExecutionError::InternalError(function_name.into()))
    }
}
