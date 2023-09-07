use std::collections::HashMap;

use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::SetError;

use dozer_core::processor_record::ProcessorRecordStore;
use dozer_core::{
    node::{OutputPortDef, OutputPortType, PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_types::errors::internal::BoxedError;
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition};
use sqlparser::ast::{SetOperator, SetQuantifier};

use super::operator::SetOperation;
use super::set_processor::SetProcessor;

#[derive(Debug)]
pub struct SetProcessorFactory {
    id: String,
    set_quantifier: SetQuantifier,
    enable_probabilistic_optimizations: bool,
}

impl SetProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(
        id: String,
        set_quantifier: SetQuantifier,
        enable_probabilistic_optimizations: bool,
    ) -> Self {
        Self {
            id,
            set_quantifier,
            enable_probabilistic_optimizations,
        }
    }
}

impl ProcessorFactory for SetProcessorFactory {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn type_name(&self) -> String {
        "Set".to_string()
    }
    fn get_input_ports(&self) -> Vec<PortHandle> {
        vec![0, 1]
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
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let output_columns = validate_set_operation_input_schemas(input_schemas)?;

        let output_schema = Schema {
            fields: output_columns,
            primary_index: input_schemas
                .get(&0)
                .map_or(Err(SetError::InvalidInputSchemas), Ok)
                .unwrap()
                .to_owned()
                .primary_index,
        };

        Ok(output_schema)
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _record_store: &ProcessorRecordStore,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(SetProcessor::new(
            self.id.clone(),
            SetOperation {
                op: SetOperator::Union,
                quantifier: self.set_quantifier,
            },
            self.enable_probabilistic_optimizations,
        )?))
    }
}

fn validate_set_operation_input_schemas(
    input_schemas: &HashMap<PortHandle, Schema>,
) -> Result<Vec<FieldDefinition>, PipelineError> {
    let mut left_columns = input_schemas
        .get(&0)
        .map_or(Err(SetError::InvalidInputSchemas), Ok)
        .unwrap()
        .to_owned()
        .fields;
    let mut right_columns = input_schemas
        .get(&1)
        .map_or(Err(SetError::InvalidInputSchemas), Ok)
        .unwrap()
        .to_owned()
        .fields;

    left_columns.sort();
    right_columns.sort();

    let mut output_fields = Vec::new();
    for (left, right) in left_columns.iter().zip(right_columns.iter()) {
        if !is_similar_fields(left, right) {
            return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
        }
        output_fields.push(FieldDefinition::new(
            left.name.clone(),
            left.typ,
            left.nullable,
            SourceDefinition::Dynamic,
        ));
    }
    Ok(output_fields)
}

fn is_similar_fields(left: &FieldDefinition, right: &FieldDefinition) -> bool {
    left.name == right.name && left.typ == right.typ && left.nullable == right.nullable
}
