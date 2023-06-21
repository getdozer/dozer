use std::collections::HashMap;

use crate::pipeline::builder::SchemaSQLContext;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::SetError;

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
    set_quantifier: SetQuantifier,
}

impl SetProcessorFactory {
    /// Creates a new [`FromProcessorFactory`].
    pub fn new(set_quantifier: SetQuantifier) -> Self {
        Self { set_quantifier }
    }
}

impl ProcessorFactory<SchemaSQLContext> for SetProcessorFactory {
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
        input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
    ) -> Result<(Schema, SchemaSQLContext), BoxedError> {
        let output_columns = validate_set_operation_input_schemas(input_schemas)?;

        let mut output_schema = Schema::empty();
        output_schema.fields = output_columns;
        output_schema.identifier = input_schemas
            .get(&0)
            .map_or(Err(SetError::InvalidInputSchemas), Ok)
            .unwrap()
            .to_owned()
            .0
            .identifier;
        output_schema.primary_index = input_schemas
            .get(&0)
            .map_or(Err(SetError::InvalidInputSchemas), Ok)
            .unwrap()
            .to_owned()
            .0
            .primary_index;

        Ok((output_schema, SchemaSQLContext::default()))
    }

    fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
    ) -> Result<Box<dyn Processor>, BoxedError> {
        Ok(Box::new(SetProcessor::new(SetOperation {
            op: SetOperator::Union,
            quantifier: self.set_quantifier,
        })?))
    }
}

fn validate_set_operation_input_schemas(
    input_schemas: &HashMap<PortHandle, (Schema, SchemaSQLContext)>,
) -> Result<Vec<FieldDefinition>, PipelineError> {
    let mut left_columns = input_schemas
        .get(&0)
        .map_or(Err(SetError::InvalidInputSchemas), Ok)
        .unwrap()
        .to_owned()
        .0
        .fields;
    let mut right_columns = input_schemas
        .get(&1)
        .map_or(Err(SetError::InvalidInputSchemas), Ok)
        .unwrap()
        .to_owned()
        .0
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
