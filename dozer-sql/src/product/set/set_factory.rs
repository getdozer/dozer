use std::collections::HashMap;

use crate::errors::PipelineError;
use crate::errors::SetError;

use dozer_core::event::EventHub;
use dozer_core::{
    node::{PortHandle, Processor, ProcessorFactory},
    DEFAULT_PORT_HANDLE,
};
use dozer_sql_expression::sqlparser::ast::{SetOperator, SetQuantifier};
use dozer_types::errors::internal::BoxedError;
use dozer_types::tonic::async_trait;
use dozer_types::types::{FieldDefinition, Schema, SourceDefinition};

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

#[async_trait]
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

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        let output_columns = validate_set_operation_input_schemas(input_schemas)?;

        let output_schema = Schema {
            fields: output_columns,
            // A key from either input is not necessarily unique across UNION.
            primary_index: vec![],
        };

        Ok(output_schema)
    }

    async fn build(
        &self,
        _input_schemas: HashMap<PortHandle, Schema>,
        _output_schemas: HashMap<PortHandle, Schema>,
        _event_hub: EventHub,
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
    let left = input_schemas
        .get(&0)
        .ok_or(PipelineError::InvalidPortHandle(0))?;
    let right = input_schemas
        .get(&1)
        .ok_or(PipelineError::InvalidPortHandle(1))?;
    if left.fields.len() != right.fields.len() {
        return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
    }
    left.fields
        .iter()
        .zip(&right.fields)
        .map(|(left, right)| {
            if left.typ != right.typ {
                return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
            }
            // UNION aligns values by position and keeps the left branch's names.
            Ok(FieldDefinition::new(
                left.name.clone(),
                left.typ,
                left.nullable || right.nullable,
                SourceDefinition::Dynamic,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::types::FieldType;

    fn schema(fields: &[(&str, FieldType, bool)]) -> Schema {
        Schema {
            fields: fields
                .iter()
                .map(|(name, typ, nullable)| {
                    FieldDefinition::new((*name).into(), *typ, *nullable, SourceDefinition::Dynamic)
                })
                .collect(),
            primary_index: vec![],
        }
    }

    #[test]
    fn union_schema_preserves_position_and_combines_nullability() {
        let left = schema(&[
            ("z", FieldType::Int, false),
            ("a", FieldType::String, false),
        ]);
        let right = schema(&[
            ("other", FieldType::Int, true),
            ("name", FieldType::String, false),
        ]);
        let output =
            validate_set_operation_input_schemas(&HashMap::from([(0, left), (1, right)])).unwrap();
        assert_eq!(
            output,
            schema(&[("z", FieldType::Int, true), ("a", FieldType::String, false)]).fields
        );
    }

    #[test]
    fn union_schema_rejects_wrong_right_branch_width_or_type() {
        let left = schema(&[("value", FieldType::Int, true)]);
        for right in [
            schema(&[]),
            schema(&[("value", FieldType::String, true)]),
            schema(&[
                ("value", FieldType::Int, true),
                ("extra", FieldType::Int, true),
            ]),
        ] {
            assert!(matches!(
                validate_set_operation_input_schemas(&HashMap::from([
                    (0, left.clone()),
                    (1, right)
                ])),
                Err(PipelineError::SetError(SetError::InvalidInputSchemas))
            ));
        }
    }
}
