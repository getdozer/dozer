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
    unary_distinct: bool,
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
            unary_distinct: false,
        }
    }

    /// Exact counted DISTINCT after projection; no second branch or hash-only
    /// probabilistic equality is appropriate for relational multiplicities.
    pub(crate) fn distinct(id: String) -> Self {
        Self {
            id,
            set_quantifier: SetQuantifier::Distinct,
            enable_probabilistic_optimizations: false,
            unary_distinct: true,
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
        if self.unary_distinct {
            vec![DEFAULT_PORT_HANDLE]
        } else {
            vec![0, 1]
        }
    }

    fn get_output_ports(&self) -> Vec<PortHandle> {
        vec![DEFAULT_PORT_HANDLE]
    }

    async fn get_output_schema(
        &self,
        _output_port: &PortHandle,
        input_schemas: &HashMap<PortHandle, Schema>,
    ) -> Result<Schema, BoxedError> {
        if self.unary_distinct {
            return input_schemas
                .get(&DEFAULT_PORT_HANDLE)
                .cloned()
                .ok_or_else(|| PipelineError::SetError(SetError::InvalidInputSchemas).into());
        }
        let output_columns = validate_set_operation_input_schemas(input_schemas)?;

        let output_schema = Schema {
            fields: output_columns,
            primary_index: input_schemas[&0].primary_index.clone(),
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
    let left_columns = &input_schemas
        .get(&0)
        .ok_or(PipelineError::SetError(SetError::InvalidInputSchemas))?
        .fields;
    let right_columns = &input_schemas
        .get(&1)
        .ok_or(PipelineError::SetError(SetError::InvalidInputSchemas))?
        .fields;
    if left_columns.len() != right_columns.len() {
        return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
    }

    let mut output_fields = Vec::new();
    for (left, right) in left_columns.iter().zip(right_columns.iter()) {
        if !is_similar_fields(left, right) {
            return Err(PipelineError::SetError(SetError::InvalidInputSchemas));
        }
        output_fields.push(FieldDefinition::new(
            left.name.clone(),
            left.typ,
            left.nullable || right.nullable,
            SourceDefinition::Dynamic,
        ));
    }
    Ok(output_fields)
}

fn is_similar_fields(left: &FieldDefinition, right: &FieldDefinition) -> bool {
    // UNION aligns values by position. The left branch supplies output names;
    // right aliases and nullable values must not reorder or reject its columns.
    left.typ == right.typ
}

#[cfg(test)]
mod tests {
    use super::*;
    use dozer_types::types::FieldType;

    #[test]
    fn union_validates_the_actual_right_port_and_column_count() {
        let column = FieldDefinition::new(
            "key".into(),
            FieldType::Int,
            true,
            SourceDefinition::Dynamic,
        );
        let left = Schema {
            fields: vec![column.clone()],
            primary_index: vec![],
        };
        let valid = HashMap::from([(0, left.clone()), (1, left.clone())]);
        assert_eq!(
            validate_set_operation_input_schemas(&valid).unwrap(),
            vec![column.clone()]
        );

        let mut wrong_type = left.clone();
        wrong_type.fields[0].typ = FieldType::String;
        let mut too_many = left.clone();
        too_many.fields.push(column);
        let mut too_few = left.clone();
        too_few.fields.clear();
        for right in [wrong_type, too_many, too_few] {
            let inputs = HashMap::from([(0, left.clone()), (1, right)]);
            assert!(matches!(
                validate_set_operation_input_schemas(&inputs),
                Err(PipelineError::SetError(SetError::InvalidInputSchemas))
            ));
        }
    }

    #[test]
    fn union_keeps_left_order_and_names_and_combines_nullability() {
        let left = Schema {
            fields: vec![
                FieldDefinition::new("z".into(), FieldType::Int, false, SourceDefinition::Dynamic),
                FieldDefinition::new(
                    "a".into(),
                    FieldType::String,
                    true,
                    SourceDefinition::Dynamic,
                ),
            ],
            primary_index: vec![],
        };
        let right = Schema {
            fields: vec![
                FieldDefinition::new(
                    "other_int".into(),
                    FieldType::Int,
                    true,
                    SourceDefinition::Dynamic,
                ),
                FieldDefinition::new(
                    "other_string".into(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
            ],
            primary_index: vec![],
        };
        let valid = HashMap::from([(0, left.clone()), (1, right.clone())]);
        let fields = validate_set_operation_input_schemas(&valid).unwrap();
        assert_eq!(fields[0].name, "z");
        assert_eq!(fields[0].typ, FieldType::Int);
        assert_eq!(fields[1].name, "a");
        assert_eq!(fields[1].typ, FieldType::String);
        assert!(fields.iter().all(|field| field.nullable));

        let mut reversed = right;
        reversed.fields.reverse();
        for inputs in [
            HashMap::from([(0, left.clone()), (1, reversed)]),
            HashMap::from([(0, left)]),
        ] {
            assert!(matches!(
                validate_set_operation_input_schemas(&inputs),
                Err(PipelineError::SetError(SetError::InvalidInputSchemas))
            ));
        }
    }
}
