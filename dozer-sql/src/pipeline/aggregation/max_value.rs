use crate::pipeline::aggregation::aggregator::{update_max_val_map, Aggregator};
use crate::pipeline::errors::PipelineError::InvalidReturnType;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType::MaxValue;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use crate::{argv, calculate_err};
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};
use std::collections::BTreeMap;

pub fn validate_max_value(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let base_arg = &argv!(args, 0, MaxValue)?.get_type(schema)?;
    let arg = &argv!(args, 1, MaxValue)?.get_type(schema)?;

    match base_arg.return_type {
        FieldType::UInt => FieldType::UInt,
        FieldType::U128 => FieldType::U128,
        FieldType::Int => FieldType::Int,
        FieldType::I128 => FieldType::I128,
        FieldType::Float => FieldType::Float,
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        FieldType::Duration => FieldType::Duration,
        FieldType::Boolean
        | FieldType::String
        | FieldType::Text
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                MaxValue.to_string(),
                arg.return_type,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                    FieldType::Duration,
                ]),
                0,
            ));
        }
    };

    Ok(ExpressionType::new(
        arg.return_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

#[derive(Debug)]
pub struct MaxValueAggregator {
    current_state: BTreeMap<Field, u64>,
    return_state: BTreeMap<Field, Vec<Field>>,
    return_type: Option<FieldType>,
}

impl MaxValueAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MaxValueAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_max_val_map(
            old,
            1_u64,
            true,
            &mut self.current_state,
            &mut self.return_state,
        )?;
        get_max_value(&self.current_state, &self.return_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_max_val_map(
            new,
            1_u64,
            false,
            &mut self.current_state,
            &mut self.return_state,
        )?;
        get_max_value(&self.current_state, &self.return_state, self.return_type)
    }
}

fn get_max_value(
    field_map: &BTreeMap<Field, u64>,
    return_map: &BTreeMap<Field, Vec<Field>>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().max(), MaxValue).clone();

        match return_map.get(&val) {
            Some(v) => match v.get(0) {
                Some(v) => {
                    let value = v.clone();
                    Ok(value)
                }
                None => Err(InvalidReturnType(format!("{:?}", return_type))),
            },
            None => Err(InvalidReturnType(format!("{:?}", return_type))),
        }
    }
}
