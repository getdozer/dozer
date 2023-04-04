use crate::calculate_err_type;
use crate::pipeline::aggregation::aggregator::Aggregator;
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::aggregate::AggregateFunctionType::Count;
use crate::pipeline::expression::execution::{Expression, ExpressionType};
use dozer_core::errors::ExecutionError::InvalidType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{Field, FieldType, Schema, SourceDefinition};
use num_traits::FromPrimitive;

pub fn validate_count(
    _args: &[Expression],
    _schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    Ok(ExpressionType::new(
        FieldType::Int,
        false,
        SourceDefinition::Dynamic,
        false,
    ))
}

#[derive(Debug)]
pub struct CountAggregator {
    current_state: u64,
    return_type: Option<FieldType>,
}

impl CountAggregator {
    pub fn new() -> Self {
        Self {
            current_state: 0_u64,
            return_type: None,
        }
    }
}

impl Aggregator for CountAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        self.current_state -= old.len() as u64;
        get_count(self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        self.current_state += new.len() as u64;
        get_count(self.current_state, self.return_type)
    }
}

fn get_count(count: u64, return_type: Option<FieldType>) -> Result<Field, PipelineError> {
    match return_type {
        Some(typ) => match typ {
            FieldType::UInt => Ok(Field::UInt(count)),
            FieldType::U128 => Ok(Field::U128(count as u128)),
            FieldType::Int => Ok(Field::Int(count as i64)),
            FieldType::I128 => Ok(Field::I128(count as i128)),
            FieldType::Float => Ok(Field::Float(OrderedFloat::from(count as f64))),
            FieldType::Decimal => Ok(Field::Decimal(calculate_err_type!(
                Decimal::from_f64(count as f64),
                Count,
                FieldType::Decimal
            ))),
            FieldType::Boolean
            | FieldType::String
            | FieldType::Text
            | FieldType::Date
            | FieldType::Timestamp
            | FieldType::Binary
            | FieldType::Bson
            | FieldType::Point => Err(PipelineError::InternalExecutionError(InvalidType(format!(
                "Not supported return type {typ} for {Count}"
            )))),
        },
        None => Err(PipelineError::InternalExecutionError(InvalidType(format!(
            "Not supported None return type for {Count}"
        )))),
    }
}
