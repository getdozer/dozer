use crate::aggregation::aggregator::{update_map, Aggregator};
use crate::errors::PipelineError;
use crate::{calculate_err, calculate_err_field};
use dozer_sql_expression::aggregate::AggregateFunctionType::Max;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::{Field, FieldType};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "dozer_types::serde")]
pub struct MaxAggregator {
    current_state: BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
}

impl MaxAggregator {
    pub fn new() -> Self {
        Self {
            current_state: BTreeMap::new(),
            return_type: None,
        }
    }
}

impl Aggregator for MaxAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, old: &[Field], new: &[Field]) -> Result<Field, PipelineError> {
        self.delete(old)?;
        self.insert(new)
    }

    fn delete(&mut self, old: &[Field]) -> Result<Field, PipelineError> {
        update_map(old, 1_u64, true, &mut self.current_state);
        get_max(&self.current_state, self.return_type)
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        update_map(new, 1_u64, false, &mut self.current_state);
        get_max(&self.current_state, self.return_type)
    }
}

fn get_max(
    field_map: &BTreeMap<Field, u64>,
    return_type: Option<FieldType>,
) -> Result<Field, PipelineError> {
    if field_map.is_empty() {
        Ok(Field::Null)
    } else {
        let val = calculate_err!(field_map.keys().max(), Max).clone();
        match return_type {
            Some(typ) => match typ {
                FieldType::UInt => Ok(Field::UInt(calculate_err_field!(val.to_uint(), Max, val))),
                FieldType::U128 => Ok(Field::U128(calculate_err_field!(val.to_u128(), Max, val))),
                FieldType::Int => Ok(Field::Int(calculate_err_field!(val.to_int(), Max, val))),
                FieldType::I128 => Ok(Field::I128(calculate_err_field!(val.to_i128(), Max, val))),
                FieldType::Float => Ok(Field::Float(OrderedFloat::from(calculate_err_field!(
                    val.to_float(),
                    Max,
                    val
                )))),
                FieldType::Decimal => Ok(Field::Decimal(calculate_err_field!(
                    val.to_decimal(),
                    Max,
                    val
                ))),
                FieldType::Timestamp => Ok(Field::Timestamp(calculate_err_field!(
                    val.to_timestamp(),
                    Max,
                    val
                ))),
                FieldType::Date => Ok(Field::Date(calculate_err_field!(val.to_date(), Max, val))),
                FieldType::Duration => Ok(Field::Duration(calculate_err_field!(
                    val.to_duration(),
                    Max,
                    val
                ))),
                FieldType::Boolean
                | FieldType::String
                | FieldType::Text
                | FieldType::Binary
                | FieldType::Json
                | FieldType::Point => Err(PipelineError::InvalidReturnType(format!(
                    "Not supported return type {typ} for {Max}"
                ))),
            },
            None => Err(PipelineError::InvalidReturnType(format!(
                "Not supported None return type for {Max}"
            ))),
        }
    }
}
