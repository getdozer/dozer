use crate::aggregation::aggregator::Aggregator;

use crate::calculate_err_field;
use crate::errors::{PipelineError, UnsupportedSqlError};
use dozer_sql_expression::aggregate::AggregateFunctionType::MaxAppendOnly;

use dozer_types::chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;

use dozer_types::types::{DozerDuration, Field, FieldType, TimeUnit};

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct MaxAppendOnlyAggregator {
    current_state: Field,
    return_type: Option<FieldType>,
}

impl MaxAppendOnlyAggregator {
    pub fn new() -> Self {
        Self {
            current_state: Field::Null,
            return_type: None,
        }
    }

    pub fn update_state(&mut self, field: Field) {
        self.current_state = field;
    }
}

impl Aggregator for MaxAppendOnlyAggregator {
    fn init(&mut self, return_type: FieldType) {
        self.return_type = Some(return_type);
    }

    fn update(&mut self, _old: &[Field], _new: &[Field]) -> Result<Field, PipelineError> {
        Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::GenericError("Append only".to_string()),
        ))
    }

    fn delete(&mut self, _old: &[Field]) -> Result<Field, PipelineError> {
        Err(PipelineError::UnsupportedSqlError(
            UnsupportedSqlError::GenericError("Append only".to_string()),
        ))
    }

    fn insert(&mut self, new: &[Field]) -> Result<Field, PipelineError> {
        let cur_max = self.current_state.clone();

        for val in new {
            if val == &Field::Null {
                continue;
            }
            match self.return_type {
                Some(typ) => match typ {
                    FieldType::UInt => {
                        let new_val = calculate_err_field!(val.to_uint(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => u64::MIN,
                            _ => calculate_err_field!(cur_max.to_uint(), MaxAppendOnly, val),
                        };
                        if new_val > max_val {
                            self.update_state(Field::UInt(new_val));
                        }
                    }
                    FieldType::U128 => {
                        let new_val = calculate_err_field!(val.to_u128(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => u128::MIN,
                            _ => calculate_err_field!(cur_max.to_u128(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::U128(new_val));
                        }
                    }
                    FieldType::Int => {
                        let new_val = calculate_err_field!(val.to_int(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => i64::MIN,
                            _ => calculate_err_field!(cur_max.to_int(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Int(new_val));
                        }
                    }
                    FieldType::Int8 => {
                        let new_val = calculate_err_field!(val.to_int8(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => i8::MIN,
                            _ => calculate_err_field!(cur_max.to_int8(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Int8(new_val));
                        }
                    }
                    FieldType::I128 => {
                        let new_val = calculate_err_field!(val.to_i128(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => i128::MIN,
                            _ => calculate_err_field!(cur_max.to_i128(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::I128(new_val));
                        }
                    }
                    FieldType::Float => {
                        let new_val = calculate_err_field!(val.to_float(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => f64::MIN,
                            _ => calculate_err_field!(cur_max.to_float(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Float(OrderedFloat(new_val)));
                        }
                    }
                    FieldType::Decimal => {
                        let new_val = calculate_err_field!(val.to_decimal(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => Decimal::MIN,
                            _ => calculate_err_field!(cur_max.to_decimal(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Decimal(new_val));
                        }
                    }
                    FieldType::Timestamp => {
                        let new_val = calculate_err_field!(val.to_timestamp(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => DateTime::<FixedOffset>::from(DateTime::<Utc>::MIN_UTC),
                            _ => calculate_err_field!(cur_max.to_timestamp(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Timestamp(new_val));
                        }
                    }
                    FieldType::Date => {
                        let new_val = calculate_err_field!(val.to_date(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => NaiveDate::MIN,
                            _ => calculate_err_field!(cur_max.to_date(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Date(new_val));
                        }
                    }
                    FieldType::Duration => {
                        let new_val = calculate_err_field!(val.to_duration(), MaxAppendOnly, val);
                        let max_val = match cur_max {
                            Field::Null => {
                                DozerDuration(std::time::Duration::ZERO, TimeUnit::Nanoseconds)
                            }
                            _ => calculate_err_field!(cur_max.to_duration(), MaxAppendOnly, val),
                        };

                        if new_val > max_val {
                            self.update_state(Field::Duration(new_val));
                        }
                    }
                    FieldType::Boolean
                    | FieldType::String
                    | FieldType::Text
                    | FieldType::Binary
                    | FieldType::Json
                    | FieldType::Point => {
                        return Err(PipelineError::InvalidReturnType(format!(
                            "Not supported return type {typ} for {MaxAppendOnly}"
                        )));
                    }
                },
                None => {
                    return Err(PipelineError::InvalidReturnType(format!(
                        "Not supported None return type for {MaxAppendOnly}"
                    )))
                }
            }
        }
        Ok(self.current_state.clone())
    }
}
