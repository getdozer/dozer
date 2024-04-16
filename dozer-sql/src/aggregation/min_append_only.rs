use crate::aggregation::aggregator::Aggregator;

use crate::calculate_err_field;
use crate::errors::{PipelineError, UnsupportedSqlError};
use dozer_sql_expression::aggregate::AggregateFunctionType::MinAppendOnly;

use dozer_types::chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;

use dozer_types::types::{DozerDuration, Field, FieldType, TimeUnit};

#[derive(Debug, bincode::Encode, bincode::Decode)]
pub struct MinAppendOnlyAggregator {
    current_state: Field,
    return_type: Option<FieldType>,
}

impl MinAppendOnlyAggregator {
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

impl Aggregator for MinAppendOnlyAggregator {
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
        let cur_min = self.current_state.clone();

        for val in new {
            if val == &Field::Null {
                continue;
            }
            match self.return_type {
                Some(typ) => match typ {
                    FieldType::UInt => {
                        let new_val = calculate_err_field!(val.to_uint(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => u64::MAX,
                            _ => calculate_err_field!(cur_min.to_uint(), MinAppendOnly, val),
                        };
                        if new_val < min_val {
                            self.update_state(Field::UInt(new_val));
                        }
                    }
                    FieldType::U128 => {
                        let new_val = calculate_err_field!(val.to_u128(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => u128::MAX,
                            _ => calculate_err_field!(cur_min.to_u128(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::U128(new_val));
                        }
                    }
                    FieldType::Int => {
                        let new_val = calculate_err_field!(val.to_int(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => i64::MAX,
                            _ => calculate_err_field!(cur_min.to_int(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Int(new_val));
                        }
                    }
                    FieldType::Int8 => {
                        let new_val = calculate_err_field!(val.to_int8(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => i8::MAX,
                            _ => calculate_err_field!(cur_min.to_int8(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Int8(new_val));
                        }
                    }
                    FieldType::I128 => {
                        let new_val = calculate_err_field!(val.to_i128(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => i128::MAX,
                            _ => calculate_err_field!(cur_min.to_i128(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::I128(new_val));
                        }
                    }
                    FieldType::Float => {
                        let new_val = calculate_err_field!(val.to_float(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => f64::MAX,
                            _ => calculate_err_field!(cur_min.to_float(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Float(OrderedFloat(new_val)));
                        }
                    }
                    FieldType::Decimal => {
                        let new_val = calculate_err_field!(val.to_decimal(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => Decimal::MAX,
                            _ => calculate_err_field!(cur_min.to_decimal(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Decimal(new_val));
                        }
                    }
                    FieldType::Timestamp => {
                        let new_val = calculate_err_field!(val.to_timestamp(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => DateTime::<FixedOffset>::from(DateTime::<Utc>::MAX_UTC),
                            _ => calculate_err_field!(cur_min.to_timestamp(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Timestamp(new_val));
                        }
                    }
                    FieldType::Date => {
                        let new_val = calculate_err_field!(val.to_date(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => NaiveDate::MAX,
                            _ => calculate_err_field!(cur_min.to_date(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
                            self.update_state(Field::Date(new_val));
                        }
                    }
                    FieldType::Duration => {
                        let new_val = calculate_err_field!(val.to_duration(), MinAppendOnly, val);
                        let min_val = match cur_min {
                            Field::Null => {
                                DozerDuration(std::time::Duration::MAX, TimeUnit::Nanoseconds)
                            }
                            _ => calculate_err_field!(cur_min.to_duration(), MinAppendOnly, val),
                        };

                        if new_val < min_val {
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
                            "Not supported return type {typ} for {MinAppendOnly}"
                        )));
                    }
                },
                None => {
                    return Err(PipelineError::InvalidReturnType(format!(
                        "Not supported None return type for {MinAppendOnly}"
                    )))
                }
            }
        }
        Ok(self.current_state.clone())
    }
}
