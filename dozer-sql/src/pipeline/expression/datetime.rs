use crate::pipeline::errors::PipelineError::{
    InvalidFunctionArgument, InvalidFunctionArgumentType,
};
use crate::pipeline::errors::{FieldTypes, PipelineError};

use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use dozer_types::chrono::Datelike;
use dozer_types::types::{Field, FieldType, Record, Schema};
use num_traits::ToPrimitive;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum DateTimeFunctionType {
    DayOfWeek,
}

impl Display for DateTimeFunctionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DateTimeFunctionType::DayOfWeek => f.write_str("DAY_OF_WEEK"),
        }
    }
}

pub(crate) fn get_datetime_function_type(
    function: &DateTimeFunctionType,
    arg: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    match function {
        DateTimeFunctionType::DayOfWeek => {
            let return_type = arg.get_type(schema)?.return_type;
            if return_type != FieldType::Date && return_type != FieldType::Timestamp {
                return Err(InvalidFunctionArgumentType(
                    DateTimeFunctionType::DayOfWeek.to_string(),
                    return_type,
                    FieldTypes::new(vec![FieldType::Date, FieldType::Timestamp]),
                    0,
                ));
            }

            Ok(ExpressionType::new(
                FieldType::Int,
                false,
                dozer_types::types::SourceDefinition::Dynamic,
                false,
            ))
        }
    }
}

impl DateTimeFunctionType {
    pub fn new(name: &str) -> Result<DateTimeFunctionType, PipelineError> {
        match name {
            "day_of_week" => Ok(DateTimeFunctionType::DayOfWeek),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        arg: &Expression,
        record: &Record,
    ) -> Result<Field, PipelineError> {
        match self {
            DateTimeFunctionType::DayOfWeek => evaluate_day_of_week(schema, arg, record),
        }
    }
}

pub(crate) fn evaluate_day_of_week(
    schema: &Schema,
    arg: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::Date(d) => Ok(Field::Int(
            d.weekday().num_days_from_monday().to_i64().ok_or(
                PipelineError::InvalidOperandType(format!("Unable to cast date {d} to i64")),
            )?,
        )),
        Field::Timestamp(ts) => Ok(Field::Int(
            ts.weekday().num_days_from_monday().to_i64().ok_or(
                PipelineError::InvalidOperandType(format!("Unable to cast timestamp {ts} to i64")),
            )?,
        )),
        _ => Err(InvalidFunctionArgument(
            DateTimeFunctionType::DayOfWeek.to_string(),
            value,
            0,
        )),
    }
}

#[test]
fn test_day_of_week() {
    let row = Record::new(None, vec![], None);

    let v = Expression::Literal(Field::Date(
        dozer_types::chrono::NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
    ));
    assert_eq!(
        evaluate_day_of_week(&Schema::empty(), &v, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(6)
    );

    let v = Expression::Literal(Field::Date(
        dozer_types::chrono::NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
    ));
    assert_eq!(
        evaluate_day_of_week(&Schema::empty(), &v, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(0)
    );
}
