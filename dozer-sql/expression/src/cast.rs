use std::fmt::{Display, Formatter};

use dozer_types::types::Record;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldType, Schema},
};

use crate::arg_utils::validate_arg_type;
use crate::error::Error;

use super::execution::{Expression, ExpressionType};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct CastOperatorType(pub FieldType);

impl Display for CastOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            FieldType::UInt => f.write_str("CAST AS UINT"),
            FieldType::U128 => f.write_str("CAST AS U128"),
            FieldType::Int => f.write_str("CAST AS INT"),
            FieldType::I128 => f.write_str("CAST AS I128"),
            FieldType::Float => f.write_str("CAST AS FLOAT"),
            FieldType::Boolean => f.write_str("CAST AS BOOLEAN"),
            FieldType::String => f.write_str("CAST AS STRING"),
            FieldType::Text => f.write_str("CAST AS TEXT"),
            FieldType::Binary => f.write_str("CAST AS BINARY"),
            FieldType::Decimal => f.write_str("CAST AS DECIMAL"),
            FieldType::Timestamp => f.write_str("CAST AS TIMESTAMP"),
            FieldType::Date => f.write_str("CAST AS DATE"),
            FieldType::Json => f.write_str("CAST AS JSON"),
            FieldType::Point => f.write_str("CAST AS POINT"),
            FieldType::Duration => f.write_str("CAST AS DURATION"),
        }
    }
}

impl CastOperatorType {
    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        arg: &mut Expression,
        record: &Record,
    ) -> Result<Field, Error> {
        let field = arg.evaluate(record, schema)?;
        cast_field(&field, self.0)
    }

    pub(crate) fn get_return_type(
        &self,
        schema: &Schema,
        arg: &Expression,
    ) -> Result<ExpressionType, Error> {
        let (expected_input_type, return_type) = match self.0 {
            FieldType::UInt => (
                vec![
                    FieldType::Int,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::I128,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::UInt,
            ),
            FieldType::U128 => (
                vec![
                    FieldType::Int,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::I128,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::U128,
            ),
            FieldType::Int => (
                vec![
                    FieldType::Int,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::I128,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::Int,
            ),
            FieldType::I128 => (
                vec![
                    FieldType::Int,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::I128,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::I128,
            ),
            FieldType::Float => (
                vec![
                    FieldType::Decimal,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::Float,
            ),
            FieldType::Boolean => (
                vec![
                    FieldType::Boolean,
                    FieldType::Decimal,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::Boolean,
            ),
            FieldType::String => (
                vec![
                    FieldType::Binary,
                    FieldType::Boolean,
                    FieldType::Date,
                    FieldType::Decimal,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::String,
                    FieldType::Text,
                    FieldType::Timestamp,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::String,
            ),
            FieldType::Text => (
                vec![
                    FieldType::Binary,
                    FieldType::Boolean,
                    FieldType::Date,
                    FieldType::Decimal,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::String,
                    FieldType::Text,
                    FieldType::Timestamp,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::Text,
            ),
            FieldType::Binary => (vec![FieldType::Binary], FieldType::Binary),
            FieldType::Decimal => (
                vec![
                    FieldType::Decimal,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::String,
                    FieldType::UInt,
                    FieldType::U128,
                ],
                FieldType::Decimal,
            ),
            FieldType::Timestamp => (
                vec![FieldType::String, FieldType::Timestamp],
                FieldType::Timestamp,
            ),
            FieldType::Date => (vec![FieldType::Date, FieldType::String], FieldType::Date),
            FieldType::Json => (
                vec![
                    FieldType::Boolean,
                    FieldType::Float,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::String,
                    FieldType::Text,
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Json,
                ],
                FieldType::Json,
            ),
            FieldType::Point => (vec![FieldType::Point], FieldType::Point),
            FieldType::Duration => (
                vec![
                    FieldType::UInt,
                    FieldType::U128,
                    FieldType::Int,
                    FieldType::I128,
                    FieldType::Duration,
                    FieldType::String,
                    FieldType::Text,
                ],
                FieldType::Duration,
            ),
        };

        let expression_type = validate_arg_type(arg, expected_input_type, schema, self, 0)?;
        Ok(ExpressionType {
            return_type,
            nullable: expression_type.nullable,
            source: expression_type.source,
            is_primary_key: expression_type.is_primary_key,
        })
    }
}

pub fn cast_field(input: &Field, output_type: FieldType) -> Result<Field, Error> {
    match output_type {
        FieldType::UInt => {
            if let Some(value) = input.to_uint() {
                Ok(Field::UInt(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::UInt,
                })
            }
        }
        FieldType::U128 => {
            if let Some(value) = input.to_u128() {
                Ok(Field::U128(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::U128,
                })
            }
        }
        FieldType::Int => {
            if let Some(value) = input.to_int() {
                Ok(Field::Int(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Int,
                })
            }
        }
        FieldType::I128 => {
            if let Some(value) = input.to_i128() {
                Ok(Field::I128(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::I128,
                })
            }
        }
        FieldType::Float => {
            if let Some(value) = input.to_float() {
                Ok(Field::Float(OrderedFloat(value)))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Float,
                })
            }
        }
        FieldType::Boolean => {
            if let Some(value) = input.to_boolean() {
                Ok(Field::Boolean(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Boolean,
                })
            }
        }
        FieldType::String => Ok(Field::String(input.to_string())),
        FieldType::Text => Ok(Field::Text(input.to_text())),
        FieldType::Binary => {
            if let Some(value) = input.to_binary() {
                Ok(Field::Binary(value.to_vec()))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Binary,
                })
            }
        }
        FieldType::Decimal => {
            if let Some(value) = input.to_decimal() {
                Ok(Field::Decimal(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Decimal,
                })
            }
        }
        FieldType::Timestamp => {
            if let Some(value) = input.to_timestamp() {
                Ok(Field::Timestamp(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Timestamp,
                })
            }
        }
        FieldType::Date => {
            if let Some(value) = input.to_date() {
                Ok(Field::Date(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Date,
                })
            }
        }
        FieldType::Json => {
            if let Some(value) = input.to_json() {
                Ok(Field::Json(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Json,
                })
            }
        }
        FieldType::Point => {
            if let Some(value) = input.to_point() {
                Ok(Field::Point(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Point,
                })
            }
        }
        FieldType::Duration => {
            if let Some(value) = input.to_duration() {
                Ok(Field::Duration(value))
            } else {
                Err(Error::InvalidCast {
                    from: input.clone(),
                    to: FieldType::Duration,
                })
            }
        }
    }
}
