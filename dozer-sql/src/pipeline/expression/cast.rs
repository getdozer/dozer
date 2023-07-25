use std::fmt::{Display, Formatter};

use dozer_core::processor_record::ProcessorRecord;
use dozer_types::{
    ordered_float::OrderedFloat,
    types::{Field, FieldType, Schema},
};

use crate::pipeline::errors::{FieldTypes, PipelineError};

use super::execution::{Expression, ExpressionType};

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum CastOperatorType {
    UInt,
    U128,
    Int,
    I128,
    Float,
    Boolean,
    String,
    Text,
    Binary,
    Decimal,
    Timestamp,
    Date,
    Json,
}

impl Display for CastOperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CastOperatorType::UInt => f.write_str("CAST AS UINT"),
            CastOperatorType::U128 => f.write_str("CAST AS U128"),
            CastOperatorType::Int => f.write_str("CAST AS INT"),
            CastOperatorType::I128 => f.write_str("CAST AS I128"),
            CastOperatorType::Float => f.write_str("CAST AS FLOAT"),
            CastOperatorType::Boolean => f.write_str("CAST AS BOOLEAN"),
            CastOperatorType::String => f.write_str("CAST AS STRING"),
            CastOperatorType::Text => f.write_str("CAST AS TEXT"),
            CastOperatorType::Binary => f.write_str("CAST AS BINARY"),
            CastOperatorType::Decimal => f.write_str("CAST AS DECIMAL"),
            CastOperatorType::Timestamp => f.write_str("CAST AS TIMESTAMP"),
            CastOperatorType::Date => f.write_str("CAST AS DATE"),
            CastOperatorType::Json => f.write_str("CAST AS JSON"),
        }
    }
}

impl CastOperatorType {
    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        arg: &Expression,
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        let field = arg.evaluate(record, schema)?;
        match self {
            CastOperatorType::UInt => {
                if let Some(value) = field.to_uint() {
                    Ok(Field::UInt(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::UInt,
                    })
                }
            }
            CastOperatorType::U128 => {
                if let Some(value) = field.to_u128() {
                    Ok(Field::U128(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::U128,
                    })
                }
            }
            CastOperatorType::Int => {
                if let Some(value) = field.to_int() {
                    Ok(Field::Int(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Int,
                    })
                }
            }
            CastOperatorType::I128 => {
                if let Some(value) = field.to_i128() {
                    Ok(Field::I128(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::I128,
                    })
                }
            }
            CastOperatorType::Float => {
                if let Some(value) = field.to_float() {
                    Ok(Field::Float(OrderedFloat(value)))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Float,
                    })
                }
            }
            CastOperatorType::Boolean => {
                if let Some(value) = field.to_boolean() {
                    Ok(Field::Boolean(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Boolean,
                    })
                }
            }
            CastOperatorType::String => {
                if let Some(value) = field.to_string() {
                    Ok(Field::String(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::String,
                    })
                }
            }
            CastOperatorType::Text => {
                if let Some(value) = field.to_text() {
                    Ok(Field::Text(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Text,
                    })
                }
            }
            CastOperatorType::Binary => {
                if let Some(value) = field.to_binary() {
                    Ok(Field::Binary(value.to_vec()))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Binary,
                    })
                }
            }
            CastOperatorType::Decimal => {
                if let Some(value) = field.to_decimal() {
                    Ok(Field::Decimal(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Decimal,
                    })
                }
            }
            CastOperatorType::Timestamp => {
                if let Some(value) = field.to_timestamp()? {
                    Ok(Field::Timestamp(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Timestamp,
                    })
                }
            }
            CastOperatorType::Date => {
                if let Some(value) = field.to_date()? {
                    Ok(Field::Date(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Date,
                    })
                }
            }
            CastOperatorType::Json => {
                if let Some(value) = field.to_json() {
                    Ok(Field::Json(value))
                } else {
                    Err(PipelineError::InvalidCast {
                        from: field,
                        to: FieldType::Json,
                    })
                }
            }
        }
    }

    pub(crate) fn get_return_type(
        &self,
        schema: &Schema,
        arg: &Expression,
    ) -> Result<ExpressionType, PipelineError> {
        let (expected_input_type, return_type) = match self {
            CastOperatorType::UInt => (
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
            CastOperatorType::U128 => (
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
            CastOperatorType::Int => (
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
            CastOperatorType::I128 => (
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
            CastOperatorType::Float => (
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
            CastOperatorType::Boolean => (
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
            CastOperatorType::String => (
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
            CastOperatorType::Text => (
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
            CastOperatorType::Binary => (vec![FieldType::Binary], FieldType::Binary),
            CastOperatorType::Decimal => (
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
            CastOperatorType::Timestamp => (
                vec![FieldType::String, FieldType::Timestamp],
                FieldType::Timestamp,
            ),
            CastOperatorType::Date => (vec![FieldType::Date, FieldType::String], FieldType::Date),
            CastOperatorType::Json => (
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

pub(crate) fn validate_arg_type(
    arg: &Expression,
    expected: Vec<FieldType>,
    schema: &Schema,
    fct: &CastOperatorType,
    idx: usize,
) -> Result<ExpressionType, PipelineError> {
    let arg_t = arg.get_type(schema)?;
    if !expected.contains(&arg_t.return_type) {
        Err(PipelineError::InvalidFunctionArgumentType(
            fct.to_string(),
            arg_t.return_type,
            FieldTypes::new(expected),
            idx,
        ))
    } else {
        Ok(arg_t)
    }
}
