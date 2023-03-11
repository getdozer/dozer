use crate::argv;
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::aggregate::AggregateFunctionType;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use dozer_types::types::{FieldType, Schema, SourceDefinition};

pub fn get_aggr_avg_return_type(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Avg)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Decimal,
        FieldType::UInt => FieldType::Decimal,
        FieldType::Float => FieldType::Float,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                "AVG".to_string(),
                r,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::Int,
                    FieldType::Float,
                ]),
                0,
            ));
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

pub fn get_aggr_count_return_type(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    Ok(ExpressionType::new(
        FieldType::Int,
        false,
        SourceDefinition::Dynamic,
        false,
    ))
}

pub fn get_aggr_min_return_type(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Min)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Int,
        FieldType::UInt => FieldType::UInt,
        FieldType::Float => FieldType::Float,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                "MIN".to_string(),
                r,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::Int,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                ]),
                0,
            ));
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

pub fn get_aggr_max_return_type(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Max)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Int,
        FieldType::UInt => FieldType::UInt,
        FieldType::Float => FieldType::Float,
        FieldType::Timestamp => FieldType::Timestamp,
        FieldType::Date => FieldType::Date,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                "MAX".to_string(),
                r,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::Int,
                    FieldType::Float,
                    FieldType::Timestamp,
                    FieldType::Date,
                ]),
                0,
            ));
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}

pub fn get_aggr_sum_return_type(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    let arg = &argv!(args, 0, AggregateFunctionType::Sum)?.get_type(schema)?;

    let ret_type = match arg.return_type {
        FieldType::Decimal => FieldType::Decimal,
        FieldType::Int => FieldType::Int,
        FieldType::UInt => FieldType::UInt,
        FieldType::Float => FieldType::Float,
        r => {
            return Err(PipelineError::InvalidFunctionArgumentType(
                "MAX".to_string(),
                r,
                FieldTypes::new(vec![
                    FieldType::Decimal,
                    FieldType::UInt,
                    FieldType::Int,
                    FieldType::Float,
                ]),
                0,
            ));
        }
    };
    Ok(ExpressionType::new(
        ret_type,
        true,
        SourceDefinition::Dynamic,
        false,
    ))
}
