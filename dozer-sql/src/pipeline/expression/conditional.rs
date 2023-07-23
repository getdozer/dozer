use crate::pipeline::errors::PipelineError::{
    InvalidConditionalExpression, InvalidFunction, NotEnoughArguments,
};
use crate::pipeline::errors::{FieldTypes, PipelineError};
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor, ExpressionType};
use dozer_types::types::{Field, FieldType, ProcessorRecord, Schema};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ConditionalExpressionType {
    Coalesce,
    NullIf,
}

pub(crate) fn get_conditional_expr_type(
    function: &ConditionalExpressionType,
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    match function {
        ConditionalExpressionType::Coalesce => validate_coalesce(args, schema),
        ConditionalExpressionType::NullIf => todo!(),
    }
}

impl ConditionalExpressionType {
    pub(crate) fn new(name: &str) -> Result<ConditionalExpressionType, PipelineError> {
        match name {
            "coalesce" => Ok(ConditionalExpressionType::Coalesce),
            "nullif" => Ok(ConditionalExpressionType::NullIf),
            _ => Err(InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        schema: &Schema,
        args: &[Expression],
        record: &ProcessorRecord,
    ) -> Result<Field, PipelineError> {
        match self {
            ConditionalExpressionType::Coalesce => evaluate_coalesce(schema, args, record),
            ConditionalExpressionType::NullIf => todo!(),
        }
    }
}

pub(crate) fn validate_coalesce(
    args: &[Expression],
    schema: &Schema,
) -> Result<ExpressionType, PipelineError> {
    if args.is_empty() {
        return Err(NotEnoughArguments(
            ConditionalExpressionType::Coalesce.to_string(),
        ));
    }

    let return_types = args
        .iter()
        .map(|expr| expr.get_type(schema).unwrap().return_type)
        .collect::<Vec<FieldType>>();
    let return_type = return_types[0];
    if !return_types.iter().all(|&typ| typ == return_type) {
        return Err(InvalidConditionalExpression(
            ConditionalExpressionType::Coalesce.to_string(),
            FieldTypes::new(return_types),
        ));
    }

    Ok(ExpressionType::new(
        return_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub(crate) fn evaluate_coalesce(
    schema: &Schema,
    args: &[Expression],
    record: &ProcessorRecord,
) -> Result<Field, PipelineError> {
    // The COALESCE function returns the first of its arguments that is not null.
    for expr in args {
        let field = expr.evaluate(record, schema)?;
        if field != Field::Null {
            return Ok(field);
        }
    }
    // Null is returned only if all arguments are null.
    Ok(Field::Null)
}

impl Display for ConditionalExpressionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionalExpressionType::Coalesce => f.write_str("COALESCE"),
            ConditionalExpressionType::NullIf => f.write_str("NULLIF"),
        }
    }
}
