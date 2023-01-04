use crate::arg_str;
use crate::pipeline::errors::PipelineError;
#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::{evaluate_round, ScalarFunctionType};
use dozer_types::types::{Field, Record};

pub(crate) fn evaluate_ucase(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::String(s) => Ok(Field::String(s.to_uppercase())),
        Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
        _ => Err(PipelineError::InvalidFunctionArgument(
            ScalarFunctionType::Ucase.to_string(),
            value,
            0,
        )),
    }
}

pub(crate) fn evaluate_concat(
    arg0: &Expression,
    arg1: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let (f0, f1) = (arg0.evaluate(record)?, arg1.evaluate(record)?);
    let (v0, v1) = (
        arg_str!(f0, ScalarFunctionType::Concat, 0)?,
        arg_str!(f1, ScalarFunctionType::Concat, 1)?,
    );
    Ok(Field::String(v0.to_owned() + v1))
}

pub(crate) fn evaluate_length(arg0: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let f0 = arg0.evaluate(record)?;
    let v0 = arg_str!(f0, ScalarFunctionType::Concat, 0)?;
    Ok(Field::UInt(v0.len() as u64))
}

#[test]
fn test_ucase() {
    let row = Record::new(None, vec![]);

    let s = Box::new(Literal(Field::String(String::from("data"))));
    let t = Box::new(Literal(Field::Text(String::from("Data"))));
    let s_output = Field::String(String::from("DATA"));
    let t_output = Field::Text(String::from("DATA"));

    assert_eq!(
        evaluate_ucase(&s, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        s_output
    );
    assert_eq!(
        evaluate_ucase(&t, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        t_output
    );
}
