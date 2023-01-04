use crate::pipeline::errors::PipelineError;
#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::evaluate_round;
use dozer_types::types::{Field, Record};

macro_rules! arg_str {
    ($field: expr, $fct: expr, $idx: expr) => {
        match $field.as_string() {
            Some(e) => Ok(e),
            _ => Err(PipelineError::InvalidFunctionArgument(
                $fct.to_string(),
                $field,
                $idx,
            )),
        }
    };
}

pub(crate) fn evaluate_ucase(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::String(s) => Ok(Field::String(s.to_uppercase())),
        Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
        _ => Err(PipelineError::InvalidFunction(format!(
            "UCASE() for {:?}",
            value
        ))),
    }
}

pub(crate) fn evaluate_concat(
    arg0: &Expression,
    arg1: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let v0: &str = arg_str!(arg0.evaluate(record)?, "CONCAT", 1)?;
    let v1 = arg0.evaluate(record)?;

    Ok(v1)
    // let s0 = v0.as_string().context(Pipe)
    //
    // match value {
    //     Field::String(s) => Ok(Field::String(s.to_uppercase())),
    //     Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
    //     _ => Err(PipelineError::InvalidFunction(format!(
    //         "UCASE() for {:?}",
    //         value
    //     ))),
    // }
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
