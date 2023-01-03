

/// `evaluate_ucase` is a scalar function which converts string, text to upper-case, implementing support for UCASE()
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

use dozer_types::types::{Field, Record};
use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::scalar::evaluate_round;

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
