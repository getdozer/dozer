use crate::pipeline::errors::PipelineError;
use crate::pipeline::errors::PipelineError::InvalidFunctionArgument;
use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use crate::pipeline::expression::scalar::ScalarFunctionType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record};
use num_traits::Float;

pub(crate) fn evaluate_abs(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        _ => Err(PipelineError::InvalidFunctionArgument(
            ScalarFunctionType::Abs.to_string(),
            value,
            0,
        )),
    }
}

pub(crate) fn evaluate_round(
    arg: &Expression,
    decimals: Option<&Expression>,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    let mut places = 0;
    if let Some(expression) = decimals {
        match expression.evaluate(record)? {
            Field::Int(i) => places = i as i32,
            Field::Float(f) => places = f.round().0 as i32,
            _ => {} // Truncate value to 0 decimals
        }
    }
    let order = OrderedFloat(10.0_f64.powi(places));

    match value {
        Field::Int(i) => Ok(Field::Int(i)),
        Field::Float(f) => Ok(Field::Float((f * order).round() / order)),
        Field::Decimal(_) => Err(PipelineError::InvalidOperandType("ROUND()".to_string())),
        _ => Err(InvalidFunctionArgument(
            ScalarFunctionType::Round.to_string(),
            value,
            0,
        )),
    }
}

#[test]
fn test_round() {
    let row = Record::new(None, vec![]);

    let v = Box::new(Literal(Field::Int(1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1)
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.1))));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(2.0))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.6))));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(3.0))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
    let d = &Box::new(Literal(Field::Int(2)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(2.63))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(212.633))));
    let d = &Box::new(Literal(Field::Int(-2)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(200.0))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
    let d = &Box::new(Literal(Field::Float(OrderedFloat(2.1))));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(2.63))
    );

    let v = Box::new(Literal(Field::Float(OrderedFloat(2.633))));
    let d = &Box::new(Literal(Field::String("2.3".to_string())));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(OrderedFloat(3.0))
    );
}
