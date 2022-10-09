use dozer_types::types::{Field, Record};

use crate::common::error::{DozerSqlError, Result};
use crate::pipeline::expression::expression::{Expression, ExpressionExecutor};
use crate::pipeline::expression::expression::Expression::Literal;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
}

impl ScalarFunctionType {
    pub fn new(name: &str) -> Result<ScalarFunctionType> {
        Ok(match name {
            "abs" => ScalarFunctionType::Abs,
            "round" => ScalarFunctionType::Round,
            _ => {
                return Err(DozerSqlError::NotImplemented(format!(
                    "Unsupported Scalar function: {}",
                    name
                )));
            }
        })
    }

    pub(crate) fn evaluate(&self, args: &Vec<Box<Expression>>, record: &Record) -> Field {
        match self {
            ScalarFunctionType::Abs => evaluate_abs(&args[0], record),
            ScalarFunctionType::Round => evaluate_round(&args[0], args.get(1), record),
        }
    }

}

fn evaluate_abs(arg: &Box<Expression>, record: &Record) -> Field {
    let value = arg.evaluate(record);
    match value {
        Field::Int(i) => Field::Int(i.abs()),
        Field::Float(f) => Field::Float(f.abs()),
        _ => Field::Invalid(format!("ABS doesn't support this type"))
    }
}

fn evaluate_round(arg: &Box<Expression>, decimals: Option<&Box<Expression>>, record: &Record) -> Field {
    let value = arg.evaluate(record);
    let mut places = 0;
    if let Some(expression) = decimals {
        match expression.evaluate(record) {
            Field::Int(i) => places = i as i32,
            Field::Float(f) => places = f.round() as i32,
            _ => {} // Truncate value to 0 decimals
        }
    }
    let order = (10.0 as f64).powi(places);

    match value {
        Field::Int(i) => Field::Int(i),
        Field::Float(f) => Field::Float((f * order).round() / order),
        Field::Decimal(_) => Field::Invalid(format!("ROUND doesn't support DECIMAL")),
        _ => Field::Invalid(format!("ROUND doesn't support {:?}", value))
    }
}

#[test]
fn test_round() {
    let row = Record::new(None, vec![]);

    let v = Box::new(Literal(Field::Int(1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Int(1)));

    let v = Box::new(Literal(Field::Float(2.1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(2.0)));

    let v = Box::new(Literal(Field::Float(2.6)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(3.0)));

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::Int(2)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(2.63)));

    let v = Box::new(Literal(Field::Float(212.633)));
    let d = &Box::new(Literal(Field::Int(-2)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(200.0)));

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::Float(2.1)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(2.63)));

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::String("2.3".to_string())));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Float(3.0)));

    let v = Box::new(Literal(Field::Boolean(true)));
    let d = &Box::new(Literal(Field::Int(2)));
    assert!(matches!(evaluate_round(&v, Some(d), &row), Field::Invalid(_)));

    // let v = Box::new(Literal(Field::Float(2.1)));
    // assert!(matches!(evaluate_round(&v, &row), Field::Float(2)));
    //
    // let v = Box::new(Literal(Field::Float(2.6)));
    // assert!(matches!(evaluate_round(&v, &row), Field::Float(3)));
    //
    // let v = Box::new(Literal(Field::Boolean(true)));
    // assert!(matches!(evaluate_round(&v, &row), Field::Invalid(_)));
}