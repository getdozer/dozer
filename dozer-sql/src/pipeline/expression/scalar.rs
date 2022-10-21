use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use anyhow::{anyhow, bail, Result};
use dozer_types::types::{Field, Record};

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
            _ => bail!("Unsupported Scalar function: {}", name),
        })
    }

    pub(crate) fn evaluate(&self, args: &[Expression], record: &Record) -> anyhow::Result<Field> {
        match self {
            ScalarFunctionType::Abs => evaluate_abs(&args[0], record),
            ScalarFunctionType::Round => evaluate_round(&args[0], args.get(1), record),
        }
    }
}

fn evaluate_abs(arg: &Expression, record: &Record) -> anyhow::Result<Field> {
    let value = arg.evaluate(record)?;
    match value {
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        _ => Err(anyhow!("ABS doesn't support this type".to_string())),
    }
}

fn evaluate_round(
    arg: &Expression,
    decimals: Option<&Expression>,
    record: &Record,
) -> anyhow::Result<Field> {
    let value = arg.evaluate(record)?;
    let mut places = 0;
    if let Some(expression) = decimals {
        match expression.evaluate(record)? {
            Field::Int(i) => places = i as i32,
            Field::Float(f) => places = f.round() as i32,
            _ => {} // Truncate value to 0 decimals
        }
    }
    let order = 10.0_f64.powi(places);

    match value {
        Field::Int(i) => Ok(Field::Int(i)),
        Field::Float(f) => Ok(Field::Float((f * order).round() / order)),
        Field::Decimal(_) => Ok(Field::Invalid("ROUND doesn't support DECIMAL".to_string())),
        _ => Err(anyhow!("ROUND doesn't support {:?}", value)),
    }
}

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

#[test]
fn test_round() {
    let row = Record::new(None, vec![]);

    let v = Box::new(Literal(Field::Int(1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Int(1)
    );

    let v = Box::new(Literal(Field::Float(2.1)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(2.0)
    );

    let v = Box::new(Literal(Field::Float(2.6)));
    let d = &Box::new(Literal(Field::Int(0)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(3.0)
    );

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::Int(2)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(2.63)
    );

    let v = Box::new(Literal(Field::Float(212.633)));
    let d = &Box::new(Literal(Field::Int(-2)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(200.0)
    );

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::Float(2.1)));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(2.63)
    );

    let v = Box::new(Literal(Field::Float(2.633)));
    let d = &Box::new(Literal(Field::String("2.3".to_string())));
    assert_eq!(
        evaluate_round(&v, Some(d), &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Float(3.0)
    );

    // let v = Box::new(Literal(Field::Boolean(true)));
    // let d = &Box::new(Literal(Field::Int(2)));
    // assert!(evaluate_round(&v, Some(d), &row) == Field::Invalid(_));
    // let v = Box::new(Literal(Field::Float(2.1)));
    // assert!(evaluate_round(&v, &row) == Field::Float(2));
    //
    // let v = Box::new(Literal(Field::Float(2.6)));
    // assert!(evaluate_round(&v, &row) == Field::Float(3));
    //
    // let v = Box::new(Literal(Field::Boolean(true)));
    // assert!(evaluate_round(&v, &row) == Field::Invalid(_));
}
