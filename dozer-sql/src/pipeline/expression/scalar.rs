use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::{Field, Record};
use num_traits::Float;
use dozer_types::serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum ScalarFunctionType {
    Abs,
    Round,
    Ucase,
}

impl ScalarFunctionType {
    pub fn new(name: &str) -> Result<ScalarFunctionType, PipelineError> {
        match name {
            "abs" => Ok(ScalarFunctionType::Abs),
            "round" => Ok(ScalarFunctionType::Round),
            "ucase" => Ok(ScalarFunctionType::Ucase),
            _ => Err(PipelineError::InvalidFunction(name.to_string())),
        }
    }

    pub(crate) fn evaluate(
        &self,
        args: &[Expression],
        record: &Record,
    ) -> Result<Field, PipelineError> {
        match self {
            ScalarFunctionType::Abs => evaluate_abs(&args[0], record),
            ScalarFunctionType::Round => evaluate_round(&args[0], args.get(1), record),
            ScalarFunctionType::Ucase => evaluate_ucase(&args[0], record),
        }
    }
}

fn evaluate_abs(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        _ => Err(PipelineError::InvalidOperandType("ABS()".to_string())),
    }
}

fn evaluate_round(
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
        _ => Err(PipelineError::InvalidOperandType("ROUND()".to_string())),
    }
}

/// `evaluate_ucase` is a scalar function which converts string, text to upper-case, implementing support for UCASE()
fn evaluate_ucase(arg: &Expression, record: &Record) -> Result<Field, PipelineError> {
    let value = arg.evaluate(record)?;
    match value {
        Field::String(s) => Ok(Field::String(s.to_uppercase())),
        Field::Text(t) => Ok(Field::Text(t.to_uppercase())),
        _ => Err(PipelineError::InvalidFunction(String::from("UCASE() for ") + &*json!({"value":value}).to_string())),
    }
}

#[cfg(test)]
use crate::pipeline::expression::execution::Expression::Literal;

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
