use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::{Expression, ExpressionExecutor};
use dozer_types::types::{Field, Record, Schema};

pub fn evaluate_and(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record, schema)? {
        Field::Boolean(true) => match right.evaluate(record, schema)? {
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "AND".to_string(),
            )),
        },
        Field::Boolean(false) => match right.evaluate(record, schema)? {
            Field::Boolean(true) => Ok(Field::Boolean(false)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "AND".to_string(),
            )),
        },
        Field::Null => Ok(Field::Boolean(false)),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "AND".to_string(),
        )),
    }
}

pub fn evaluate_or(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    match left.evaluate(record, schema)? {
        Field::Boolean(true) => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(true)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(true)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "OR".to_string(),
            )),
        },
        Field::Boolean(false) | Field::Null => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(false)),
            not_supported_field => Err(PipelineError::InvalidType(
                not_supported_field,
                "OR".to_string(),
            )),
        },
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "OR".to_string(),
        )),
    }
}

pub fn evaluate_not(
    schema: &Schema,
    value: &Expression,
    record: &Record,
) -> Result<Field, PipelineError> {
    let value_p = value.evaluate(record, schema)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        Field::Null => Ok(Field::Null),
        not_supported_field => Err(PipelineError::InvalidType(
            not_supported_field,
            "NOT".to_string(),
        )),
    }
}

use crate::pipeline::expression::execution::Expression::Literal;
use dozer_types::{ordered_float::OrderedFloat, rust_decimal::Decimal};
#[cfg(test)]
use proptest::prelude::*;

#[test]
fn test_logical() {
    proptest!(
        ProptestConfig::with_cases(1000),
        move |(bool1: bool, bool2: bool, u_num: u64, i_num: i64, f_num: f64, str in ".*")| {
        _test_bool_bool_and(bool1, bool2);
        _test_bool_null_and(Field::Boolean(bool1), Field::Null);
        _test_bool_null_and(Field::Null, Field::Boolean(bool1));

        _test_bool_bool_or(bool1, bool2);
        _test_bool_null_or(bool1);
        _test_null_bool_or(bool2);

        _test_bool_not(bool2);

        _test_bool_non_bool_and(Field::UInt(u_num), Field::Boolean(bool1));
        _test_bool_non_bool_and(Field::Int(i_num), Field::Boolean(bool1));
        _test_bool_non_bool_and(Field::Float(OrderedFloat(f_num)), Field::Boolean(bool1));
        _test_bool_non_bool_and(Field::Decimal(Decimal::from(u_num)), Field::Boolean(bool1));
        _test_bool_non_bool_and(Field::String(str.clone()), Field::Boolean(bool1));
        _test_bool_non_bool_and(Field::Text(str.clone()), Field::Boolean(bool1));

        _test_bool_non_bool_and(Field::Boolean(bool2), Field::UInt(u_num));
        _test_bool_non_bool_and(Field::Boolean(bool2), Field::Int(i_num));
        _test_bool_non_bool_and(Field::Boolean(bool2), Field::Float(OrderedFloat(f_num)));
        _test_bool_non_bool_and(Field::Boolean(bool2), Field::Decimal(Decimal::from(u_num)));
        _test_bool_non_bool_and(Field::Boolean(bool2), Field::String(str.clone()));
        _test_bool_non_bool_and(Field::Boolean(bool2), Field::Text(str.clone()));

        _test_bool_non_bool_or(Field::UInt(u_num), Field::Boolean(bool1));
        _test_bool_non_bool_or(Field::Int(i_num), Field::Boolean(bool1));
        _test_bool_non_bool_or(Field::Float(OrderedFloat(f_num)), Field::Boolean(bool1));
        _test_bool_non_bool_or(Field::Decimal(Decimal::from(u_num)), Field::Boolean(bool1));
        _test_bool_non_bool_or(Field::String(str.clone()), Field::Boolean(bool1));
        _test_bool_non_bool_or(Field::Text(str.clone()), Field::Boolean(bool1));

        _test_bool_non_bool_or(Field::Boolean(bool2), Field::UInt(u_num));
        _test_bool_non_bool_or(Field::Boolean(bool2), Field::Int(i_num));
        _test_bool_non_bool_or(Field::Boolean(bool2), Field::Float(OrderedFloat(f_num)));
        _test_bool_non_bool_or(Field::Boolean(bool2), Field::Decimal(Decimal::from(u_num)));
        _test_bool_non_bool_or(Field::Boolean(bool2), Field::String(str.clone()));
        _test_bool_non_bool_or(Field::Boolean(bool2), Field::Text(str));
    });
}

fn _test_bool_bool_and(bool1: bool, bool2: bool) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(Field::Boolean(bool1)));
    let r = Box::new(Literal(Field::Boolean(bool2)));
    let _ans = bool1 & bool2;
    assert!(
        matches!(
            evaluate_and(&Schema::empty(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        )
    );
}

fn _test_bool_null_and(f1: Field, f2: Field) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    assert!(matches!(
        evaluate_and(&Schema::empty(), &l, &r, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    ));
}

fn _test_bool_bool_or(bool1: bool, bool2: bool) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(Field::Boolean(bool1)));
    let r = Box::new(Literal(Field::Boolean(bool2)));
    let _ans = bool1 | bool2;
    assert!(matches!(
        evaluate_or(&Schema::empty(), &l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_ans)
    ));
}

fn _test_bool_null_or(_bool: bool) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(Field::Boolean(_bool)));
    let r = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_or(&Schema::empty(), &l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_bool)
    ));
}

fn _test_null_bool_or(_bool: bool) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(Field::Null));
    let r = Box::new(Literal(Field::Boolean(_bool)));
    assert!(matches!(
        evaluate_or(&Schema::empty(), &l, &r, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_bool)
    ));
}

fn _test_bool_not(bool: bool) {
    let row = Record::new(None, vec![], None);
    let v = Box::new(Literal(Field::Boolean(bool)));
    let _ans = !bool;
    assert!(matches!(
        evaluate_not(&Schema::empty(), &v, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_ans)
    ));
}

fn _test_bool_non_bool_and(f1: Field, f2: Field) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    let _ans = evaluate_and(&Schema::empty(), &l, &r, &row);
    assert!(evaluate_and(&Schema::empty(), &l, &r, &row).is_err());
}

fn _test_bool_non_bool_or(f1: Field, f2: Field) {
    let row = Record::new(None, vec![], None);
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    assert!(evaluate_or(&Schema::empty(), &l, &r, &row).is_err());
}
