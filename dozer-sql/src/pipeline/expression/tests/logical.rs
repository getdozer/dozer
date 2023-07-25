use crate::pipeline::expression::execution::Expression::Literal;
use crate::pipeline::expression::logical::{evaluate_and, evaluate_not, evaluate_or};
use dozer_core::processor_record::ProcessorRecord;
use dozer_types::types::{Field, Schema};
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
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(Field::Boolean(bool1)));
    let r = Box::new(Literal(Field::Boolean(bool2)));
    assert!(
        matches!(
            evaluate_and(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        )
    );
}

fn _test_bool_null_and(f1: Field, f2: Field) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    assert!(matches!(
        evaluate_and(&Schema::default(), &l, &r, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(false)
    ));
}

fn _test_bool_bool_or(bool1: bool, bool2: bool) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(Field::Boolean(bool1)));
    let r = Box::new(Literal(Field::Boolean(bool2)));
    assert!(matches!(
        evaluate_or(&Schema::default(), &l, &r, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_ans)
    ));
}

fn _test_bool_null_or(_bool: bool) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(Field::Boolean(_bool)));
    let r = Box::new(Literal(Field::Null));
    assert!(matches!(
        evaluate_or(&Schema::default(), &l, &r, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_bool)
    ));
}

fn _test_null_bool_or(_bool: bool) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(Field::Null));
    let r = Box::new(Literal(Field::Boolean(_bool)));
    assert!(matches!(
        evaluate_or(&Schema::default(), &l, &r, &row)
            .unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_bool)
    ));
}

fn _test_bool_not(bool: bool) {
    let row = ProcessorRecord::new();
    let v = Box::new(Literal(Field::Boolean(bool)));
    assert!(matches!(
        evaluate_not(&Schema::default(), &v, &row).unwrap_or_else(|e| panic!("{}", e.to_string())),
        Field::Boolean(_ans)
    ));
}

fn _test_bool_non_bool_and(f1: Field, f2: Field) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    assert!(evaluate_and(&Schema::default(), &l, &r, &row).is_err());
}

fn _test_bool_non_bool_or(f1: Field, f2: Field) {
    let row = ProcessorRecord::new();
    let l = Box::new(Literal(f1));
    let r = Box::new(Literal(f2));
    assert!(evaluate_or(&Schema::default(), &l, &r, &row).is_err());
}
