use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::error::Error;
use crate::execution::Expression;

pub fn evaluate_and(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, Error> {
    let l_field = left.evaluate(record, schema)?;
    let r_field = right.evaluate(record, schema)?;
    match l_field {
        Field::Boolean(true) => match r_field {
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(Error::InvalidType(r_field, "AND".to_string())),
        },
        Field::Boolean(false) => match r_field {
            Field::Boolean(true) => Ok(Field::Boolean(false)),
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(Error::InvalidType(r_field, "AND".to_string())),
        },
        Field::Null => Ok(Field::Boolean(false)),
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(Error::InvalidType(l_field, "AND".to_string())),
    }
}

pub fn evaluate_or(
    schema: &Schema,
    left: &Expression,
    right: &Expression,
    record: &Record,
) -> Result<Field, Error> {
    let l_field = left.evaluate(record, schema)?;
    let r_field = right.evaluate(record, schema)?;
    match l_field {
        Field::Boolean(true) => match r_field {
            Field::Boolean(false) => Ok(Field::Boolean(true)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(true)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(Error::InvalidType(r_field, "OR".to_string())),
        },
        Field::Boolean(false) | Field::Null => match right.evaluate(record, schema)? {
            Field::Boolean(false) => Ok(Field::Boolean(false)),
            Field::Boolean(true) => Ok(Field::Boolean(true)),
            Field::Null => Ok(Field::Boolean(false)),
            Field::UInt(_)
            | Field::U128(_)
            | Field::Int(_)
            | Field::I128(_)
            | Field::Float(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Binary(_)
            | Field::Decimal(_)
            | Field::Timestamp(_)
            | Field::Date(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_) => Err(Error::InvalidType(r_field, "OR".to_string())),
        },
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(Error::InvalidType(l_field, "OR".to_string())),
    }
}

pub fn evaluate_not(schema: &Schema, value: &Expression, record: &Record) -> Result<Field, Error> {
    let value_p = value.evaluate(record, schema)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        Field::Null => Ok(Field::Null),
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::I128(_)
        | Field::Float(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Binary(_)
        | Field::Decimal(_)
        | Field::Timestamp(_)
        | Field::Date(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(Error::InvalidType(value_p, "NOT".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_types::types::Record;
    use dozer_types::types::{Field, Schema};
    use dozer_types::{ordered_float::OrderedFloat, rust_decimal::Decimal};
    use proptest::prelude::*;
    use Expression::Literal;

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
        let row = Record::new(vec![]);
        let l = Box::new(Literal(Field::Boolean(bool1)));
        let r = Box::new(Literal(Field::Boolean(bool2)));
        assert!(matches!(
            evaluate_and(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_null_and(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(f1));
        let r = Box::new(Literal(f2));
        assert!(matches!(
            evaluate_and(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(false)
        ));
    }

    fn _test_bool_bool_or(bool1: bool, bool2: bool) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(Field::Boolean(bool1)));
        let r = Box::new(Literal(Field::Boolean(bool2)));
        assert!(matches!(
            evaluate_or(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_null_or(_bool: bool) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(Field::Boolean(_bool)));
        let r = Box::new(Literal(Field::Null));
        assert!(matches!(
            evaluate_or(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_bool)
        ));
    }

    fn _test_null_bool_or(_bool: bool) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(Field::Null));
        let r = Box::new(Literal(Field::Boolean(_bool)));
        assert!(matches!(
            evaluate_or(&Schema::default(), &l, &r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_bool)
        ));
    }

    fn _test_bool_not(bool: bool) {
        let row = Record::new(vec![]);
        let v = Box::new(Literal(Field::Boolean(bool)));
        assert!(matches!(
            evaluate_not(&Schema::default(), &v, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_non_bool_and(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(f1));
        let r = Box::new(Literal(f2));
        assert!(evaluate_and(&Schema::default(), &l, &r, &row).is_err());
    }

    fn _test_bool_non_bool_or(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let l = Box::new(Literal(f1));
        let r = Box::new(Literal(f2));
        assert!(evaluate_or(&Schema::default(), &l, &r, &row).is_err());
    }
}
