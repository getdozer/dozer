use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::error::Error;
use crate::execution::Expression;

fn boolean_operand(field: Field, operator: &str) -> Result<Option<bool>, Error> {
    match field {
        Field::Boolean(value) => Ok(Some(value)),
        Field::Null => Ok(None),
        other => Err(Error::InvalidType(other, operator.to_string())),
    }
}

pub fn evaluate_and(
    schema: &Schema,
    left: &mut Expression,
    right: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let left = boolean_operand(left.evaluate(record, schema)?, "AND")?;
    let right = boolean_operand(right.evaluate(record, schema)?, "AND")?;
    Ok(match (left, right) {
        (Some(false), _) | (_, Some(false)) => Field::Boolean(false),
        (Some(true), Some(true)) => Field::Boolean(true),
        _ => Field::Null,
    })
}

pub fn evaluate_or(
    schema: &Schema,
    left: &mut Expression,
    right: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let left = boolean_operand(left.evaluate(record, schema)?, "OR")?;
    let right = boolean_operand(right.evaluate(record, schema)?, "OR")?;
    Ok(match (left, right) {
        (Some(true), _) | (_, Some(true)) => Field::Boolean(true),
        (Some(false), Some(false)) => Field::Boolean(false),
        _ => Field::Null,
    })
}

pub fn evaluate_not(
    schema: &Schema,
    value: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let value_p = value.evaluate(record, schema)?;

    match value_p {
        Field::Boolean(value_v) => Ok(Field::Boolean(!value_v)),
        Field::Null => Ok(Field::Null),
        Field::UInt(_)
        | Field::U128(_)
        | Field::Int(_)
        | Field::Int8(_)
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
        let mut l = Box::new(Literal(Field::Boolean(bool1)));
        let mut r = Box::new(Literal(Field::Boolean(bool2)));
        assert!(matches!(
            evaluate_and(&Schema::default(), &mut l, &mut r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_null_and(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let expected = if f1 == Field::Boolean(false) || f2 == Field::Boolean(false) {
            Field::Boolean(false)
        } else {
            Field::Null
        };
        let mut l = Box::new(Literal(f1));
        let mut r = Box::new(Literal(f2));
        assert_eq!(
            evaluate_and(&Schema::default(), &mut l, &mut r, &row).unwrap(),
            expected
        );
    }

    fn _test_bool_bool_or(bool1: bool, bool2: bool) {
        let row = Record::new(vec![]);
        let mut l = Box::new(Literal(Field::Boolean(bool1)));
        let mut r = Box::new(Literal(Field::Boolean(bool2)));
        assert!(matches!(
            evaluate_or(&Schema::default(), &mut l, &mut r, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_null_or(_bool: bool) {
        let row = Record::new(vec![]);
        let mut l = Box::new(Literal(Field::Boolean(_bool)));
        let mut r = Box::new(Literal(Field::Null));
        assert_eq!(
            evaluate_or(&Schema::default(), &mut l, &mut r, &row).unwrap(),
            if _bool {
                Field::Boolean(true)
            } else {
                Field::Null
            }
        );
    }

    fn _test_null_bool_or(_bool: bool) {
        let row = Record::new(vec![]);
        let mut l = Box::new(Literal(Field::Null));
        let mut r = Box::new(Literal(Field::Boolean(_bool)));
        assert_eq!(
            evaluate_or(&Schema::default(), &mut l, &mut r, &row).unwrap(),
            if _bool {
                Field::Boolean(true)
            } else {
                Field::Null
            }
        );
    }

    fn _test_bool_not(bool: bool) {
        let row = Record::new(vec![]);
        let mut v = Box::new(Literal(Field::Boolean(bool)));
        assert!(matches!(
            evaluate_not(&Schema::default(), &mut v, &row)
                .unwrap_or_else(|e| panic!("{}", e.to_string())),
            Field::Boolean(_ans)
        ));
    }

    fn _test_bool_non_bool_and(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let mut l = Box::new(Literal(f1));
        let mut r = Box::new(Literal(f2));
        assert!(evaluate_and(&Schema::default(), &mut l, &mut r, &row).is_err());
    }

    fn _test_bool_non_bool_or(f1: Field, f2: Field) {
        let row = Record::new(vec![]);
        let mut l = Box::new(Literal(f1));
        let mut r = Box::new(Literal(f2));
        assert!(evaluate_or(&Schema::default(), &mut l, &mut r, &row).is_err());
    }
}
