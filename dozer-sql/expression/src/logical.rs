use dozer_types::types::Record;
use dozer_types::types::{Field, Schema};

use crate::error::Error;
use crate::execution::Expression;

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

fn boolean_operand(value: Field, operator: &str) -> Result<Option<bool>, Error> {
    match value {
        Field::Boolean(value) => Ok(Some(value)),
        Field::Null => Ok(None),
        value => Err(Error::InvalidType(value, operator.to_owned())),
    }
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

    use crate::operator::{BinaryOperatorType, UnaryOperatorType};
    use dozer_types::types::Record;
    use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
    use dozer_types::{ordered_float::OrderedFloat, rust_decimal::Decimal};
    use proptest::prelude::*;
    use Expression::Literal;

    #[test]
    fn logical_truth_tables_preserve_unknown() {
        use Field::{Boolean, Null};
        let cases = [
            (Boolean(true), Boolean(true), Boolean(true), Boolean(true)),
            (Boolean(true), Boolean(false), Boolean(false), Boolean(true)),
            (Boolean(true), Null, Null, Boolean(true)),
            (Boolean(false), Boolean(true), Boolean(false), Boolean(true)),
            (
                Boolean(false),
                Boolean(false),
                Boolean(false),
                Boolean(false),
            ),
            (Boolean(false), Null, Boolean(false), Null),
            (Null, Boolean(true), Null, Boolean(true)),
            (Null, Boolean(false), Boolean(false), Null),
            (Null, Null, Null, Null),
        ];
        let schema = Schema::default();
        let record = Record::new(vec![]);
        for (left, right, expected_and, expected_or) in cases {
            let mut left = Literal(left);
            let mut right = Literal(right);
            assert_eq!(
                evaluate_and(&schema, &mut left, &mut right, &record).unwrap(),
                expected_and
            );
            assert_eq!(
                evaluate_or(&schema, &mut left, &mut right, &record).unwrap(),
                expected_or
            );
        }
    }

    #[test]
    fn negating_a_compound_unknown_does_not_select_it() {
        let schema = Schema {
            fields: vec![FieldDefinition::new(
                "membership".into(),
                FieldType::Boolean,
                true,
                SourceDefinition::Dynamic,
            )],
            primary_index: vec![],
        };
        let record = Record::new(vec![Field::Null]);
        for (operator, other) in [
            (BinaryOperatorType::And, true),
            (BinaryOperatorType::Or, false),
        ] {
            let mut expression = Expression::UnaryOperator {
                operator: UnaryOperatorType::Not,
                arg: Box::new(Expression::BinaryOperator {
                    left: Box::new(Expression::Column { index: 0 }),
                    operator,
                    right: Box::new(Literal(Field::Boolean(other))),
                }),
            };
            assert_eq!(expression.evaluate(&record, &schema).unwrap(), Field::Null);
            assert!(expression.get_type(&schema).unwrap().nullable);
        }
    }

    #[test]
    fn logical_result_nullability_includes_both_operands() {
        for (left_nullable, right_nullable) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let schema = Schema {
                fields: vec![
                    FieldDefinition::new(
                        "left".into(),
                        FieldType::Boolean,
                        left_nullable,
                        SourceDefinition::Dynamic,
                    ),
                    FieldDefinition::new(
                        "right".into(),
                        FieldType::Boolean,
                        right_nullable,
                        SourceDefinition::Dynamic,
                    ),
                ],
                primary_index: vec![],
            };
            for operator in [BinaryOperatorType::And, BinaryOperatorType::Or] {
                let expression = Expression::BinaryOperator {
                    left: Box::new(Expression::Column { index: 0 }),
                    operator,
                    right: Box::new(Expression::Column { index: 1 }),
                };
                let result_type = expression.get_type(&schema).unwrap();
                assert_eq!(result_type.return_type, FieldType::Boolean);
                assert_eq!(result_type.nullable, left_nullable || right_nullable);
            }
        }
    }

    #[test]
    fn unknown_does_not_hide_an_invalid_operand_type() {
        let record = Record::new(vec![]);
        for (left, right) in [(Field::Null, Field::Int(1)), (Field::Int(1), Field::Null)] {
            let mut left = Literal(left);
            let mut right = Literal(right);
            assert!(matches!(
                evaluate_and(&Schema::default(), &mut left, &mut right, &record),
                Err(Error::InvalidType(Field::Int(1), _))
            ));
            assert!(matches!(
                evaluate_or(&Schema::default(), &mut left, &mut right, &record),
                Err(Error::InvalidType(Field::Int(1), _))
            ));
        }
    }

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
        assert_eq!(
            evaluate_and(&Schema::default(), &mut l, &mut r, &row).unwrap(),
            Field::Boolean(bool1 && bool2)
        );
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
        assert_eq!(
            evaluate_or(&Schema::default(), &mut l, &mut r, &row).unwrap(),
            Field::Boolean(bool1 || bool2)
        );
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
        assert_eq!(
            evaluate_not(&Schema::default(), &mut v, &row).unwrap(),
            Field::Boolean(!bool)
        );
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
