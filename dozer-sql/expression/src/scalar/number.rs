use crate::error::Error;
use crate::execution::Expression;
use crate::scalar::common::ScalarFunctionType;
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};
use num_traits::{Float, ToPrimitive};

pub(crate) fn evaluate_abs(
    schema: &Schema,
    arg: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::UInt(u) => Ok(Field::UInt(u)),
        Field::U128(u) => Ok(Field::U128(u)),
        Field::Int(i) => Ok(Field::Int(i.abs())),
        Field::I128(i) => Ok(Field::I128(i.abs())),
        Field::Float(f) => Ok(Field::Float(f.abs())),
        Field::Decimal(d) => Ok(Field::Decimal(d.abs())),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Date(_)
        | Field::Timestamp(_)
        | Field::Binary(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => Err(Error::InvalidFunctionArgument {
            function_name: ScalarFunctionType::Abs.to_string(),
            argument_index: 0,
            argument: value,
        }),
    }
}

pub(crate) fn evaluate_round(
    schema: &Schema,
    arg: &mut Expression,
    decimals: Option<&mut Expression>,
    record: &Record,
) -> Result<Field, Error> {
    let value = arg.evaluate(record, schema)?;
    let mut places = 0;
    if let Some(expression) = decimals {
        let field = expression.evaluate(record, schema)?;
        match field {
            Field::UInt(u) => places = u as i32,
            Field::U128(u) => places = u as i32,
            Field::Int(i) => places = i as i32,
            Field::I128(i) => places = i as i32,
            Field::Float(f) => places = f.round().0 as i32,
            Field::Decimal(d) => {
                places = d
                    .to_i32()
                    .ok_or(Error::InvalidCast {
                        from: field,
                        to: FieldType::Decimal,
                    })
                    .unwrap()
            }
            Field::Boolean(_)
            | Field::String(_)
            | Field::Text(_)
            | Field::Date(_)
            | Field::Timestamp(_)
            | Field::Binary(_)
            | Field::Json(_)
            | Field::Point(_)
            | Field::Duration(_)
            | Field::Null => {} // Truncate value to 0 decimals
        }
    }
    let order = OrderedFloat(10.0_f64.powi(places));

    match value {
        Field::UInt(u) => Ok(Field::UInt(u)),
        Field::U128(u) => Ok(Field::U128(u)),
        Field::Int(i) => Ok(Field::Int(i)),
        Field::I128(i) => Ok(Field::I128(i)),
        Field::Float(f) => Ok(Field::Float((f * order).round() / order)),
        Field::Decimal(d) => Ok(Field::Decimal(d.round_dp(places as u32))),
        Field::Null => Ok(Field::Null),
        Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Date(_)
        | Field::Timestamp(_)
        | Field::Binary(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_) => Err(Error::InvalidFunctionArgument {
            function_name: ScalarFunctionType::Round.to_string(),
            argument_index: 0,
            argument: value,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use dozer_types::ordered_float::OrderedFloat;
    use dozer_types::types::Record;
    use dozer_types::types::{Field, Schema};
    use proptest::prelude::*;
    use std::ops::Neg;
    use Expression::Literal;

    #[test]
    fn test_abs() {
        proptest!(ProptestConfig::with_cases(1000), |(i_num in 0i64..100000000i64, f_num in 0f64..100000000f64)| {
            let row = Record::new(vec![]);

            let mut v = Box::new(Literal(Field::Int(i_num.neg())));
            assert_eq!(
                evaluate_abs(&Schema::default(), &mut v, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Int(i_num)
            );

            let row = Record::new(vec![]);

            let mut v = Box::new(Literal(Field::Float(OrderedFloat(f_num.neg()))));
            assert_eq!(
                evaluate_abs(&Schema::default(), &mut v, &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num))
            );
        });
    }

    #[test]
    fn test_round() {
        proptest!(ProptestConfig::with_cases(1000), |(i_num: i64, f_num: f64, i_pow: i32, f_pow: f32)| {
            let row = Record::new(vec![]);

            let mut v = Box::new(Literal(Field::Int(i_num)));
            let d = &mut Box::new(Literal(Field::Int(0)));
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Int(i_num)
            );

            let mut v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
            let d = &mut Box::new(Literal(Field::Int(0)));
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num.round()))
            );

            let mut v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
            let d = &mut Box::new(Literal(Field::Int(i_pow as i64)));
            let order = 10.0_f64.powi(i_pow);
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat((f_num * order).round() / order))
            );

            let mut v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
            let d = &mut Box::new(Literal(Field::Float(OrderedFloat(f_pow as f64))));
            let order = 10.0_f64.powi(f_pow.round() as i32);
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat((f_num * order).round() / order))
            );

            let mut v = Box::new(Literal(Field::Float(OrderedFloat(f_num))));
            let d = &mut Box::new(Literal(Field::String(f_pow.to_string())));
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Float(OrderedFloat(f_num.round()))
            );

            let mut v = Box::new(Literal(Field::Null));
            let d = &mut Box::new(Literal(Field::String(i_pow.to_string())));
            assert_eq!(
                evaluate_round(&Schema::default(), &mut v, Some(d), &row)
                    .unwrap_or_else(|e| panic!("{}", e.to_string())),
                Field::Null
            );
        });
    }
}
