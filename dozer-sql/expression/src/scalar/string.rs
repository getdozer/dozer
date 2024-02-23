use crate::error::Error;
use std::fmt::Write;
use std::fmt::{Display, Formatter};

use crate::execution::{Expression, ExpressionType};

use crate::arg_utils::validate_arg_type;
use crate::scalar::common::ScalarFunctionType;

use dozer_types::types::Record;
use dozer_types::types::{Field, FieldType, Schema};
use like::{Escape, Like};

pub(crate) fn validate_ucase(arg: &Expression, schema: &Schema) -> Result<ExpressionType, Error> {
    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Ucase,
        0,
    )
}

pub fn evaluate_ucase(
    schema: &Schema,
    arg: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let f = arg.evaluate(record, schema)?;
    let v = f.to_string();
    let ret = v.to_uppercase();

    Ok(match arg.get_type(schema)?.return_type {
        FieldType::String => Field::String(ret),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::Text(ret),
    })
}

pub fn validate_concat(args: &[Expression], schema: &Schema) -> Result<ExpressionType, Error> {
    let mut ret_type = FieldType::String;
    for exp in args {
        let r = validate_arg_type(
            exp,
            vec![FieldType::String, FieldType::Text],
            schema,
            ScalarFunctionType::Concat,
            0,
        )?;
        if matches!(r.return_type, FieldType::Text) {
            ret_type = FieldType::Text;
        }
    }
    Ok(ExpressionType::new(
        ret_type,
        false,
        dozer_types::types::SourceDefinition::Dynamic,
        false,
    ))
}

pub fn evaluate_concat(
    schema: &Schema,
    args: &mut [Expression],
    record: &Record,
) -> Result<Field, Error> {
    let mut res_type = FieldType::String;
    let mut res_vec: Vec<String> = Vec::with_capacity(args.len());

    for e in args {
        if matches!(e.get_type(schema)?.return_type, FieldType::Text) {
            res_type = FieldType::Text;
        }
        let f = e.evaluate(record, schema)?;
        let val = f.to_string();
        res_vec.push(val);
    }

    let res_str = res_vec.iter().fold(String::new(), |a, b| a + b.as_str());
    Ok(match res_type {
        FieldType::Text => Field::Text(res_str),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::String
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::String(res_str),
    })
}

pub(crate) fn evaluate_length(
    schema: &Schema,
    arg0: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let f0 = arg0.evaluate(record, schema)?;
    let v0 = f0.to_string();
    Ok(Field::UInt(v0.len() as u64))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrimType {
    Trailing,
    Leading,
    Both,
}

impl Display for TrimType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TrimType::Trailing => f.write_str("TRAILING "),
            TrimType::Leading => f.write_str("LEADING "),
            TrimType::Both => f.write_str("BOTH "),
        }
    }
}

pub fn validate_trim(arg: &Expression, schema: &Schema) -> Result<ExpressionType, Error> {
    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )
}

pub fn evaluate_trim(
    schema: &Schema,
    arg: &mut Expression,
    what: &mut Option<Box<Expression>>,
    typ: &Option<TrimType>,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;
    let arg_value = arg_field.to_string();

    let v1: Vec<_> = match what {
        Some(e) => {
            let f = e.evaluate(record, schema)?;
            f.to_string().chars().collect()
        }
        _ => vec![' '],
    };

    let retval = match typ {
        Some(TrimType::Both) => arg_value.trim_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Leading) => arg_value.trim_start_matches::<&[char]>(&v1).to_string(),
        Some(TrimType::Trailing) => arg_value.trim_end_matches::<&[char]>(&v1).to_string(),
        None => arg_value.trim_matches::<&[char]>(&v1).to_string(),
    };

    Ok(match arg.get_type(schema)?.return_type {
        FieldType::String => Field::String(retval),
        FieldType::UInt
        | FieldType::U128
        | FieldType::Int
        | FieldType::I128
        | FieldType::Float
        | FieldType::Decimal
        | FieldType::Boolean
        | FieldType::Text
        | FieldType::Date
        | FieldType::Timestamp
        | FieldType::Binary
        | FieldType::Json
        | FieldType::Point
        | FieldType::Duration => Field::Text(retval),
    })
}

pub(crate) fn get_like_operator_type(
    arg: &Expression,
    pattern: &Expression,
    schema: &Schema,
) -> Result<ExpressionType, Error> {
    validate_arg_type(
        pattern,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )?;

    validate_arg_type(
        arg,
        vec![FieldType::String, FieldType::Text],
        schema,
        ScalarFunctionType::Concat,
        0,
    )
}

pub fn evaluate_like(
    schema: &Schema,
    arg: &mut Expression,
    pattern: &mut Expression,
    escape: Option<char>,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;
    let arg_value = arg_field.to_string();
    let arg_string = arg_value.as_str();

    let pattern_field = pattern.evaluate(record, schema)?;
    let pattern_value = pattern_field.to_string();
    let pattern_string = pattern_value.as_str();

    if let Some(escape_char) = escape {
        let arg_escape = &arg_string.escape(&escape_char.to_string())?;
        let result =
            Like::<false>::like(arg_escape.as_str(), pattern_string).map(Field::Boolean)?;
        return Ok(result);
    }

    let result = Like::<false>::like(arg_string, pattern_string).map(Field::Boolean)?;
    Ok(result)
}

pub(crate) fn evaluate_to_char(
    schema: &Schema,
    arg: &mut Expression,
    pattern: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let arg_field = arg.evaluate(record, schema)?;

    let pattern_field = pattern.evaluate(record, schema)?;
    let pattern_value = pattern_field.to_string();

    let output = match arg_field {
        Field::Timestamp(value) => value.format(pattern_value.as_str()).to_string(),
        Field::Date(value) => {
            let mut formatted = String::new();
            let format_result = write!(formatted, "{}", value.format(pattern_value.as_str()));
            if format_result.is_ok() {
                formatted
            } else {
                pattern_value
            }
        }
        Field::Null => return Ok(Field::Null),
        _ => {
            return Err(Error::InvalidFunctionArgument {
                function_name: "TO_CHAR".to_string(),
                argument_index: 0,
                argument: arg_field,
            });
        }
    };

    Ok(Field::String(output))
}

pub(crate) fn evaluate_chr(
    schema: &Schema,
    arg: &mut Expression,
    record: &Record,
) -> Result<Field, Error> {
    let value = arg.evaluate(record, schema)?;
    match value {
        Field::UInt(u) => Ok(Field::String((((u % 256) as u8) as char).to_string())),
        Field::U128(u) => Ok(Field::String((((u % 256) as u8) as char).to_string())),
        Field::Int(i) => Ok(Field::String((((i % 256) as u8) as char).to_string())),
        Field::I128(i) => Ok(Field::String((((i % 256) as u8) as char).to_string())),
        Field::Float(_)
        | Field::Decimal(_)
        | Field::Boolean(_)
        | Field::String(_)
        | Field::Text(_)
        | Field::Date(_)
        | Field::Timestamp(_)
        | Field::Binary(_)
        | Field::Json(_)
        | Field::Point(_)
        | Field::Duration(_)
        | Field::Null => Err(Error::InvalidFunctionArgument {
            function_name: ScalarFunctionType::Chr.to_string(),
            argument_index: 0,
            argument: value,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Expression::Literal;

    use proptest::prelude::*;

    #[test]
    fn test_string() {
        proptest!(
            ProptestConfig::with_cases(1000),
            move |(s_val in ".+", s_val1 in ".*", s_val2 in ".*", c_val: char, i_val: i64) | {
                test_like(&s_val, c_val);
                test_ucase(&s_val, c_val);
                test_concat(&s_val1, &s_val2, c_val);
                test_trim(&s_val, c_val);
                test_chr(i_val)
        });
    }

    fn test_like(s_val: &str, c_val: char) {
        let row = Record::new(vec![]);

        // Field::String
        let mut value = Box::new(Literal(Field::String(format!("Hello{}", s_val))));
        let mut pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(true)
        );

        let mut value = Box::new(Literal(Field::String(format!("Hello, {}orld!", c_val))));
        let mut pattern = Box::new(Literal(Field::String("Hello, _orld!".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(true)
        );

        let mut value = Box::new(Literal(Field::String(s_val.to_string())));
        let mut pattern = Box::new(Literal(Field::String("Hello%".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(false)
        );

        let c_value = &s_val[0..0];
        let mut value = Box::new(Literal(Field::String(format!("Hello, {}!", c_value))));
        let mut pattern = Box::new(Literal(Field::String("Hello, _!".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(false)
        );

        // todo: should find the way to generate escape character using proptest
        // let mut value = Box::new(Literal(Field::String(format!("Hello, {}%", c_val))));
        // let mut pattern = Box::new(Literal(Field::String("Hello, %".to_owned())));
        // let escape = Some(c_val);
        //
        // assert_eq!(
        //     evaluate_like(&Schema::default(), &mut value, &mut pattern, escape, &row).unwrap(),
        //     Field::Boolean(true)
        // );

        // Field::Text
        let mut value = Box::new(Literal(Field::Text(format!("Hello{}", s_val))));
        let mut pattern = Box::new(Literal(Field::Text("Hello%".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(true)
        );

        let mut value = Box::new(Literal(Field::Text(format!("Hello, {}orld!", c_val))));
        let mut pattern = Box::new(Literal(Field::Text("Hello, _orld!".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(true)
        );

        let mut value = Box::new(Literal(Field::Text(s_val.to_string())));
        let mut pattern = Box::new(Literal(Field::Text("Hello%".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(false)
        );

        let c_value = &s_val[0..0];
        let mut value = Box::new(Literal(Field::Text(format!("Hello, {}!", c_value))));
        let mut pattern = Box::new(Literal(Field::Text("Hello, _!".to_owned())));

        assert_eq!(
            evaluate_like(&Schema::default(), &mut value, &mut pattern, None, &row).unwrap(),
            Field::Boolean(false)
        );

        // todo: should find the way to generate escape character using proptest
        // let mut value = Box::new(Literal(Field::Text(format!("Hello, {}%", c_val))));
        // let mut pattern = Box::new(Literal(Field::Text("Hello, %".to_owned())));
        // let escape = Some(c_val);
        //
        // assert_eq!(
        //     evaluate_like(&Schema::default(), &mut value, &mut pattern, escape, &row).unwrap(),
        //     Field::Boolean(true)
        // );
    }

    fn test_ucase(s_val: &str, c_val: char) {
        let row = Record::new(vec![]);

        // Field::String
        let mut value = Box::new(Literal(Field::String(s_val.to_string())));
        assert_eq!(
            evaluate_ucase(&Schema::default(), &mut value, &row).unwrap(),
            Field::String(s_val.to_uppercase())
        );

        let mut value = Box::new(Literal(Field::String(c_val.to_string())));
        assert_eq!(
            evaluate_ucase(&Schema::default(), &mut value, &row).unwrap(),
            Field::String(c_val.to_uppercase().to_string())
        );

        // Field::Text
        let mut value = Box::new(Literal(Field::Text(s_val.to_string())));
        assert_eq!(
            evaluate_ucase(&Schema::default(), &mut value, &row).unwrap(),
            Field::Text(s_val.to_uppercase())
        );

        let mut value = Box::new(Literal(Field::Text(c_val.to_string())));
        assert_eq!(
            evaluate_ucase(&Schema::default(), &mut value, &row).unwrap(),
            Field::Text(c_val.to_uppercase().to_string())
        );
    }

    fn test_concat(s_val1: &str, s_val2: &str, c_val: char) {
        let row = Record::new(vec![]);

        // Field::String
        let val1 = Literal(Field::String(s_val1.to_string()));
        let val2 = Literal(Field::String(s_val2.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::String(s_val1.to_string() + s_val2)
            );
        }

        let val1 = Literal(Field::String(s_val2.to_string()));
        let val2 = Literal(Field::String(s_val1.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::String(s_val2.to_string() + s_val1)
            );
        }

        let val1 = Literal(Field::String(s_val1.to_string()));
        let val2 = Literal(Field::String(c_val.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::String(s_val1.to_string() + c_val.to_string().as_str())
            );
        }

        let val1 = Literal(Field::String(c_val.to_string()));
        let val2 = Literal(Field::String(s_val1.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::String(c_val.to_string() + s_val1)
            );
        }

        // Field::Text
        let val1 = Literal(Field::Text(s_val1.to_string()));
        let val2 = Literal(Field::Text(s_val2.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::Text(s_val1.to_string() + s_val2)
            );
        }

        let val1 = Literal(Field::Text(s_val2.to_string()));
        let val2 = Literal(Field::Text(s_val1.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::Text(s_val2.to_string() + s_val1)
            );
        }

        let val1 = Literal(Field::Text(s_val1.to_string()));
        let val2 = Literal(Field::Text(c_val.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::Text(s_val1.to_string() + c_val.to_string().as_str())
            );
        }

        let val1 = Literal(Field::Text(c_val.to_string()));
        let val2 = Literal(Field::Text(s_val1.to_string()));

        if validate_concat(&[val1.clone(), val2.clone()], &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_concat(&Schema::default(), &mut [val1, val2], &row).unwrap(),
                Field::Text(c_val.to_string() + s_val1)
            );
        }
    }

    fn test_trim(s_val1: &str, c_val: char) {
        let row = Record::new(vec![]);

        // Field::String
        let mut value = Literal(Field::String(s_val1.to_string()));
        let what = ' ';

        if validate_trim(&value, &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_trim(&Schema::default(), &mut value, &mut None, &None, &row).unwrap(),
                Field::String(s_val1.trim_matches(what).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut None,
                    &Some(TrimType::Trailing),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_end_matches(what).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut None,
                    &Some(TrimType::Leading),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_start_matches(what).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut None,
                    &Some(TrimType::Both),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_matches(what).to_string())
            );
        }

        let mut value = Literal(Field::String(s_val1.to_string()));
        let mut what = Some(Box::new(Literal(Field::String(c_val.to_string()))));

        if validate_trim(&value, &Schema::default()).is_ok() {
            assert_eq!(
                evaluate_trim(&Schema::default(), &mut value, &mut what, &None, &row).unwrap(),
                Field::String(s_val1.trim_matches(c_val).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut what,
                    &Some(TrimType::Trailing),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_end_matches(c_val).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut what,
                    &Some(TrimType::Leading),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_start_matches(c_val).to_string())
            );
            assert_eq!(
                evaluate_trim(
                    &Schema::default(),
                    &mut value,
                    &mut what,
                    &Some(TrimType::Both),
                    &row
                )
                .unwrap(),
                Field::String(s_val1.trim_matches(c_val).to_string())
            );
        }
    }

    fn test_chr(val: i64) {
        let row = Record::new(vec![]);

        // Field::String
        let mut value = Box::new(Literal(Field::Int(val)));
        assert_eq!(
            evaluate_chr(&Schema::default(), &mut value, &row).unwrap(),
            Field::String((((val % 256) as u8) as char).to_string())
        );
    }
}
