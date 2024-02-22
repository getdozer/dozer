use std::collections::HashMap;

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, ParseError, Utc},
    ordered_float::OrderedFloat,
    rust_decimal::prelude::ToPrimitive,
    types::{Field, FieldType, Record, Schema},
};

use crate::connector::Error;

use super::parse::ParsedValue;

pub fn map_row(mut row: HashMap<String, ParsedValue>, schema: &Schema) -> Result<Record, Error> {
    let mut values = vec![];
    for field in &schema.fields {
        let value = row
            .remove(&field.name)
            .ok_or_else(|| Error::FieldNotFound(field.name.clone()))?;
        values.push(map_value(value, field.typ, field.nullable, &field.name)?);
    }

    Ok(Record::new(values))
}

fn map_value(
    value: ParsedValue,
    typ: FieldType,
    nullable: bool,
    name: &str,
) -> Result<Field, Error> {
    match (value, typ, nullable) {
        (ParsedValue::Null, _, false) => Err(Error::NullValue(name.to_string())),
        (ParsedValue::Null, _, true) => Ok(Field::Null),
        (ParsedValue::String(string), FieldType::Float, _) => {
            Ok(Field::Float(OrderedFloat(string.parse()?)))
        }
        (ParsedValue::Number(number), FieldType::Float, _) => Ok(Field::Float(OrderedFloat(
            number
                .to_f64()
                .ok_or_else(|| Error::FloatOverflow(number))?,
        ))),
        (ParsedValue::String(string), FieldType::Decimal, _) => Ok(Field::Decimal(string.parse()?)),
        (ParsedValue::Number(number), FieldType::Decimal, _) => Ok(Field::Decimal(number)),
        (ParsedValue::Number(number), FieldType::Int, _) => {
            Ok(Field::Int(number.to_i64().unwrap()))
        }
        (ParsedValue::Number(number), FieldType::UInt, _) => {
            Ok(Field::UInt(number.to_u64().unwrap()))
        }
        (ParsedValue::String(string), FieldType::String, _) => Ok(Field::String(string)),
        (ParsedValue::Number(_), FieldType::String, _) => Err(Error::TypeMismatch {
            field: name.to_string(),
            expected: FieldType::String,
            actual: FieldType::Decimal,
        }),
        (_, FieldType::Binary, _) => unimplemented!("parse binary from redo sql"),
        (ParsedValue::String(string), FieldType::Date, _) => Ok(Field::Date(
            parse_date(&string).map_err(|e| Error::ParseDateTime(e, string))?,
        )),
        (ParsedValue::Number(_), FieldType::Date, _) => Err(Error::TypeMismatch {
            field: name.to_string(),
            expected: FieldType::Date,
            actual: FieldType::Decimal,
        }),
        (ParsedValue::String(string), FieldType::Timestamp, _) => Ok(Field::Timestamp(
            parse_date_time(&string).map_err(|e| Error::ParseDateTime(e, string))?,
        )),
        (ParsedValue::Number(_), FieldType::Timestamp, _) => Err(Error::TypeMismatch {
            field: name.to_string(),
            expected: FieldType::Timestamp,
            actual: FieldType::Decimal,
        }),
        _ => unreachable!(),
    }
}

fn parse_date(string: &str) -> Result<NaiveDate, ParseError> {
    NaiveDate::parse_from_str(string, "%d-%b-%y")
}

fn parse_date_time(string: &str) -> Result<DateTime<FixedOffset>, ParseError> {
    let date_time = NaiveDateTime::parse_from_str(string, "%d-%b-%y %I.%M.%S%.6f %p")?;
    Ok(Ok(DateTime::<Utc>::from_naive_utc_and_offset(date_time, Utc))?.fixed_offset())
}
