use std::borrow::Cow;

use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, Utc},
    ordered_float::OrderedFloat,
    types::{Field, FieldType, Record, Schema},
};
use memchr::memchr;

use crate::connector::{Error, ParseDateError, Result};

use super::parse::ParsedRow;

pub fn map_row(row: ParsedRow, schema: &Schema) -> Result<Record> {
    let mut values = vec![];
    for (field, value) in schema.fields.iter().zip(row) {
        if field.name == "INGESTED_AT" {
            values.push(Field::Timestamp(
                dozer_ingestion_connector::dozer_types::chrono::offset::Utc::now()
                    .with_timezone(&FixedOffset::east_opt(0).unwrap()),
            ));
            continue;
        }
        values.push(map_value(value, field.typ, field.nullable, &field.name)?);
    }

    Ok(Record::new(values))
}

fn map_value(value: Option<Cow<str>>, typ: FieldType, nullable: bool, name: &str) -> Result<Field> {
    let Some(string) = value else {
        if nullable {
            return Ok(Field::Null);
        } else {
            return Err(Error::NullValue(name.to_owned()));
        }
    };
    match typ {
        FieldType::Float => Ok(Field::Float(OrderedFloat(string.parse()?))),
        FieldType::Decimal => {
            let string = string.replace(',', "");
            Ok(Field::Decimal(
                string
                    .parse()
                    .map_err(|e| Error::NumberToDecimal(e, string))?,
            ))
        }
        FieldType::Int => Ok(Field::Int(
            string
                .parse()
                .map_err(|_| Error::ParseIntFailed(string.into()))?,
        )),
        FieldType::UInt => Ok(Field::UInt(
            string
                .parse()
                .map_err(|_| Error::ParseUIntFailed(string.into()))?,
        )),
        FieldType::String => Ok(Field::String(string.into())),
        FieldType::Binary => unimplemented!("parse binary from redo sql"),
        FieldType::Date => Ok(Field::Date(
            parse_date(&string).map_err(|e| Error::ParseDateTime(e, string.into()))?,
        )),
        FieldType::Timestamp => Ok(Field::Timestamp(
            parse_date_time(&string).map_err(|e| Error::ParseDateTime(e, string.into()))?,
        )),
        _ => unreachable!(),
    }
}

fn parse_date(string: &str) -> std::result::Result<NaiveDate, ParseDateError> {
    const TO_DATE: &str = "TO_DATE('";

    let date = string.get(TO_DATE.len()..).ok_or(ParseDateError::Oracle)?;
    let end = memchr(b'\'', date.as_bytes()).ok_or(ParseDateError::Oracle)?;
    Ok(NaiveDate::parse_from_str(&date[..end], "%d-%m-%Y")?)
}

fn parse_date_time(string: &str) -> std::result::Result<DateTime<FixedOffset>, ParseDateError> {
    const TO_TIMESTAMP: &str = "TO_TIMESTAMP('";

    let date = string
        .get(TO_TIMESTAMP.len()..)
        .ok_or(ParseDateError::Oracle)?;
    let end = memchr(b'\'', date.as_bytes()).ok_or(ParseDateError::Oracle)?;
    let date_time = NaiveDateTime::parse_from_str(dbg!(&date[..end]), "%d-%m-%Y %I.%M.%S%.6f %p")?;
    Ok(DateTime::<Utc>::from_naive_utc_and_offset(date_time, Utc).fixed_offset())
}

#[cfg(test)]
mod tests {
    use dozer_ingestion_connector::dozer_types::chrono::{self, DateTime};

    #[test]
    fn test_parse_date() {
        let date = super::parse_date("TO_DATE('01-01-2021','DD-MM-YYYY')").unwrap();
        assert_eq!(date, chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap());
    }

    #[test]
    fn test_parse_timestamp() {
        let date = super::parse_date_time("TO_TIMESTAMP('01-01-2021 05.00.00.024589 AM')").unwrap();
        assert_eq!(
            date,
            DateTime::parse_from_rfc3339("2021-01-01T05:00:00.024589Z").unwrap()
        );
    }
}
