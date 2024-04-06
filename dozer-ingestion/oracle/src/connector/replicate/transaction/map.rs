use dozer_ingestion_connector::dozer_types::{
    chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, ParseError, Utc},
    ordered_float::OrderedFloat,
    rust_decimal::prelude::ToPrimitive,
    types::{Field, FieldType, Operation, Record, Schema},
};

use crate::connector::Error;

use super::{
    parse::{ParsedOperation, ParsedOperationKind, ParsedRow, ParsedTransaction, ParsedValue},
    Transaction,
};

#[derive(Debug, Clone)]
pub struct Mapper {
    schemas: Vec<Schema>,
}

impl Mapper {
    pub fn new(schemas: Vec<Schema>) -> Self {
        Self { schemas }
    }

    pub fn process<'a>(
        &'a self,
        iterator: impl Iterator<Item = Result<ParsedTransaction, Error>> + 'a,
    ) -> impl Iterator<Item = Result<Transaction, Error>> + 'a {
        Processor {
            iterator,
            mapper: self,
        }
    }

    fn map(&self, operation: ParsedOperation) -> Result<(usize, Operation), Error> {
        let schema = &self.schemas[operation.table_index];
        Ok((
            operation.table_index,
            match operation.kind {
                ParsedOperationKind::Insert(row) => Operation::Insert {
                    new: map_row(row, schema)?,
                },
                ParsedOperationKind::Delete(row) => Operation::Delete {
                    old: map_row(row, schema)?,
                },
                ParsedOperationKind::Update { old, new } => Operation::Update {
                    old: map_row(old, schema)?,
                    new: map_row(new, schema)?,
                },
            },
        ))
    }
}

#[derive(Debug)]
struct Processor<'a, I: Iterator<Item = Result<ParsedTransaction, Error>>> {
    iterator: I,
    mapper: &'a Mapper,
}

impl<'a, I: Iterator<Item = Result<ParsedTransaction, Error>>> Iterator for Processor<'a, I> {
    type Item = Result<Transaction, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let transaction = match self.iterator.next()? {
            Ok(transaction) => transaction,
            Err(err) => return Some(Err(err)),
        };

        let mut operations = vec![];
        for operation in transaction.operations {
            match self.mapper.map(operation) {
                Ok(operation) => operations.push(operation),
                Err(err) => return Some(Err(err)),
            }
        }

        Some(Ok(Transaction {
            commit_scn: transaction.commit_scn,
            commit_timestamp: transaction.commit_timestamp,
            operations,
        }))
    }
}

fn map_row(mut row: ParsedRow, schema: &Schema) -> Result<Record, Error> {
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
        (ParsedValue::String(string), FieldType::Decimal, _) => Ok(Field::Decimal(
            string
                .parse()
                .map_err(|e| Error::NumberToDecimal(e, string))?,
        )),
        (ParsedValue::Number(number), FieldType::Decimal, _) => Ok(Field::Decimal(number)),
        (ParsedValue::Number(number), FieldType::Int, _) => Ok(Field::Int(
            number
                .to_i64()
                .ok_or_else(|| Error::ParseIntFailed(number))?,
        )),
        (ParsedValue::Number(number), FieldType::UInt, _) => Ok(Field::UInt(
            number
                .to_u64()
                .ok_or_else(|| Error::ParseUIntFailed(number))?,
        )),
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
    NaiveDate::parse_from_str(string, "%d-%m-%Y")
}

fn parse_date_time(string: &str) -> Result<DateTime<FixedOffset>, ParseError> {
    let date_time = NaiveDateTime::parse_from_str(string, "%d-%m-%Y %I.%M.%S%.6f %p")?;
    Ok(Ok(DateTime::<Utc>::from_naive_utc_and_offset(date_time, Utc))?.fixed_offset())
}
