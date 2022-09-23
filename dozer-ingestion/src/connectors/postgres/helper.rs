use bytes::Bytes;

use crate::connectors::postgres::xlog_mapper::TableColumn;
use dozer_shared::types::*;
use postgres::{Client, Column, NoTls, Row};
use postgres_types::{Type, WasNull};
use std::error::Error;
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

pub fn postgres_type_to_field(
    value: &Bytes,
    column: &TableColumn,
) -> Field {
    if let Some(column_type) = &column.r#type {
        match column_type {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => {
                Field::Int(String::from_utf8(value.to_vec()).unwrap().parse().unwrap())
            }
            &Type::FLOAT4 | &Type::FLOAT8 => {
                Field::Float(String::from_utf8(value.to_vec()).unwrap().parse::<f64>().unwrap())
            },
            &Type::TEXT => Field::String(String::from_utf8(value.to_vec()).unwrap()),
            &Type::BYTEA => Field::Binary(value.to_vec()),
            &Type::NUMERIC => Field::Decimal(Decimal::from_f64(String::from_utf8(value.to_vec()).unwrap().parse::<f64>().unwrap()).unwrap()),
            &Type::TIMESTAMP => {
                let date = NaiveDateTime::parse_from_str(String::from_utf8(value.to_vec()).unwrap().as_str(), "%Y-%m-%d %H:%M:%S").unwrap();
                Field::Timestamp(DateTime::<Utc>::from_utc(date, Utc))
            },
            &Type::TIMESTAMPTZ => {
                let date: DateTime<FixedOffset> = DateTime::parse_from_str(String::from_utf8(value.to_vec()).unwrap().as_str(), "%Y-%m-%d %H:%M:%S%.f%#z").unwrap();
                Field::Timestamp(DateTime::<Utc>::from_utc(date.naive_utc(), Utc))
            },
            &Type::JSONB | &Type::JSON => Field::Bson(value.to_vec()),
            &Type::BOOL => Field::Boolean(value.slice(0..1) == "t"),
            _ => Field::Null
        }
    } else {
        Field::Null
    }
}

pub fn postgres_type_to_dozer_type(column: &TableColumn) -> Field {
    if let Some(column_type) = &column.r#type {
        match column_type {
            &Type::INT4 | &Type::INT8 | &Type::INT2 => Field::Int(0),
            &Type::TEXT => Field::String("".parse().unwrap()),
            &Type::FLOAT4 | &Type::FLOAT8 => Field::Float(0.0),
            &Type::BOOL => Field::Boolean(false),
            &Type::BIT => Field::Binary(vec![]),
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Field::Timestamp(DateTime::default()),
            _ => Field::Null
        }
    } else {
        Field::Null
    }
}

fn handle_error(e: postgres::error::Error) -> Field {
    if let Some(e) = e.source() {
        if let Some(_e) = e.downcast_ref::<WasNull>() {
            Field::Null
        } else {
            panic!("Conversion error: {:?}", e);
        }
    } else {
        panic!("Conversion error: {:?}", e);
    }
}
pub fn value_to_field(row: &tokio_postgres::Row, idx: usize, col_type: &Type) -> Field {
    let t = col_type.to_owned();
    match t.name() {
        "bool" => {
            let val: bool = row.get(idx);
            Field::Boolean(val)
        }
        "char" => {
            let val: i8 = row.get(idx);
            // TODO: Fix Char
            // Field::CharField(char::from_digit(val.try_into().unwrap(), 10).unwrap())
            Field::Int(val.into())
        }
        "int2" => {
            let val: i16 = row.get(idx);
            Field::Int(val.into())
        }
        "int8" | "int4" => {
            let value: Result<i32, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => Field::Int(val.into()),
                Err(error) => handle_error(error),
            }
        }
        "float4" | "float8" => {
            let val: Result<f32, postgres::Error> = row.try_get(idx);
            match val {
                Ok(val) => Field::Float(val.into()),
                Err(error) => handle_error(error),
            }
        }
        "numeric" => {
            // let val: u32 = row.get(idx);
            // Field::Float(val.into())
            Field::Null
            // TODO: handle numeric
            // https://github.com/paupino/rust-decimal
        }

        "string" | "text" | "bpchar" => {
            let value: Result<&str, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => Field::String(val.to_string()),
                Err(_error) => Field::Null,
            }
        }

        "timestamp" | "timestamptz" | "date" | "tsvector" => Field::Null,

        // TODO: ignore custom types
        "mpaa_rating" | "_text" => Field::Null,
        "bytea" | "_bytea" => {
            let val: Result<Vec<u8>, postgres::Error> = row.try_get(idx);
            match val {
                Ok(val) => Field::Binary(val),
                Err(error) => handle_error(error),
            }
        }

        v => {
            println!("{}", v);
            panic!("error");
        }
    }
}

pub fn get_values(row: &Row, columns: &[Column]) -> Vec<Field> {
    let mut values: Vec<Field> = vec![];
    let mut idx = 0;
    for col in columns.iter() {
        let val: Field = value_to_field(row, idx, col.type_());
        values.push(val);
        idx = idx + 1;
    }
    values
}

pub fn map_row_to_operation_event(
    table_name: String,
    row: &Row,
    columns: &[Column],
    idx: u32,
) -> OperationEvent {
    let rec = Record {
        values: get_values(row, columns),
        schema_id: 1,
    };

    let op = Operation::Insert {
        table_name,
        new: rec,
    };
    let evt: OperationEvent = OperationEvent {
        operation: op,
        id: idx,
    };
    evt
}

pub fn connect(conn_str: String) -> Result<Client, postgres::Error> {
    let client = Client::connect(&conn_str.clone(), NoTls)?;
    Ok(client)
}

pub async fn async_connect(conn_str: String) -> Result<tokio_postgres::Client, postgres::Error> {
    let (client, connection) = tokio_postgres::connect(&conn_str.clone(), NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
            panic!("Connection failed!");
        }
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use super::*;

    #[macro_export]
    macro_rules! test_conversion {
        ($a:expr,$b:expr,$c:expr) => {
            let value = postgres_type_to_field(&Bytes::from($a), &TableColumn {
                name: "column".to_string(),
                type_id: $b.oid() as i32,
                flags: 0,
                r#type: Some($b)
            });
            println!("{:?}", value);
            assert_eq!(value, $c);
        };
    }

    #[test]
    fn it_converts_postgres_type_to_field() {
        test_conversion!("12", Type::INT8, Field::Int(12));
        test_conversion!("4.7809", Type::FLOAT8, Field::Float(4.7809));
        let value = String::from("Test text");
        test_conversion!("Test text", Type::TEXT, Field::String(value));

        // UTF-8 bytes representation of json (https://www.charset.org/utf-8)
        let value: Vec<u8> = vec![98, 121, 116, 101, 97];
        test_conversion!("bytea", Type::BYTEA, Field::Binary(value));

        let value = Decimal::from_f64(8.28).unwrap();
        test_conversion!("8.28", Type::NUMERIC, Field::Decimal(value));

        let value = DateTime::<Utc>::from_utc(
            NaiveDate::from_ymd(2022, 9, 16).and_hms(5, 56, 29),
            Utc
        );
        test_conversion!("2022-09-16 05:56:29", Type::TIMESTAMP, Field::Timestamp(value));

        let value = DateTime::<Utc>::from_utc(
            NaiveDate::from_ymd(2022, 9, 16).and_hms_micro(3, 56, 30, 959787),
            Utc
        );
        test_conversion!("2022-09-16 10:56:30.959787+07", Type::TIMESTAMPTZ, Field::Timestamp(value));

        // UTF-8 bytes representation of json (https://www.charset.org/utf-8)
        let value = vec![123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125];
        test_conversion!("{\"abc\":\"foo\"}", Type::JSONB, Field::Bson(value));

        test_conversion!("t", Type::BOOL, Field::Boolean(true));
        test_conversion!("f", Type::BOOL, Field::Boolean(false));
    }
}
