use bytes::Bytes;

use crate::connectors::postgres::xlog_mapper::TableColumn;
use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use dozer_types::types::*;
use postgres::{Client, Column, NoTls, Row};
use postgres_types::{Type, WasNull};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::error::Error;
use std::vec;
use postgres_types::FromSql;

pub fn postgres_type_to_field(value: &Bytes, column: &TableColumn) -> Field {
    if let Some(column_type) = &column.r#type {
        match column_type {
            &Type::INT2 | &Type::INT4 | &Type::INT8 => {
                Field::Int(String::from_utf8(value.to_vec()).unwrap().parse().unwrap())
            }
            &Type::FLOAT4 | &Type::FLOAT8 => Field::Float(
                String::from_utf8(value.to_vec())
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            ),
            &Type::TEXT => Field::String(String::from_utf8(value.to_vec()).unwrap()),
            &Type::BYTEA => Field::Binary(value.to_vec()),
            &Type::NUMERIC => Field::Decimal(
                Decimal::from_f64(
                    String::from_utf8(value.to_vec())
                        .unwrap()
                        .parse::<f64>()
                        .unwrap(),
                )
                .unwrap(),
            ),
            &Type::TIMESTAMP => {
                let date = NaiveDateTime::parse_from_str(
                    String::from_utf8(value.to_vec()).unwrap().as_str(),
                    "%Y-%m-%d %H:%M:%S",
                )
                .unwrap();
                Field::Timestamp(DateTime::<Utc>::from_utc(date, Utc))
            }
            &Type::TIMESTAMPTZ => {
                let date: DateTime<FixedOffset> = DateTime::parse_from_str(
                    String::from_utf8(value.to_vec()).unwrap().as_str(),
                    "%Y-%m-%d %H:%M:%S%.f%#z",
                )
                .unwrap();
                Field::Timestamp(DateTime::<Utc>::from_utc(date.naive_utc(), Utc))
            }
            &Type::JSONB | &Type::JSON => Field::Bson(value.to_vec()),
            &Type::BOOL => Field::Boolean(value.slice(0..1) == "t"),
            _ => Field::Null,
        }
    } else {
        Field::Null
    }
}

pub fn postgres_type_to_dozer_type(col_type: Option<&Type>) -> FieldType {
    if let Some(column_type) = col_type {
        match column_type {
            &Type::BOOL => FieldType::Boolean,
            &Type::INT2 | &Type::INT4 | &Type::INT8 => FieldType::Int,
            &Type::CHAR | &Type::TEXT => FieldType::String,
            &Type::FLOAT4 | &Type::FLOAT8 => FieldType::Float,
            &Type::BIT => FieldType::Binary,
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => FieldType::Timestamp,
            _ => FieldType::Null,
        }
    } else {
        FieldType::Null
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
    match col_type {
        &Type::BOOL => {
            let value: Result<bool, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Boolean(val),
                Err(error) => handle_error(error),
            }
        }
        &Type::CHAR | &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR => {
            let value: Result<String, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::String(val),
                Err(error) => handle_error(error),
            }
        }
        &Type::INT2 => {
            let value: Result<i16, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Int(val.into()),
                Err(error) => handle_error(error),
            }
        }
        &Type::INT4 => {
            let value: Result<i32, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Int(val.into()),
                Err(error) => handle_error(error),
            }
        }
        &Type::INT8 => {
            let value: Result<i64, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Int(val),
                Err(error) => handle_error(error),
            }
        }
        &Type::FLOAT4  => {
            let value: Result<f32, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Float(val.into()),
                Err(error) => handle_error(error),
            }
        }

        &Type::FLOAT8 => {
            let value: Result<f64, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Float(val),
                Err(error) => handle_error(error),
            }
        }
        &Type::NUMERIC => {
            // let ra: Result<Decimal, _> = row.try_get(idx);
            // rust_decimal::Decimal::from_sql(&Type::NUMERIC, raw);
            // rust_decimal::Decimal::from_(&postgres_types::Type::NUMERIC, raw);
            // let value: Result<Decimal, _> = FromSql::from_sql(&Type::from(Type::NUMERIC), raw);
            // Field::Decimal(value.unwrap())
            Field::Null
        }

        &Type::TIMESTAMP => {
            let value: Result<NaiveDateTime, _> = row.try_get(idx);
            match value {
                Ok(val) => Field::Timestamp(DateTime::<Utc>::from_utc(val, Utc)),
                Err(error) => handle_error(error),
            }
        }
        &Type::TIMESTAMPTZ => {
            let value: Result<DateTime<FixedOffset>, _> = row.try_get(idx);

            match value {
                Ok(val) => Field::Timestamp(DateTime::<Utc>::from_utc(val.naive_utc(), Utc)),
                Err(error) => handle_error(error),
            }
        }
        &Type::TS_VECTOR => {
            // Not supported type
            Field::Null
        }
        // "date" | "tsvector" => Field::Null,

        // TODO: ignore custom types
        // "mpaa_rating" | "_text" => Field::Null,
        // "bytea" | "_bytea" => {
        //     let val: Result<Vec<u8>, postgres::Error> = row.try_get(idx);
        //     match val {
        //         Ok(val) => Field::Binary(val),
        //         Err(error) => handle_error(error),
        //     }
        // }
        _ => {
            if col_type.schema() == "pg_catalog" {
                // println!("UNSUPPORTED TYPE: {:?}", col_type);
            }
            Field::Null
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
        schema_id: Some(SchemaIdentifier { id: 1, version: 1 }),
        values: get_values(row, columns),
    };

    let op = Operation::Insert { new: rec };
    let evt: OperationEvent = OperationEvent {
        operation: op,
        seq_no: idx as u64,
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

pub fn map_schema(rel_id: &u32, columns: &[Column]) -> Schema {
    let field_defs = columns
        .iter()
        .map(|col| FieldDefinition {
            name: col.name().to_string(),
            typ: postgres_type_to_dozer_type(Some(col.type_())),
            nullable: true,
        })
        .collect();

    Schema {
        identifier: Some(SchemaIdentifier { id: *rel_id, version: 1 }),
        fields: field_defs,
        values: vec![],
        primary_index: vec![0],
        secondary_indexes: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[macro_export]
    macro_rules! test_conversion {
        ($a:expr,$b:expr,$c:expr) => {
            let value = postgres_type_to_field(
                &Bytes::from($a),
                &TableColumn {
                    name: "column".to_string(),
                    type_id: $b.oid() as i32,
                    flags: 0,
                    r#type: Some($b),
                },
            );
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

        let value =
            DateTime::<Utc>::from_utc(NaiveDate::from_ymd(2022, 9, 16).and_hms(5, 56, 29), Utc);
        test_conversion!(
            "2022-09-16 05:56:29",
            Type::TIMESTAMP,
            Field::Timestamp(value)
        );

        let value = DateTime::<Utc>::from_utc(
            NaiveDate::from_ymd(2022, 9, 16).and_hms_micro(3, 56, 30, 959787),
            Utc,
        );
        test_conversion!(
            "2022-09-16 10:56:30.959787+07",
            Type::TIMESTAMPTZ,
            Field::Timestamp(value)
        );

        // UTF-8 bytes representation of json (https://www.charset.org/utf-8)
        let value = vec![123, 34, 97, 98, 99, 34, 58, 34, 102, 111, 111, 34, 125];
        test_conversion!("{\"abc\":\"foo\"}", Type::JSONB, Field::Bson(value));

        test_conversion!("t", Type::BOOL, Field::Boolean(true));
        test_conversion!("f", Type::BOOL, Field::Boolean(false));
    }
}
