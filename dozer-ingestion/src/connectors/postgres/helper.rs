use bytes::Bytes;

use dozer_shared::types::*;
use dozer_storage::storage::RocksStorage;
use postgres::{Column, Row};
use postgres_types::{Type, WasNull};
use std::error::Error;
use std::sync::Arc;

pub fn postgres_type_to_bytes(
    value: &Bytes,
    column: &postgres_protocol::message::backend::Column,
) -> Vec<u8> {
    let column_type = Type::from_oid(column.type_id() as u32).unwrap();
    match column_type {
        Type::INT4 => {
            let number: i32 = String::from_utf8(value.to_vec()).unwrap().parse().unwrap();
            number.to_be_bytes().to_vec()
        }
        Type::TEXT | _ => value.to_vec(),
    }
}

fn handle_error(e: tokio_postgres::error::Error) -> Field {
    if let Some(e) = e.source() {
        if let Some(_e) = e.downcast_ref::<WasNull>() {
            Field::Empty
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
            Field::BoolField(val)
        }
        "char" => {
            let val: i8 = row.get(idx);
            // TODO: Fix Char
            // Field::CharField(char::from_digit(val.try_into().unwrap(), 10).unwrap())
            Field::IntField(val.into())
        }
        "int2" => {
            let val: i16 = row.get(idx);
            Field::IntField(val.into())
        }
        "int8" | "int4" => {
            let value: Result<i32, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => Field::IntField(val.into()),
                Err(error) => handle_error(error),
            }
        }
        "float4" | "float8" => {
            let val: Result<f32, postgres::Error> = row.try_get(idx);
            match val {
                Ok(val) => Field::FloatField(val.into()),
                Err(error) => handle_error(error),
            }
        }
        "numeric" => {
            // let val: Decimal = row.get(idx);
            // val.to_be_bytes().to_vec()
            // TODO: handle numeric
            // https://github.com/paupino/rust-decimal
            Field::Empty
        }

        "string" | "text" | "bpchar" => {
            let value: Result<&str, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => Field::StringField(val.to_string()),
                Err(error) => handle_error(error),
            }
        }

        "timestamp" | "timestamptz" | "date" | "tsvector" => Field::Empty,

        // TODO: ignore custom types
        "mpaa_rating" | "_text" => Field::Empty,
        "bytea" | "_bytea" => Field::Empty,

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
pub fn insert_operation_row(
    storage_client: Arc<RocksStorage>,
    table_name: String,
    row: &Row,
    columns: &[Column],
    idx: u32,
) {
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
    storage_client.insert_operation_event(&evt);
}

pub async fn insert_operation_events(
    storage_client: Arc<RocksStorage>,
    operations: Vec<OperationEvent>,
) {
    for op in operations.iter() {
        storage_client.insert_operation_event(op);
    }
}
