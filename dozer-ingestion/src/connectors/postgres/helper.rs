use bytes::Bytes;
use dozer_shared::storage::storage_client::StorageClient;
use dozer_shared::storage::{Operation, Record, ServerResponse};
use postgres::Column;
use postgres_types::{Type, WasNull};
use std::error::Error;

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

fn handle_error(e: tokio_postgres::error::Error) -> Vec<u8> {
    if let Some(e) = e.source() {
        if let Some(_e) = e.downcast_ref::<WasNull>() {
            vec![]
        } else {
            panic!("Conversion error: {:?}", e);
        }
    } else {
        panic!("Conversion error: {:?}", e);
    }
}
pub fn to_vec_bytes(row: &tokio_postgres::Row, idx: usize, col_type: &Type) -> Vec<u8> {
    let t = col_type.to_owned();
    match t.name() {
        "bool" => {
            let val: bool = row.get(idx);
            (val as i32).to_be_bytes().to_vec()
        }
        "char" | "int2" => {
            let value: Result<i16, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => val.to_be_bytes().to_vec(),
                Err(error) => handle_error(error),
            }
        }
        "int8" | "int4" => {
            let value: Result<i32, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => (val as i32).to_be_bytes().to_vec(),
                Err(error) => handle_error(error),
            }
        }
        "float4" | "float8" => {
            let val: f32 = row.try_get(idx).unwrap();
            val.to_be_bytes().to_vec()
        }
        "numeric" => {
            // let val: Decimal = row.get(idx);
            // val.to_be_bytes().to_vec()
            // TODO: handle numeric
            // https://github.com/paupino/rust-decimal
            vec![]
        }

        "string" | "text" | "bpchar" => {
            let value: Result<&str, postgres::Error> = row.try_get(idx);

            match value {
                Ok(val) => val.as_bytes().to_vec(),
                Err(error) => handle_error(error),
            }
        }

        "timestamp" | "timestamptz" | "date" | "tsvector" => {
            // let val: chrono::DateTime<Local> = row.try_get(idx).unwrap();
            // val.as_bytes().to_vec()
            vec![]
        }

        // TODO: ignore custom types
        "mpaa_rating" | "_text" => "0".as_bytes().to_vec(),
        "bytea" | "_bytea" => "0".as_bytes().to_vec(),

        v => {
            println!("{}", v);
            panic!("error");
        }
    }
}

async fn insert_record(
    storage_client: &mut StorageClient<tonic::transport::channel::Channel>,
    schema_id: i32,
    values: Vec<Vec<u8>>,
) -> ServerResponse {
    let request = tonic::Request::new(Record {
        schema_id: schema_id.try_into().unwrap(),
        values,
    });

    storage_client
        .insert_record(request)
        .await
        .unwrap()
        .into_inner()
}

pub async fn insert_row_record(
    storage_client: &mut StorageClient<tonic::transport::channel::Channel>,
    row: &tokio_postgres::Row,
    columns: &[Column],
    schema_id: i32,
) -> ServerResponse {
    let mut values = Vec::new();

    let len = columns.len();
    for x in 0..len {
        let col_type = columns[x].type_();
        // let val: bytes::Bytes = row.get(x);
        let val: Vec<u8> = to_vec_bytes(row, x, col_type);
        values.push(val);
    }

    insert_record(storage_client, schema_id, values).await
}

pub async fn insert_operations(
    storage_client: &mut StorageClient<tonic::transport::channel::Channel>,
    operations: Vec<Operation>,
) {
    for operation in operations.iter() {
        let request = tonic::Request::new(operation.clone());

        storage_client.insert_operation(request).await.unwrap();
    }
}
