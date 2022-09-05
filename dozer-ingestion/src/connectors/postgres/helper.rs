use dozer_shared::storage::storage_client::StorageClient;
use dozer_shared::storage::{Record, ServerResponse};
use postgres::Column;
use postgres_types::Type;

pub fn to_vec_bytes(row: &tokio_postgres::Row, idx: usize, col_type: &Type) -> Vec<u8> {
    let t = col_type.to_owned();
    match t.name() {
        "bool" | "char" | "int8" | "int4" | "int2" => {
            let val: i32 = row.try_get(idx).unwrap();
            (val as i32).to_be_bytes().to_vec()
        }
        "float4" | "float8" => {
            let val: bool = row.try_get(idx).unwrap();
            (val as i32).to_be_bytes().to_vec()
        }

        "string" | "text" => {
            let val: &str = row.try_get(idx).unwrap();
            val.as_bytes().to_vec()
        }

        "timestamp" | "timestamptz" => {
            // let val: chrono::DateTime<Local> = row.try_get(idx).unwrap();
            // val.as_bytes().to_vec()
            "1".as_bytes().to_vec()
        }
        v => {
            println!("{}", v);
            panic!("error");
        }
    }
}

pub async fn insert_record(
    storage_client: &mut StorageClient<tonic::transport::channel::Channel>,
    row: &tokio_postgres::Row,
    columns: &[Column],
) -> ServerResponse {
    let mut values = Vec::new();

    let len = columns.len();
    for x in 0..len {
        let col_type = columns[x].type_();
        // let val: bytes::Bytes = row.get(x);
        let val: Vec<u8> = to_vec_bytes(row, x, col_type);
        values.push(val);
    }

    let request = tonic::Request::new(Record {
        schema_id: 1,
        values,
    });

    let response = storage_client.insert_record(request).await.unwrap();

    response.into_inner()
}
