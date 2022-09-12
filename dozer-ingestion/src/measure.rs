use connectors::postgres::helper;
use dozer_shared::storage::storage_client::StorageClient;
use postgres::{NoTls, Row, Statement};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tonic::transport::Channel;

mod connectors;
mod storage_client;

struct Params {
    row: Option<Row>,
    stmt: Option<Statement>,
}

async fn _connect(conn_str: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
            panic!("Connection failed!");
        }
    });
    client
}

async fn setup(gate: Arc<Mutex<Params>>, conn_str: &str) {
    // connector.initialize().await;
    let client = _connect(conn_str).await;
    let query = format!("select * from actor limit 1");
    let opt_stmt = Some(client.prepare(&query).await.unwrap());
    let opt_row: Option<Row> = Some(
        client
            .query_one(opt_stmt.as_ref().unwrap(), &[])
            .await
            .unwrap(),
    );
    let mut gate = gate.lock().unwrap();
    gate.row = opt_row;
    gate.stmt = opt_stmt;
}

#[tokio::main]
async fn main() {
    let a = Arc::new(Mutex::new(Params {
        row: None,
        stmt: None,
    }));
    let conn_str = "host=127.0.0.1 port=5432 user=postgres dbname=pagila";
    setup(Arc::clone(&a), conn_str).await;
    let gate = Arc::clone(&a);
    let before = Instant::now();
    for i in 1..1000000000 {
        let gate = gate.lock().unwrap();
        let sclient: &mut StorageClient<Channel> =
            &mut futures::executor::block_on(storage_client::initialize());

        let response = helper::insert_row_record(
            sclient,
            gate.row.as_ref().unwrap(),
            gate.stmt.as_ref().unwrap().columns(),
            1,
        )
        .await;
        // println!("{}, {:?}", i, response);

        const BACKSPACE: char = 8u8 as char;

        if i % 100 == 0 {
            print!(
                "{}\rCount: {}, Elapsed time: {:.2?}",
                BACKSPACE,
                i,
                before.elapsed(),
            );
        }
    }
}
