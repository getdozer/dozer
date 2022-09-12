use crate::connectors::postgres::helper::insert_operation_row;
use dozer_storage::storage::RocksStorage;
use futures::StreamExt;
use std::sync::Arc;
use tokio_postgres::SimpleQueryMessage::Row;
use tokio_postgres::{Client, NoTls, RowStream}; // 0.4.10

pub struct PostgresSnapshotter {
    pub tables: Option<Vec<String>>,
    pub conn_str: String,
    pub storage_client: Arc<RocksStorage>,
}

impl PostgresSnapshotter {
    async fn _connect(&mut self) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
                panic!("Connection failed!");
            }
        });
        client
    }
    async fn get_tables(&self, client: Client) -> Vec<String> {
        match self.tables.as_ref() {
            None => {
                let query = "SELECT ist.table_name, t.relid AS id
                FROM information_schema.tables ist
                LEFT JOIN pg_catalog.pg_statio_user_tables t ON t.relname = ist.table_name
                WHERE ist.table_schema = 'public'
                ORDER BY ist.table_name;";

                let mut rows: Vec<String> = vec![];
                let results = client.simple_query(query).await.unwrap();
                for row in results {
                    if let Row(row) = row {
                        rows.push(row.get(0).unwrap().to_string());
                    }
                }
                rows
            }
            Some(arr) => arr.to_vec(),
        }
    }

    pub async fn run(&mut self) {
        let client = self._connect().await;
        let tables = self.get_tables(client).await;

        println!("Initialized with tables: {:?}", tables);

        let client = self._connect().await;
        for t in tables.iter() {
            println!("Syncing table {} .....", t);
            let query = format!("select * from {}", t);
            let stmt = client.prepare(&query).await.unwrap();
            let columns = stmt.columns();
            println!("{:?}", columns);

            let empty_vec: Vec<String> = Vec::new();
            let stream: RowStream = client.query_raw(&stmt, empty_vec).await.unwrap();

            tokio::pin!(stream);
            let mut i = 0;
            loop {
                match stream.next().await {
                    Some(Ok(row)) => {
                        insert_operation_row(
                            Arc::clone(&self.storage_client),
                            t.to_string(),
                            &row,
                            columns,
                            i,
                        );
                    }
                    Some(Err(error)) => {
                        panic!("{}", error)
                    }
                    _ => {
                        break;
                    }
                };
                i = i + 1;
            }
        }
    }
}
