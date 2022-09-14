use crate::connectors::postgres::helper::insert_operation_row;
use crate::connectors::storage::RocksStorage;
use dozer_shared::types::OperationEvent;
use futures::{Stream, StreamExt};
use postgres::Column;
use postgres::Error;
use postgres::SimpleQueryMessage::Row;
use postgres::{Client, NoTls, RowIter};
use std::cell::RefCell;
use std::cell::RefMut;
use std::pin::Pin;
use std::sync::Arc; // 0.4.10
pub struct PostgresSnapshotter {
    pub tables: Option<Vec<String>>,
    pub conn_str: String,
    pub storage_client: Arc<RocksStorage>,
}

impl PostgresSnapshotter {
    fn _connect(&mut self) -> Result<Client, Error> {
        let client = Client::connect(&self.conn_str, NoTls)?;
        Ok(client)
    }
    pub fn get_tables(&self, mut client: Arc<RefCell<Client>>) -> Result<Vec<String>, Error> {
        match self.tables.as_ref() {
            None => {
                let query = "SELECT ist.table_name, t.relid AS id
                FROM information_schema.tables ist
                LEFT JOIN pg_catalog.pg_statio_user_tables t ON t.relname = ist.table_name
                WHERE ist.table_schema = 'public'
                ORDER BY ist.table_name;";

                let mut rows: Vec<String> = vec![];
                let results = client.borrow_mut().simple_query(query)?;
                for row in results {
                    if let Row(row) = row {
                        rows.push(row.get(0).unwrap().to_string());
                    }
                }
                Ok(rows)
            }
            Some(arr) => Ok(arr.to_vec()),
        }
    }

    // pub fn get_iterator(
    //     &mut self,
    //     client: Arc<RefCell<Client>>,
    //     table: String,
    // ) -> Result<Arc<RowIter>, Error> {
    //     let query = format!("select * from {}", table);
    //     let stmt = client.clone().borrow_mut().prepare(&query)?;
    //     let columns = stmt.columns();
    //     println!("{:?}", columns);

    //     let empty_vec: Vec<String> = Vec::new();
    //     let iter = client.clone().borrow_mut().query_raw(&stmt, empty_vec)?;
    //     Ok(Arc::new(iter))
    // }

    // fn process_stream(&self, table_name: String, columns: &[Column], stream: &RowIter) {
    //     let mut i = 0;
    //     loop {
    //         match stream.next() {
    //             Some(Ok(row)) => {
    //                 insert_operation_row(
    //                     Arc::clone(&self.storage_client),
    //                     table_name.to_owned(),
    //                     &row,
    //                     columns,
    //                     i,
    //                 );
    //             }
    //             Some(Err(error)) => {
    //                 panic!("{}", error)
    //             }
    //             _ => {
    //                 break;
    //             }
    //         };
    //         i = i + 1;
    //     }
    // }
}
