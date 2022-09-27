use crate::connectors::ingestor::IngestionMessage;
use crate::connectors::ingestor::Ingestor;
use dozer_types::types::Schema;
use postgres::fallible_iterator::FallibleIterator;
use postgres::Error;
use postgres::SimpleQueryMessage::Row;
use postgres::{Client, NoTls};
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use super::helper; // 0.4.10
pub struct PostgresSnapshotter {
    pub tables: Option<Vec<String>>,
    pub conn_str: String,
    pub ingestor: Arc<Mutex<Ingestor>>,
}

impl PostgresSnapshotter {
    fn _connect(&mut self) -> Result<Client, Error> {
        let client = Client::connect(&self.conn_str, NoTls)?;
        Ok(client)
    }
    pub fn get_tables(&self, client: Arc<RefCell<Client>>) -> Result<Vec<String>, Error> {
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

    pub fn sync_tables(&self) -> Result<Vec<String>, Error> {
        let client_plain = Arc::new(RefCell::new(helper::connect(self.conn_str.clone())?));

        let tables = self.get_tables(client_plain.clone())?;

        let mut idx: u32 = 0;
        for table in tables.iter() {
            let query = format!("select * from {}", table);
            let stmt = client_plain.clone().borrow_mut().prepare(&query)?;
            let columns = stmt.columns();

            // Ingest schema for every table
            let schema = helper::map_schema(table.to_string(), columns);
            self.ingestor
                .lock()
                .unwrap()
                .handle_message(IngestionMessage::Schema(schema));

            let empty_vec: Vec<String> = Vec::new();
            for msg in client_plain
                .clone()
                .borrow_mut()
                .query_raw(&stmt, empty_vec)?
                .iterator()
            {
                match msg {
                    Ok(msg) => {
                        let evt = helper::map_row_to_operation_event(
                            table.to_string(),
                            &msg,
                            columns,
                            idx,
                        );
                        self.ingestor
                            .lock()
                            .unwrap()
                            .handle_message(IngestionMessage::OperationEvent(evt));
                    }
                    Err(e) => {
                        println!("{:?}", e);
                        panic!("Something happened");
                    }
                }
                idx = idx + 1;
            }

            self.ingestor
                .lock()
                .unwrap()
                .handle_message(IngestionMessage::Commit());
        }
        Ok(tables)
    }
}
