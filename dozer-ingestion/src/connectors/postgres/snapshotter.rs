use crate::connectors::connector::TableInfo;
use crate::connectors::ingestor::IngestionMessage;
use crate::connectors::ingestor::Ingestor;

use super::helper;
use super::schema_helper::SchemaHelper;
use anyhow::Context;
use postgres::fallible_iterator::FallibleIterator;
use postgres::Error;
use postgres::{Client, NoTls};
use std::cell::RefCell;
use std::sync::{Arc, Mutex}; // 0.4.10
pub struct PostgresSnapshotter {
    pub tables: Option<Vec<TableInfo>>,
    pub conn_str: String,
    pub ingestor: Arc<Mutex<Ingestor>>,
}

impl PostgresSnapshotter {
    fn _connect(&mut self) -> Result<Client, Error> {
        let client = Client::connect(&self.conn_str, NoTls)?;
        Ok(client)
    }
    pub fn get_tables(&self) -> anyhow::Result<Vec<TableInfo>> {
        match self.tables.as_ref() {
            None => {
                let mut helper = SchemaHelper {
                    conn_str: self.conn_str.clone(),
                };
                let arr = helper.get_tables()?;
                Ok(arr)
            }
            Some(arr) => Ok(arr.clone()),
        }
    }

    pub fn sync_tables(&self) -> anyhow::Result<Vec<String>> {
        let client_plain = Arc::new(RefCell::new(helper::connect(self.conn_str.clone())?));

        let tables = self.get_tables()?;

        let mut idx: u32 = 0;
        for table_info in tables.iter() {
            let column_str: Vec<String> = table_info
                .columns
                .clone()
                .context("columns not found")?
                .iter()
                .map(|c| format!("\"{}\"", c))
                .collect();

            let column_str = column_str.join(",");
            let query = format!("select {} from {}", column_str, table_info.name);
            let stmt = client_plain.clone().borrow_mut().prepare(&query)?;
            let columns = stmt.columns();

            // Ingest schema for every table
            let schema = helper::map_schema(&table_info.id, columns);
            self.ingestor
                .lock()
                .unwrap()
                .handle_message(IngestionMessage::Schema(schema.clone()));

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
                            table_info.name.to_string(),
                            schema
                                .identifier
                                .clone()
                                .context("identifier is expected in snapshotter")?,
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
                idx += 1;
            }

            self.ingestor
                .lock()
                .unwrap()
                .handle_message(IngestionMessage::Commit());
        }

        let table_names = tables.iter().map(|t| t.name.clone()).collect();

        Ok(table_names)
    }
}
