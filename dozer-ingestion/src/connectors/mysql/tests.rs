use crate::connectors::TableInfo;
use dozer_types::{
    errors::internal::BoxedError,
    ingestion_types::{IngestionMessage, IngestorError, IngestorForwarder},
};
use mysql_async::{prelude::Queryable, Opts, Pool};
use std::sync::{mpsc::Sender, Mutex};

pub const SERVER_URL: &str = "mysql://root:mysql@localhost:3306/test";

pub fn conn_opts() -> Opts {
    Opts::from_url(SERVER_URL).unwrap()
}

pub fn conn_pool() -> Pool {
    Pool::new(conn_opts())
}

pub async fn create_test_table(name: &str) -> TableInfo {
    let TestTable {
        create_table_sql,
        table_info,
        ..
    } = test_tables().into_iter().find(|t| t.name == name).unwrap();

    conn_pool()
        .get_conn()
        .await
        .unwrap()
        .exec_drop(create_table_sql, ())
        .await
        .unwrap();

    table_info
}

pub fn test_tables() -> Vec<TestTable> {
    vec![
        TestTable {
            name: "test1",
            create_table_sql: "CREATE TABLE IF NOT EXISTS test1 (c1 INT NOT NULL, c2 VARCHAR(20), c3 DOUBLE, PRIMARY KEY (c1))",
            table_info: TableInfo {
                schema: Some("test".into()),
                name: "test1".into(),
                column_names: vec!["c1".into(), "c2".into(), "c3".into()],
            }
        },
        TestTable {
            name: "test2",
            create_table_sql: "CREATE TABLE IF NOT EXISTS test2 (id INT NOT NULL, value JSON, PRIMARY KEY (id))",
            table_info: TableInfo {
                schema: Some("test".into()),
                name: "test2".into(),
                column_names: vec!["id".into(), "value".into()],
            }
        },
        TestTable {
            name: "test3",
            create_table_sql: "CREATE TABLE IF NOT EXISTS test3 (a INT NOT NULL, b FLOAT, PRIMARY KEY (a))",
            table_info: TableInfo {
                schema: Some("test".into()),
                name: "test3".into(),
                column_names: vec!["a".into(), "b".into()],
            }
        },
    ]
}

pub struct TestTable {
    pub name: &'static str,
    pub create_table_sql: &'static str,
    pub table_info: TableInfo,
}

#[derive(Debug)]
pub struct MockIngestionStream {
    pub sender: Mutex<Sender<IngestionMessage>>,
}

impl MockIngestionStream {
    pub fn new(sender: Sender<IngestionMessage>) -> Self {
        Self {
            sender: Mutex::new(sender),
        }
    }
}

impl IngestorForwarder for MockIngestionStream {
    fn forward(&self, msg: IngestionMessage) -> Result<(), IngestorError> {
        self.sender
            .lock()
            .unwrap()
            .send(msg)
            .map_err(BoxedError::from)?;
        Ok(())
    }
}
