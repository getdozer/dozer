use crate::connectors::TableInfo;
use dozer_types::{
    errors::internal::BoxedError,
    ingestion_types::{IngestionMessage, IngestorError, IngestorForwarder},
};
use mysql_async::{prelude::Queryable, Opts, Pool};
use std::sync::{mpsc::Sender, Mutex};

pub struct TestConfig {
    pub url: String,
    pub opts: Opts,
    pub pool: Pool,
}

impl TestConfig {
    pub fn new(url: String) -> Self {
        let opts = Opts::from_url(url.as_str()).unwrap();
        let pool = Pool::new(opts.clone());
        Self { url, opts, pool }
    }
}

pub fn mysql_test_config() -> TestConfig {
    TestConfig::new("mysql://root:mysql@localhost:3306/test".into())
}

pub fn mariadb_test_config() -> TestConfig {
    TestConfig::new("mysql://root:mariadb@localhost:3307/test".into())
}

pub async fn create_test_table(name: &str, config: &TestConfig) -> TableInfo {
    let TestTable {
        create_table_sql,
        table_info,
        ..
    } = test_tables().into_iter().find(|t| t.name == name).unwrap();

    config
        .pool
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
