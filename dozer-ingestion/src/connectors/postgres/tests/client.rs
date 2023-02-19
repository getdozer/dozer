use crate::connectors::postgres::connection::helper::{connect, map_connection_config};
use dozer_types::models::connection::ConnectionConfig;
use dozer_types::rust_decimal::Decimal;
use postgres::Client;
use std::fmt::Write;

pub struct TestPostgresClient {
    client: Client,
    pub postgres_config: tokio_postgres::Config,
}

impl TestPostgresClient {
    pub fn new(auth: &ConnectionConfig) -> Self {
        let postgres_config = map_connection_config(auth).unwrap();

        let client = connect(postgres_config.clone()).unwrap();

        Self {
            client,
            postgres_config,
        }
    }

    pub fn execute_query(&mut self, query: &str) {
        self.client.query(query, &[]).unwrap();
    }

    pub fn create_simple_table(&mut self, schema: &str, table_name: &str) {
        self.execute_query(&format!(
            "CREATE TABLE {schema}.{table_name}
(
    id          SERIAL
        PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      DOUBLE PRECISION
);"
        ));
    }

    pub fn drop_schema(&mut self, schema: &str) {
        self.execute_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
    }

    pub fn drop_table(&mut self, schema: &str, table_name: &str) {
        self.execute_query(&format!("DROP TABLE IF EXISTS {schema}.{table_name}"));
    }

    pub fn create_schema(&mut self, schema: &str) {
        self.drop_schema(schema);
        self.execute_query(&format!("CREATE SCHEMA {schema}"));
    }

    pub fn insert_rows(&mut self, table_name: &str, count: u64) {
        let mut buf = String::new();
        for i in 0..count {
            if i > 0 {
                buf.write_str(",").unwrap();
            }
            buf.write_fmt(format_args!(
                "(\'Product {}\',\'Product {} description\',{})",
                i,
                i,
                Decimal::new((i * 41) as i64, 2)
            ))
            .unwrap();
        }

        let query = format!("insert into {table_name}(name, description, weight) values {buf}",);

        self.execute_query(&query);
    }
}
