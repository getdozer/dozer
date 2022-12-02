use crate::connectors::postgres::connection::helper::{connect, map_connection_config};
use dozer_types::models::connection::Authentication;
use postgres::Client;

pub struct TestPostgresClient {
    client: Client,
    pub postgres_config: tokio_postgres::Config,
}

impl TestPostgresClient {
    pub fn new(auth: &Authentication) -> Self {
        let postgres_config = map_connection_config(auth).unwrap();

        let client = connect(postgres_config.clone()).unwrap();

        Self {
            client,
            postgres_config,
        }
    }

    pub fn execute_query(&mut self, query: String) {
        self.client.query(&query, &[]).unwrap();
    }

    pub fn create_simple_table(&mut self, schema: String, table_name: String) {
        self.execute_query(format!(
            "CREATE TABLE {}.{}
(
    id          SERIAL
        PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    description VARCHAR(512),
    weight      DOUBLE PRECISION
);",
            &schema, &table_name
        ));
    }

    pub fn drop_schema(&mut self, schema: String) {
        self.execute_query(format!("DROP SCHEMA IF EXISTS {} CASCADE", &schema));
    }

    pub fn create_schema(&mut self, schema: String) {
        self.drop_schema(schema.clone());
        self.execute_query(format!("CREATE SCHEMA {}", &schema));
    }
}
