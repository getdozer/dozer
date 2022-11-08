use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::log::debug;

use odbc::{create_environment_v3, Data, Environment, NoData, Statement, Version3};
use std::collections::HashMap;

pub struct Client {
    env: Environment<Version3>,
    conn_string: String,
}

impl Client {
    pub fn new(config: &SnowflakeConfig) -> Self {
        let mut conn_hashmap: HashMap<String, String> = HashMap::new();
        conn_hashmap.insert(
            "Driver".to_string(),
            // TODO: fix usage of snowflake odbc lib
            "{/opt/snowflake/snowflakeodbc/lib/libSnowflake.dylib}".to_string(),
        );
        conn_hashmap.insert("Server".to_string(), config.clone().server);
        conn_hashmap.insert("Port".to_string(), config.clone().port);
        conn_hashmap.insert("Uid".to_string(), config.clone().user);
        conn_hashmap.insert("Pwd".to_string(), config.clone().password);
        conn_hashmap.insert("Schema".to_string(), config.clone().schema);
        conn_hashmap.insert("Warehouse".to_string(), config.clone().warehouse);
        conn_hashmap.insert("Database".to_string(), config.clone().database);

        let mut parts = vec![];
        conn_hashmap.keys().into_iter().for_each(|k| {
            parts.push(format!("{}={}", k, conn_hashmap.get(k).unwrap()));
        });

        let conn_string = parts.join(";");

        debug!("Snowflake conn string: {:?}", conn_string);

        Self {
            env: create_environment_v3().map_err(|e| e.unwrap()).unwrap(),
            conn_string,
        }
    }

    pub fn execute(&self, query: String) {
        let conn = self
            .env
            .connect_with_connection_string(&self.conn_string)
            .unwrap();
        let stmt = Statement::with_parent(&conn).unwrap();

        match stmt.exec_direct(&query).unwrap() {
            Data(mut stmt) => {
                let cols = stmt.num_result_cols().unwrap();
                while let Some(mut cursor) = stmt.fetch().unwrap() {
                    for i in 1..(cols + 1) {
                        match cursor.get_data::<&str>(i as u16).unwrap() {
                            Some(val) => debug!(" {}", val),
                            None => debug!(" NULL"),
                        }
                    }
                    debug!("");
                }
            }
            NoData(_) => debug!("Query executed, no data returned"),
        };
    }
}
