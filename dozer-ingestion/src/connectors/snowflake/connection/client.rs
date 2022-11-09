use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::log::debug;

use crate::errors::{ConnectorError, SnowflakeError};

use odbc::odbc_safe::AutocommitOn;
use odbc::{
    ColumnDescriptor, Connection, Data, DiagnosticRecord, Executed, HasResult, NoData,
    ResultSetState, Statement,
};
use std::collections::HashMap;

pub struct ResultIterator<'a, 'b> {
    stmt: Statement<'a, 'b, Executed, HasResult, AutocommitOn>,
    cols: i16,
}

impl Iterator for ResultIterator<'_, '_> {
    type Item = Vec<Option<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt.fetch().unwrap() {
            None => None,
            Some(mut cursor) => {
                let mut values = vec![];
                for i in 1..(self.cols + 1) {
                    values.push(cursor.get_data::<String>(i as u16).unwrap());
                }

                Some(values)
            }
        }
    }
}

pub struct Client {
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

        Self { conn_string }
    }

    pub fn get_conn_string(&self) -> String {
        self.conn_string.clone()
    }

    pub fn exec(
        &self,
        conn: &Connection<AutocommitOn>,
        query: String,
    ) -> Result<Option<bool>, SnowflakeError> {
        let stmt = Statement::with_parent(conn).unwrap();

        let result = stmt
            .exec_direct(&query)
            .map_err(SnowflakeError::QueryError)?;
        match result {
            Data(_) => Ok(Some(true)),
            NoData(_) => Ok(None),
        }
    }

    fn parse_not_exist_error(e: DiagnosticRecord) -> Result<bool, SnowflakeError> {
        if e.get_native_error() == 2003 {
            Ok(false)
        } else {
            Err(SnowflakeError::QueryError(e))
        }
    }

    fn parse_exist(result: ResultSetState<Executed, AutocommitOn>) -> bool {
        match result {
            Data(_) => true,
            NoData(_) => false,
        }
    }

    pub fn stream_exist(
        &self,
        conn: &Connection<AutocommitOn>,
        stream_name: &String,
    ) -> Result<bool, SnowflakeError> {
        let query = format!("DESCRIBE STREAM {};", stream_name);

        let stmt = Statement::with_parent(conn).unwrap();
        stmt.exec_direct(&query)
            .map_or_else(Self::parse_not_exist_error, |result| {
                Ok(Self::parse_exist(result))
            })
    }

    pub fn table_exist(
        &self,
        conn: &Connection<AutocommitOn>,
        table_name: &String,
    ) -> Result<bool, SnowflakeError> {
        let query = format!("DESCRIBE TABLE {};", table_name);

        let stmt = Statement::with_parent(conn).unwrap();
        stmt.exec_direct(&query)
            .map_or_else(Self::parse_not_exist_error, |result| {
                Ok(Self::parse_exist(result))
            })
    }

    pub fn fetch<'a, 'b>(
        &self,
        conn: &'a Connection<AutocommitOn>,
        query: String,
    ) -> Result<Option<(Vec<ColumnDescriptor>, ResultIterator<'a, 'b>)>, ConnectorError> {
        let stmt = Statement::with_parent(conn).unwrap();
        // TODO: use stmt.close_cursor to improve efficiency

        match stmt.exec_direct(&query).unwrap() {
            Data(stmt) => {
                let cols = stmt.num_result_cols().unwrap();
                let mut schema = vec![];
                for i in 1..(cols + 1) {
                    schema.push(stmt.describe_col(i.try_into().unwrap()).unwrap());
                }
                Ok(Some((schema, ResultIterator { cols, stmt })))
            }
            NoData(_) => Ok(None),
        }
    }
}
