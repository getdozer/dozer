use dozer_types::ingestion_types::SnowflakeConfig;
use dozer_types::log::debug;

use crate::errors::{ConnectorError, SnowflakeError, SnowflakeSchemaError};

use crate::connectors::snowflake::schema_helper::SchemaHelper;
use crate::connectors::TableInfo;
use crate::errors::SnowflakeError::QueryError;
use crate::errors::SnowflakeSchemaError::SchemaConversionError;
use dozer_types::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use dozer_types::types::*;
use odbc::ffi::{SqlDataType, SQL_TIMESTAMP_STRUCT};
use odbc::odbc_safe::AutocommitOn;
use odbc::{
    ColumnDescriptor, Connection, Cursor, Data, DiagnosticRecord, Executed, HasResult, NoData,
    ResultSetState, Statement,
};
use std::collections::HashMap;
use std::fmt::Write;

pub fn convert_data(
    cursor: &mut Cursor<Executed, AutocommitOn>,
    i: u16,
    column_descriptor: &ColumnDescriptor,
) -> Result<Field, SnowflakeSchemaError> {
    match column_descriptor.data_type {
        SqlDataType::SQL_CHAR | SqlDataType::SQL_VARCHAR => {
            match cursor
                .get_data::<String>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(value) => Ok(Field::from(value)),
            }
        }
        SqlDataType::SQL_NUMERIC
        | SqlDataType::SQL_DECIMAL
        | SqlDataType::SQL_INTEGER
        | SqlDataType::SQL_SMALLINT => {
            match cursor
                .get_data::<i64>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(value) => Ok(Field::from(value)),
            }
        }
        SqlDataType::SQL_FLOAT | SqlDataType::SQL_REAL | SqlDataType::SQL_DOUBLE => {
            match cursor
                .get_data::<f64>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(value) => Ok(Field::from(value)),
            }
        }
        SqlDataType::SQL_TIMESTAMP => {
            match cursor
                .get_data::<SQL_TIMESTAMP_STRUCT>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(value) => {
                    let date = NaiveDate::from_ymd(
                        value.year as i32,
                        value.month as u32,
                        value.day as u32,
                    );
                    let time = NaiveTime::from_hms_milli(
                        value.hour as u32,
                        value.minute as u32,
                        value.second as u32,
                        value.fraction,
                    );
                    Ok(Field::from(NaiveDateTime::new(date, time)))
                }
            }
        }
        SqlDataType::SQL_EXT_BIT => {
            match cursor
                .get_data::<bool>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(v) => Ok(Field::from(v)),
            }
        }
        _ => Err(SnowflakeSchemaError::ColumnTypeNotSupported(format!(
            "{:?}",
            &column_descriptor.data_type
        ))),
    }
}

pub struct ResultIterator<'a, 'b> {
    stmt: Statement<'a, 'b, Executed, HasResult, AutocommitOn>,
    cols: i16,
    schema: Vec<ColumnDescriptor>,
}

impl Iterator for ResultIterator<'_, '_> {
    type Item = Vec<Option<Field>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt.fetch().unwrap() {
            None => None,
            Some(mut cursor) => {
                let mut values = vec![];
                for i in 1..(self.cols + 1) {
                    let descriptor = self.schema.get((i - 1) as usize)?;
                    let value = convert_data(&mut cursor, i as u16, descriptor).unwrap();
                    values.push(Some(value));
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
        let driver = match &config.driver {
            None => "Snowflake".to_string(),
            Some(driver) => driver.to_string(),
        };
        conn_hashmap.insert("Driver".to_string(), driver);
        conn_hashmap.insert("Server".to_string(), config.clone().server);
        conn_hashmap.insert("Port".to_string(), config.clone().port);
        conn_hashmap.insert("Uid".to_string(), config.clone().user);
        conn_hashmap.insert("Pwd".to_string(), config.clone().password);
        conn_hashmap.insert("Schema".to_string(), config.clone().schema);
        conn_hashmap.insert("Warehouse".to_string(), config.clone().warehouse);
        conn_hashmap.insert("Database".to_string(), config.clone().database);
        conn_hashmap.insert("Role".to_string(), "ACCOUNTADMIN".to_string());

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
        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;

        let result = stmt
            .exec_direct(&query)
            .map_err(|e| QueryError(Box::new(e)))?;
        match result {
            Data(_) => Ok(Some(true)),
            NoData(_) => Ok(None),
        }
    }

    fn parse_not_exist_error(e: DiagnosticRecord) -> Result<bool, SnowflakeError> {
        if e.get_native_error() == 2003 {
            Ok(false)
        } else {
            Err(QueryError(Box::new(e)))
        }
    }

    fn parse_exist(result: ResultSetState<Executed, AutocommitOn>) -> bool {
        match result {
            Data(mut x) => x.fetch().unwrap().is_some(),
            NoData(_) => false,
        }
    }

    pub fn stream_exist(
        &self,
        conn: &Connection<AutocommitOn>,
        stream_name: &String,
    ) -> Result<bool, SnowflakeError> {
        let query = format!("DESCRIBE STREAM {};", stream_name);

        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
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
        let query = format!(
            "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}';",
            table_name
        );

        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
        stmt.exec_direct(&query)
            .map_or_else(Self::parse_not_exist_error, |result| {
                Ok(Self::parse_exist(result))
            })
    }

    pub fn drop_stream(
        &self,
        conn: &Connection<AutocommitOn>,
        stream_name: &String,
    ) -> Result<bool, SnowflakeError> {
        let query = format!("DROP STREAM IF EXISTS {}", stream_name);

        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
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
        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
        // TODO: use stmt.close_cursor to improve efficiency

        match stmt
            .exec_direct(&query)
            .map_err(|e| QueryError(Box::new(e)))?
        {
            Data(stmt) => {
                let cols = stmt
                    .num_result_cols()
                    .map_err(|e| QueryError(Box::new(e)))?;
                let schema_result: Result<Vec<ColumnDescriptor>, SnowflakeError> = (1..(cols + 1))
                    .map(|i| {
                        let value = i.try_into();
                        match value {
                            Ok(v) => {
                                Ok(stmt.describe_col(v).map_err(|e| QueryError(Box::new(e)))?)
                            }
                            Err(e) => Err(SnowflakeError::SnowflakeSchemaError(
                                SchemaConversionError(e),
                            )),
                        }
                    })
                    .collect();

                let schema = schema_result?;
                Ok(Some((
                    schema.clone(),
                    ResultIterator { cols, stmt, schema },
                )))
            }
            NoData(_) => Ok(None),
        }
    }

    pub fn execute_query(
        &self,
        conn: &Connection<AutocommitOn>,
        query: &str,
    ) -> Result<(), SnowflakeError> {
        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
        stmt.exec_direct(query)
            .map_err(|e| QueryError(Box::new(e)))?;
        Ok(())
    }

    pub fn fetch_tables(
        &self,
        tables: Option<Vec<TableInfo>>,
        _config: &SnowflakeConfig,
        conn: &Connection<AutocommitOn>,
    ) -> Result<Vec<SchemaWithChangesType>, SnowflakeError> {
        let tables_condition = tables.map_or("".to_string(), |tables| {
            let mut buf = String::new();
            buf.write_str(" AND TABLE_NAME IN(").unwrap();
            for (idx, table_info) in tables.iter().enumerate() {
                if idx > 0 {
                    buf.write_char(',').unwrap();
                }
                buf.write_str(&format!("\'{}\'", table_info.name)).unwrap();
            }
            buf.write_char(')').unwrap();
            buf
        });

        let query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'PUBLIC' {}
            ORDER BY TABLE_NAME, ORDINAL_POSITION",
            tables_condition
        );

        let stmt = Statement::with_parent(conn).map_err(|e| QueryError(Box::new(e)))?;
        match stmt
            .exec_direct(&query)
            .map_err(|e| QueryError(Box::new(e)))?
        {
            Data(data) => {
                let cols = data
                    .num_result_cols()
                    .map_err(|e| QueryError(Box::new(e)))?;

                let schema_result: Result<Vec<ColumnDescriptor>, SnowflakeError> = (1..(cols + 1))
                    .map(|i| {
                        let value = i.try_into();
                        match value {
                            Ok(v) => {
                                Ok(data.describe_col(v).map_err(|e| QueryError(Box::new(e)))?)
                            }
                            Err(e) => Err(SnowflakeError::SnowflakeSchemaError(
                                SchemaConversionError(e),
                            )),
                        }
                    })
                    .collect();

                let schema = schema_result?;

                let mut schemas: HashMap<String, Schema> = HashMap::new();
                let iterator = ResultIterator {
                    cols,
                    stmt: data,
                    schema,
                };

                for row_data in iterator {
                    let empty = "".to_string();
                    let table_name =
                        if let Some(Field::String(table_name)) = &row_data.get(1).unwrap() {
                            table_name
                        } else {
                            &empty
                        };
                    let field_name =
                        if let Some(Field::String(field_name)) = &row_data.get(2).unwrap() {
                            field_name
                        } else {
                            &empty
                        };
                    let type_name =
                        if let Some(Field::String(type_name)) = &row_data.get(3).unwrap() {
                            type_name
                        } else {
                            &empty
                        };
                    let nullable = if let Some(Field::Boolean(b)) = &row_data.get(4).unwrap() {
                        b
                    } else {
                        &false
                    };
                    let scale = if let Some(Field::Int(scale)) = &row_data.get(5).unwrap() {
                        Some(*scale)
                    } else {
                        None
                    };

                    schemas
                        .entry(table_name.clone())
                        .or_insert(Schema {
                            identifier: Some(SchemaIdentifier { id: 0, version: 0 }),
                            fields: vec![],
                            primary_index: vec![],
                        })
                        .fields
                        .push(FieldDefinition {
                            name: field_name.clone(),
                            typ: SchemaHelper::map_schema_type(type_name, scale)?,
                            nullable: *nullable,
                        })
                }

                Ok(schemas
                    .into_iter()
                    .map(|(name, schema)| {
                        (name, schema, ReplicationChangesTrackingType::FullChanges)
                    })
                    .collect())
            }
            NoData(_) => Ok(vec![]),
        }
    }
}
