use dozer_ingestion_connector::{
    dozer_types::{
        chrono::{NaiveDate, NaiveDateTime, NaiveTime},
        indexmap::IndexMap,
        log::debug,
        models::ingestion_types::SnowflakeConfig,
        models::sink_config::snowflake::ConnectionParameters as SnowflakeSinkConfig,
        rust_decimal::Decimal,
        types::*,
    },
    CdcType, SourceSchema,
};
use odbc::ffi::{SqlDataType, SQL_DATE_STRUCT, SQL_TIMESTAMP_STRUCT};
use odbc::odbc_safe::{AutocommitOn, Odbc3};
use odbc::{ColumnDescriptor, Cursor, DiagnosticRecord, Environment, Executed, HasResult};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::fmt::Write;
use std::ops::Deref;
use std::{collections::HashMap, vec};

use crate::{
    schema_helper::SchemaHelper, SnowflakeError, SnowflakeSchemaError, SnowflakeStreamError,
};

use super::pool::{Conn, Pool};
use super::{helpers::is_network_failure, params::OdbcValue};

fn convert_decimal(bytes: &[u8], scale: u16) -> Result<Field, SnowflakeSchemaError> {
    let is_negative = bytes[bytes.len() - 4] == 255;
    let mut multiplier: i64 = 1;
    let mut result: i64 = 0;
    let bytes: &[u8] = &bytes[4..11];
    bytes.iter().for_each(|w| {
        let number = *w as i64;
        result += number * multiplier;
        multiplier *= 256;
    });

    if is_negative {
        result = -result;
    }

    Ok(Field::from(
        Decimal::try_new(result, scale as u32)
            .map_err(SnowflakeSchemaError::DecimalConvertError)?,
    ))
}

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
        SqlDataType::SQL_DECIMAL
        | SqlDataType::SQL_NUMERIC
        | SqlDataType::SQL_INTEGER
        | SqlDataType::SQL_SMALLINT => match column_descriptor.decimal_digits {
            None => {
                match cursor
                    .get_data::<i64>(i)
                    .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
                {
                    None => Ok(Field::Null),
                    Some(value) => Ok(Field::from(value)),
                }
            }
            Some(digits) => {
                match cursor
                    .get_data::<&[u8]>(i)
                    .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
                {
                    None => Ok(Field::Null),
                    Some(value) => convert_decimal(value, digits),
                }
            }
        },
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
                    let date = NaiveDate::from_ymd_opt(
                        value.year as i32,
                        value.month as u32,
                        value.day as u32,
                    )
                    .map_or_else(|| Err(SnowflakeSchemaError::InvalidDateError), Ok)?;
                    let time = NaiveTime::from_hms_nano_opt(
                        value.hour as u32,
                        value.minute as u32,
                        value.second as u32,
                        value.fraction,
                    )
                    .map_or_else(|| Err(SnowflakeSchemaError::InvalidTimeError), Ok)?;
                    Ok(Field::from(NaiveDateTime::new(date, time)))
                }
            }
        }
        SqlDataType::SQL_DATE => {
            match cursor
                .get_data::<SQL_DATE_STRUCT>(i)
                .map_err(|e| SnowflakeSchemaError::ValueConversionError(Box::new(e)))?
            {
                None => Ok(Field::Null),
                Some(value) => {
                    let date = NaiveDate::from_ymd_opt(
                        value.year as i32,
                        value.month as u32,
                        value.day as u32,
                    )
                    .map_or_else(|| Err(SnowflakeSchemaError::InvalidDateError), Ok)?;
                    Ok(Field::from(date))
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

pub struct ClientConfig {
    pub server: String,
    pub port: Option<String>,
    pub user: String,
    pub password: String,

    pub role: Option<String>,
    pub driver: Option<String>,

    pub warehouse: String,

    pub database: Option<String>,
    pub schema: Option<String>,
}

pub struct Client<'env> {
    pool: Pool<'env>,
    name: String,
}

impl<'env> Client<'env> {
    pub fn new(config: ClientConfig, env: &'env Environment<Odbc3>) -> Self {
        let mut conn_hashmap: HashMap<String, String> = HashMap::new();
        let driver = match &config.driver {
            None => "Snowflake".to_string(),
            Some(driver) => driver.to_string(),
        };

        conn_hashmap.insert("Driver".to_string(), driver);
        conn_hashmap.insert("Server".to_string(), config.server);
        conn_hashmap.insert(
            "Port".to_string(),
            config.port.unwrap_or_else(|| "443".to_string()),
        );
        conn_hashmap.insert("Uid".to_string(), config.user);
        conn_hashmap.insert("Pwd".to_string(), config.password);
        if let Some(schema) = config.schema {
            conn_hashmap.insert("Schema".to_string(), schema);
        }
        conn_hashmap.insert("Warehouse".to_string(), config.warehouse);
        if let Some(database) = config.database {
            conn_hashmap.insert("Database".to_string(), database);
        }
        if let Some(role) = config.role {
            conn_hashmap.insert("Role".to_string(), role);
        }

        let mut parts = vec![];
        conn_hashmap.keys().for_each(|k| {
            parts.push(format!("{}={}", k, conn_hashmap.get(k).unwrap()));
        });

        let conn_string = parts.join(";");

        debug!("Snowflake conn string: {:?}", conn_string);
        let name = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        let pool = Pool::new(env, conn_string);
        Self { pool, name }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn exec(&self, query: &str) -> Result<(), SnowflakeError> {
        exec_drop(&self.pool, query, &[]).map_err(SnowflakeError::QueryError)
    }

    pub fn exec_with_params(
        &self,
        query: &str,
        params: &[OdbcValue],
    ) -> Result<(), SnowflakeError> {
        exec_drop(&self.pool, query, params).map_err(SnowflakeError::QueryError)
    }

    pub fn exec_stream_creation(&self, query: String) -> Result<bool, SnowflakeError> {
        let result = exec_drop(&self.pool, &query, &[]);
        result.map_or_else(
            |e| {
                if e.get_native_error() == 2203 {
                    Ok(false)
                } else if e.get_native_error() == 707 {
                    Err(SnowflakeError::SnowflakeStreamError(
                        SnowflakeStreamError::TimeTravelNotAvailableError,
                    ))
                } else {
                    Err(SnowflakeError::QueryError(e))
                }
            },
            |_| Ok(true),
        )
    }

    pub fn parse_stream_creation_error(e: Box<DiagnosticRecord>) -> Result<bool, SnowflakeError> {
        if e.get_native_error() == 2203 {
            Ok(false)
        } else {
            Err(SnowflakeError::QueryError(e))
        }
    }

    fn parse_not_exist_error(e: Box<DiagnosticRecord>) -> Result<bool, SnowflakeError> {
        if e.get_native_error() == 2003 {
            Ok(false)
        } else {
            Err(SnowflakeError::QueryError(e))
        }
    }

    pub fn stream_exist(&self, stream_name: &String) -> Result<bool, SnowflakeError> {
        let query = format!("SHOW STREAMS LIKE '{stream_name}';");

        exec_first_exists(&self.pool, &query).map_or_else(Self::parse_not_exist_error, Ok)
    }

    pub fn drop_stream(&self, stream_name: &String) -> Result<bool, SnowflakeError> {
        let query = format!("DROP STREAM IF EXISTS {stream_name}");

        exec_first_exists(&self.pool, &query).map_or_else(Self::parse_not_exist_error, Ok)
    }

    pub fn fetch(&self, query: String) -> Result<ExecIter<'env>, SnowflakeError> {
        exec_iter(self.pool.clone(), query, vec![])
    }

    #[allow(clippy::type_complexity)]
    pub fn fetch_tables(
        &self,
        tables_indexes: Option<HashMap<String, usize>>,
        keys: HashMap<String, Vec<String>>,
        schema_name: String,
    ) -> Result<Vec<Result<(String, SourceSchema), SnowflakeError>>, SnowflakeError> {
        let tables_condition = tables_indexes.as_ref().map_or("".to_string(), |tables| {
            let mut buf = String::new();
            buf.write_str(" AND TABLE_NAME IN(").unwrap();
            for (idx, table_name) in tables.keys().enumerate() {
                if idx > 0 {
                    buf.write_char(',').unwrap();
                }
                buf.write_str(&format!("\'{}\'", table_name)).unwrap();
            }
            buf.write_char(')').unwrap();
            buf
        });

        let query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema_name}' {tables_condition}
            ORDER BY TABLE_NAME, ORDINAL_POSITION"
        );

        let results = exec_iter(self.pool.clone(), query, vec![])?;
        let mut schemas: IndexMap<String, (usize, Result<Schema, SnowflakeSchemaError>)> =
            IndexMap::new();
        for (idx, result) in results.enumerate() {
            let row_data = result?;
            let empty = "".to_string();
            let table_name = if let Field::String(table_name) = &row_data.get(1).unwrap() {
                table_name
            } else {
                &empty
            };
            let field_name = if let Field::String(field_name) = &row_data.get(2).unwrap() {
                field_name
            } else {
                &empty
            };
            let type_name = if let Field::String(type_name) = &row_data.get(3).unwrap() {
                type_name
            } else {
                &empty
            };
            let nullable = if let Field::String(b) = &row_data.get(4).unwrap() {
                let is_nullable = b == "NO";
                if is_nullable {
                    &true
                } else {
                    &false
                }
            } else {
                &false
            };
            let scale = if let Field::Int(scale) = &row_data.get(5).unwrap() {
                Some(*scale)
            } else {
                None
            };

            let table_index = match &tables_indexes {
                None => idx,
                Some(indexes) => *indexes.get(table_name).unwrap_or(&idx),
            };

            match SchemaHelper::map_schema_type(type_name, scale) {
                Ok(typ) => {
                    if let Ok(schema) = schemas
                        .entry(table_name.clone())
                        .or_insert((
                            table_index,
                            Ok(Schema {
                                fields: vec![],
                                primary_index: vec![],
                            }),
                        ))
                        .1
                        .as_mut()
                    {
                        schema.fields.push(FieldDefinition {
                            name: field_name.clone(),
                            typ,
                            nullable: *nullable,
                            source: SourceDefinition::Dynamic,
                        });
                    }
                }
                Err(e) => {
                    schemas.insert(table_name.clone(), (table_index, Err(e)));
                }
            }
        }

        schemas.sort_by(|_, a, _, b| a.0.cmp(&b.0));

        Ok(schemas
            .into_iter()
            .map(|(name, (_, schema))| match schema {
                Ok(mut schema) => {
                    let mut indexes = vec![];
                    if let Some(columns) = keys.get(&name) {
                        schema.fields.iter().enumerate().for_each(|(idx, f)| {
                            if columns.contains(&f.name) {
                                indexes.push(idx);
                            }
                        });
                    }

                    let cdc_type = if indexes.is_empty() {
                        CdcType::Nothing
                    } else {
                        CdcType::FullChanges
                    };

                    schema.primary_index = indexes;

                    Ok((name, SourceSchema::new(schema, cdc_type)))
                }
                Err(e) => Err(SnowflakeError::SnowflakeSchemaError(e)),
            })
            .collect())
    }

    pub fn fetch_keys(&self) -> Result<HashMap<String, Vec<String>>, SnowflakeError> {
        'retry: loop {
            let query = "SHOW PRIMARY KEYS IN SCHEMA".to_string();
            let results = exec_iter(self.pool.clone(), query, vec![])?;
            let mut keys: HashMap<String, Vec<String>> = HashMap::new();
            for result in results {
                let row_data = match result {
                    Err(SnowflakeError::NonResumableQuery(_)) => continue 'retry,
                    result => result?,
                };
                let empty = "".to_string();
                let table_name = row_data.get(3).map_or(empty.clone(), |v| match v {
                    Field::String(v) => v.clone(),
                    _ => empty.clone(),
                });
                let column_name = row_data.get(4).map_or(empty.clone(), |v| match v {
                    Field::String(v) => v.clone(),
                    _ => empty.clone(),
                });

                keys.entry(table_name).or_default().push(column_name);
            }
            break Ok(keys);
        }
    }
}

impl From<SnowflakeConfig> for ClientConfig {
    fn from(
        SnowflakeConfig {
            server,
            port,
            user,
            password,
            database,
            schema,
            warehouse,
            driver,
            role,
            ..
        }: SnowflakeConfig,
    ) -> Self {
        Self {
            server,
            port: Some(port),
            user,
            password,
            role: Some(role),
            driver,
            warehouse,
            database: Some(database),
            schema: Some(schema),
        }
    }
}

impl From<SnowflakeSinkConfig> for ClientConfig {
    fn from(
        SnowflakeSinkConfig {
            server,
            port,
            user,
            password,
            driver,
            role,
            warehouse,
        }: SnowflakeSinkConfig,
    ) -> Self {
        Self {
            server,
            port,
            user,
            password,
            role,
            driver,
            warehouse,
            database: None,
            schema: None,
        }
    }
}

macro_rules! retry {
    ($operation:expr $(, $label:tt)? $(,)?) => {
        match $operation {
            Err(err) if is_network_failure(&err) => continue $($label)?,
            result => result,
        }
    };
}

fn add_query_offset(query: &str, offset: u64) -> Result<String, SnowflakeError> {
    if offset == 0 {
        Ok(query.into())
    } else {
        let resumable = query
            .trim_start()
            .get(0..7)
            .map(|s| s.to_uppercase() == "SELECT ")
            .unwrap_or(false);

        if resumable {
            Ok(format!(
                "{query} LIMIT 18446744073709551615 OFFSET {offset}"
            ))
        } else {
            Err(SnowflakeError::NonResumableQuery(query.to_string()))
        }
    }
}

fn get_fields_from_cursor(
    mut cursor: Cursor<Executed, AutocommitOn>,
    cols: i16,
    schema: &[ColumnDescriptor],
) -> Result<Vec<Field>, SnowflakeError> {
    let mut values = vec![];
    for i in 1..(cols + 1) {
        let descriptor = schema.get((i - 1) as usize).unwrap();
        let value = convert_data(&mut cursor, i as u16, descriptor)?;
        values.push(value);
    }

    Ok(values)
}

fn exec_drop(pool: &Pool, query: &str, params: &[OdbcValue]) -> Result<(), Box<DiagnosticRecord>> {
    let conn = pool.get_conn()?;
    {
        let _result = exec_helper(&conn, query, params)?;
    }
    conn.return_();
    Ok(())
}

fn exec_first_exists(pool: &Pool, query: &str) -> Result<bool, Box<DiagnosticRecord>> {
    loop {
        let conn = pool.get_conn()?;
        let result = match exec_helper(&conn, query, &[])? {
            Some(mut data) => retry!(data.fetch())?.is_some(),
            None => false,
        };
        conn.return_();
        break Ok(result);
    }
}

fn exec_iter(
    pool: Pool,
    query: String,
    params: Vec<OdbcValue>,
) -> Result<ExecIter, SnowflakeError> {
    use genawaiter::{
        rc::{gen, Gen},
        yield_,
    };
    use ExecIterResult::*;

    let mut generator: Gen<ExecIterResult, (), _> = gen!({
        let mut cursor_position = 0u64;
        'retry: loop {
            let conn = pool.get_conn().map_err(SnowflakeError::QueryError)?;
            {
                let mut data =
                    match exec_helper(&conn, &add_query_offset(&query, cursor_position)?, &params)
                        .map_err(SnowflakeError::QueryError)?
                    {
                        Some(data) => data,
                        None => break,
                    };
                let cols = data
                    .num_result_cols()
                    .map_err(|e| SnowflakeError::QueryError(e.into()))?;
                let mut schema = Vec::new();
                for i in 1..(cols + 1) {
                    let value = i.try_into();
                    let column_descriptor = match value {
                        Ok(v) => data
                            .describe_col(v)
                            .map_err(|e| SnowflakeError::QueryError(e.into()))?,
                        Err(e) => Err(SnowflakeSchemaError::SchemaConversionError(e))?,
                    };
                    schema.push(column_descriptor)
                }
                yield_!(Schema(schema.clone()));

                while let Some(cursor) =
                    retry!(data.fetch(),'retry).map_err(|e| SnowflakeError::QueryError(e.into()))?
                {
                    let fields = get_fields_from_cursor(cursor, cols, &schema)?;
                    yield_!(Row(fields));
                    cursor_position += 1;
                }
            }
            conn.return_();
            break;
        }
        Ok::<(), SnowflakeError>(())
    });

    let mut iterator = std::iter::from_fn(move || {
        use genawaiter::GeneratorState::*;
        match generator.resume() {
            Yielded(fields) => Some(Ok(fields)),
            Complete(Err(err)) => Some(Err(err)),
            Complete(Ok(())) => None,
        }
    });

    let schema = match iterator.next() {
        Some(Ok(Schema(schema))) => Some(schema),
        Some(Err(err)) => Err(err)?,
        None => None,
        _ => unreachable!(),
    };

    Ok(ExecIter {
        iterator: Box::new(iterator),
        schema,
    })
}

enum ExecIterResult {
    Schema(Vec<ColumnDescriptor>),
    Row(Vec<Field>),
}

pub struct ExecIter<'env> {
    iterator: Box<dyn Iterator<Item = Result<ExecIterResult, SnowflakeError>> + 'env>,
    schema: Option<Vec<ColumnDescriptor>>,
}

impl<'env> ExecIter<'env> {
    pub fn schema(&self) -> Option<&Vec<ColumnDescriptor>> {
        self.schema.as_ref()
    }
}

impl<'env> Iterator for ExecIter<'env> {
    type Item = Result<Vec<Field>, SnowflakeError>;

    fn next(&mut self) -> Option<Self::Item> {
        use ExecIterResult::*;
        loop {
            let result = match self.iterator.next()? {
                Ok(Schema(schema)) => {
                    self.schema = Some(schema);
                    continue;
                }
                Ok(Row(row)) => Ok(row),
                Err(err) => Err(err),
            };
            return Some(result);
        }
    }
}

fn exec_helper<'a>(
    conn: &'a Conn<'_>,
    query: &str,
    params: &'a [OdbcValue],
) -> Result<Option<odbc::Statement<'a, 'a, Executed, HasResult, AutocommitOn>>, Box<DiagnosticRecord>>
{
    loop {
        let mut statement = retry!(odbc::Statement::with_parent(conn.deref()))?;
        for (i, param) in params.iter().enumerate() {
            let parameter_index = i as u16 + 1;
            statement = param.bind(statement, parameter_index)?
        }
        let result = retry!(statement.exec_direct(query))?;
        break match result {
            odbc::ResultSetState::Data(data) => Ok(Some(data)),
            odbc::ResultSetState::NoData(_) => Ok(None),
        };
    }
}
