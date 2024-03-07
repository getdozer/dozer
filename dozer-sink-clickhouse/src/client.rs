#![allow(dead_code)]
use super::ddl::get_create_table_query;
use super::types::ValueWrapper;
use crate::errors::QueryError;
use crate::types::{insert_multi, map_value_wrapper_to_field};
use clickhouse_rs::{ClientHandle, Pool};
use dozer_types::log::{debug, info};
use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::types::{Field, FieldDefinition};
use serde::Serialize;

pub struct SqlResult {
    pub rows: Vec<Vec<Field>>,
}

#[derive(Clone)]
pub struct ClickhouseClient {
    pool: Pool,
    config: ClickhouseSinkConfig,
}
#[derive(Debug, Clone, Serialize)]
pub struct QueryId(pub String);

#[derive(Debug, Clone, Serialize)]
#[serde(crate = "dozer_types::serde")]

pub struct QueryLog {
    pub query_duration_ms: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub memory_usage: u64,
}
pub struct ClickhouseOptionsWrapper(ClickhouseSinkConfig);

impl ClickhouseClient {
    pub fn new(config: ClickhouseSinkConfig) -> Self {
        let url = Self::construct_url(&config);
        let pool = Pool::new(url);
        Self { pool, config }
    }

    pub fn construct_url(config: &ClickhouseSinkConfig) -> String {
        let user_password = match &config.password {
            Some(password) => format!("{}:{}", config.user, password),
            None => config.user.to_string(),
        };

        let url = format!(
            "{}://{}@{}:{}/{}",
            config.scheme, user_password, config.host, config.port, config.database
        );
        debug!("{url}");
        url
    }

    pub fn get_log_query(log_comment: &str) -> String {
        format!(
            r#"
                SELECT 
                query_duration_ms,
                read_rows,
                read_bytes,
                written_rows,
                written_bytes,
                result_rows,
                result_bytes,
                memory_usage
                FROM system.query_log WHERE 
                log_comment = '{}'    
                "#,
            log_comment
        )
    }
    pub async fn get_client_handle(&self) -> Result<ClientHandle, QueryError> {
        let client = self.pool.get_handle().await?;
        Ok(client)
    }

    pub async fn drop_table(&self, datasource_name: &str) -> Result<(), QueryError> {
        let mut client = self.pool.get_handle().await?;
        let ddl = format!("DROP TABLE IF EXISTS {}", datasource_name);
        println!("#{ddl}");
        client.execute(ddl).await?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        datasource_name: &str,
        fields: &[FieldDefinition],
    ) -> Result<(), QueryError> {
        let mut client = self.pool.get_handle().await?;
        let ddl = get_create_table_query(datasource_name, fields, self.config.clone());
        info!("Creating Clickhouse Sink Table");
        info!("{ddl}");
        client.execute(ddl).await?;
        Ok(())
    }

    pub async fn fetch_all(
        &self,
        query: &str,
        schema: Vec<FieldDefinition>,
        query_id: Option<String>,
    ) -> Result<SqlResult, QueryError> {
        let mut client = self.pool.get_handle().await?;
        // TODO: query_id doesnt work
        // https://github.com/suharev7/clickhouse-rs/issues/176
        // let query = Query::new(sql).id(query_id.to_string())
        let query = query_id.map_or(query.to_string(), |id| {
            format!("{0} settings log_comment = '{1}'", query, id)
        });

        let block = client.query(&query).fetch_all().await?;

        let mut rows: Vec<Vec<Field>> = vec![];
        for row in block.rows() {
            let mut row_data = vec![];
            for (idx, field) in schema.clone().into_iter().enumerate() {
                let v: ValueWrapper = row.get(idx)?;
                row_data.push(map_value_wrapper_to_field(v, field)?);
            }
            rows.push(row_data);
        }

        Ok(SqlResult { rows })
    }

    pub async fn _fetch_query_log(&self, query_id: String) -> Result<QueryLog, QueryError> {
        let mut client = self.pool.get_handle().await?;
        let query = Self::get_log_query(&query_id);
        let block = client.query(query).fetch_all().await?;
        let first_row = block.rows().next();

        if let Some(row) = first_row {
            let query_log = QueryLog {
                query_duration_ms: row.get("query_duration_ms")?,
                read_rows: row.get("read_rows")?,
                read_bytes: row.get("read_bytes")?,
                written_rows: row.get("written_rows")?,
                written_bytes: row.get("written_bytes")?,
                result_rows: row.get("result_rows")?,
                result_bytes: row.get("result_bytes")?,
                memory_usage: row.get("memory_usage")?,
            };
            Ok(query_log)
        } else {
            Err(QueryError::CustomError(format!(
                "No query log found for {0}",
                query_id
            )))
        }
    }

    pub async fn check_table(&self, table_name: &str) -> Result<bool, QueryError> {
        let mut client = self.pool.get_handle().await?;
        let query = format!("CHECK TABLE {}", table_name);
        client.query(query).fetch_all().await?;

        // if error not found, table exists
        Ok(true)
    }

    pub async fn create_materialized_view(
        &self,
        name: &str,
        target_table: &str,
        query: &str,
    ) -> Result<(), QueryError> {
        let mut client = self.pool.get_handle().await?;
        let ddl = format!(
            "CREATE MATERIALIZED VIEW {} TO {} AS {}",
            name, target_table, query
        );
        client.execute(ddl).await?;
        Ok(())
    }

    pub async fn insert(
        &self,
        table_name: &str,
        fields: &[FieldDefinition],
        values: &[Field],
    ) -> Result<(), QueryError> {
        let client = self.pool.get_handle().await?;
        insert_multi(client, table_name, fields, &[values.to_vec()]).await
    }

    pub async fn insert_multi(
        &self,
        table_name: &str,
        fields: &[FieldDefinition],
        values: &[Vec<Field>],
    ) -> Result<(), QueryError> {
        let client = self.pool.get_handle().await?;
        insert_multi(client, table_name, fields, values).await
    }
}
