#![allow(dead_code)]
use super::ddl::get_create_table_query;
use super::types::ValueWrapper;
use crate::errors::QueryError;
use crate::types::{insert_multi, map_value_wrapper_to_field};
use clickhouse_rs::types::Query;
use clickhouse_rs::{ClientHandle, Pool};
use dozer_types::log::{debug, info};
use dozer_types::models::sink::{ClickhouseSinkConfig, ClickhouseTableOptions};
use dozer_types::types::{Field, FieldDefinition};
pub struct SqlResult {
    pub rows: Vec<Vec<Field>>,
}

#[derive(Clone)]
pub struct ClickhouseClient {
    pool: Pool,
}

impl ClickhouseClient {
    pub fn new(config: ClickhouseSinkConfig) -> Self {
        let url = Self::construct_url(&config);
        let pool = Pool::new(url);
        Self { pool }
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

    pub async fn get_client_handle(&self) -> Result<ClientHandle, QueryError> {
        let client = self.pool.get_handle().await?;
        Ok(client)
    }

    pub async fn drop_table(&self, datasource_name: &str) -> Result<(), QueryError> {
        let mut client = self.pool.get_handle().await?;
        let ddl = format!("DROP TABLE IF EXISTS {}", datasource_name);
        info!("#{ddl}");
        client.execute(ddl).await?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        datasource_name: &str,
        fields: &[FieldDefinition],
        table_options: Option<ClickhouseTableOptions>,
    ) -> Result<(), QueryError> {
        let mut client = self.pool.get_handle().await?;
        let ddl = get_create_table_query(datasource_name, fields, table_options);
        info!("Creating Clickhouse Table");
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
        // TODO: Include query_id in RowBinary protocol.
        // https://github.com/suharev7/clickhouse-rs/issues/176
        // https://github.com/ClickHouse/ClickHouse/blob/master/src/Client/Connection.cpp
        // https://www.propeldata.com/blog/how-to-check-your-clickhouse-version

        let query = Query::new(&query).id(query_id.map_or("".to_string(), |q| q.to_string()));
        // let query = query_id.map_or(query.to_string(), |id| {
        //     format!("{0} settings log_comment = '{1}'", query, id)
        // });

        let block = client.query(query).fetch_all().await?;

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

    pub async fn check_table(&self, table_name: &str) -> Result<bool, QueryError> {
        let mut client = self.pool.get_handle().await?;
        let query = format!("CHECK TABLE {}", table_name);
        client.query(query).fetch_all().await?;

        // if error not found, table exists
        Ok(true)
    }

    pub async fn insert(
        &self,
        table_name: &str,
        fields: &[FieldDefinition],
        values: &[Field],
        query_id: Option<String>,
    ) -> Result<(), QueryError> {
        let client = self.pool.get_handle().await?;
        insert_multi(client, table_name, fields, &[values.to_vec()], query_id).await
    }

    pub async fn insert_multi(
        &self,
        table_name: &str,
        fields: &[FieldDefinition],
        values: &[Vec<Field>],
        query_id: Option<String>,
    ) -> Result<(), QueryError> {
        let client = self.pool.get_handle().await?;
        insert_multi(client, table_name, fields, values, query_id).await
    }
}
