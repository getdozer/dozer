use crate::ClickhouseSinkError::SinkTableDoesNotExist;
use crate::{ddl, ClickhouseSinkError};
use clickhouse::{Client, Row};
use dozer_types::errors::internal::BoxedError;
use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::{FieldType, Schema};

#[derive(Debug, Row, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub(crate) struct ClickhouseSchemaColumn {
    pub(crate) name: String,
    pub(crate) r#type: String,
    pub(crate) default_type: String,
    pub(crate) default_expression: String,
    pub(crate) comment: String,
    pub(crate) codec_expression: String,
    pub(crate) ttl_expression: String,
}

#[derive(Debug, Row, Deserialize, Serialize, Clone)]
#[serde(crate = "dozer_types::serde")]
pub(crate) struct ClickhouseTable {
    pub(crate) database: String,
    pub(crate) name: String,
    pub(crate) engine: String,
    pub(crate) engine_full: String,
}

#[derive(Debug, Row, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub(crate) struct ClickhouseKeyColumnDef {
    pub(crate) column_name: Option<String>,
    pub(crate) constraint_name: Option<String>,
    pub(crate) constraint_schema: String,
}

pub struct ClickhouseSchema {}

impl ClickhouseSchema {
    pub async fn get_clickhouse_table(
        client: &Client,
        config: &ClickhouseSinkConfig,
        dozer_schema: &Schema,
    ) -> Result<ClickhouseTable, ClickhouseSinkError> {
        match Self::fetch_sink_table_info(client, &config.sink_table_name).await {
            Ok(table) => {
                ClickhouseSchema::compare_with_dozer_schema(
                    client,
                    dozer_schema.clone(),
                    table.clone(),
                )
                .await?;
                Ok(table)
            }
            Err(ClickhouseSinkError::ClickhouseQueryError(
                clickhouse::error::Error::RowNotFound,
            )) => {
                if config.create_table_options.is_none() {
                    Err(SinkTableDoesNotExist)
                } else {
                    let create_table_query = ddl::ClickhouseDDL::get_create_table_query(
                        config.sink_table_name.clone(),
                        dozer_schema.clone(),
                        config.create_table_options.clone(),
                        config.primary_keys.clone(),
                    );

                    client.query(&create_table_query).execute().await?;
                    Self::fetch_sink_table_info(client, &config.sink_table_name).await
                }
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_primary_keys(
        client: &Client,
        config: &ClickhouseSinkConfig,
    ) -> Result<Vec<String>, BoxedError> {
        let existing_pk =
            Self::fetch_primary_keys(client, &config.sink_table_name, &config.database).await?;

        if let Some(expected_pk) = &config.primary_keys {
            if expected_pk.len() != existing_pk.len() {
                return Err(ClickhouseSinkError::PrimaryKeyMismatch(
                    expected_pk.clone(),
                    existing_pk.clone(),
                )
                .into());
            }

            for pk in expected_pk {
                if !existing_pk.iter().any(|existing_pk| existing_pk == pk) {
                    return Err(ClickhouseSinkError::PrimaryKeyMismatch(
                        expected_pk.clone(),
                        existing_pk.clone(),
                    )
                    .into());
                }
            }
        }

        Ok(existing_pk)
    }
    async fn compare_with_dozer_schema(
        client: &Client,
        schema: Schema,
        table: ClickhouseTable,
    ) -> Result<(), ClickhouseSinkError> {
        let columns: Vec<ClickhouseSchemaColumn> = client
            .query(&format!(
                "DESCRIBE TABLE {database}.{table_name}",
                table_name = table.name,
                database = table.database
            ))
            .fetch_all::<ClickhouseSchemaColumn>()
            .await?;

        for field in schema.fields {
            let Some(column) = columns.iter().find(|column| column.name == field.name) else {
                return Err(ClickhouseSinkError::ColumnNotFound(field.name));
            };

            let mut expected_type = match field.typ {
                FieldType::UInt => "UInt64",
                FieldType::U128 => "Uint128",
                FieldType::Int => "Int64",
                FieldType::I128 => "Int128",
                FieldType::Float => "Float64",
                FieldType::Boolean => "Bool",
                FieldType::String | FieldType::Text => "String",
                FieldType::Binary => "UInt8",
                FieldType::Decimal => "Decimal64",
                FieldType::Timestamp => "DateTime64(9)",
                FieldType::Date => "Date",
                FieldType::Json => "Json",
                FieldType::Point => "Point",
                FieldType::Duration => {
                    return Err(ClickhouseSinkError::TypeNotSupported(
                        field.name,
                        "Duration".to_string(),
                    ))
                }
            }
            .to_string();

            if field.nullable {
                expected_type = format!("Nullable({expected_type})");
            }

            if field.typ == FieldType::Binary {
                expected_type = format!("Array({expected_type})");
            }

            let column_type = column.r#type.clone();
            if expected_type != column_type {
                return Err(ClickhouseSinkError::ColumnTypeMismatch(
                    field.name,
                    expected_type.to_string(),
                    column_type.to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn fetch_sink_table_info(
        client: &Client,
        sink_table_name: &str,
    ) -> Result<ClickhouseTable, ClickhouseSinkError> {
        Ok(client
            .query("SELECT database, name, engine, engine_full FROM system.tables WHERE name = ?")
            .bind(sink_table_name)
            .fetch_one::<ClickhouseTable>()
            .await?)
    }

    async fn fetch_primary_keys(
        client: &Client,
        sink_table_name: &str,
        schema: &str,
    ) -> Result<Vec<String>, ClickhouseSinkError> {
        Ok(client
            .query("SELECT ?fields FROM INFORMATION_SCHEMA.key_column_usage WHERE table_name = ? AND constraint_schema = ?")
            .bind(sink_table_name)
            .bind(schema)
            .fetch_all::<ClickhouseKeyColumnDef>()
            .await?
            .iter()
            .filter_map(|key_column_def| {
                key_column_def.column_name.clone()
            })
            .collect()
        )
    }
}
