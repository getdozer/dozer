use crate::client::ClickhouseClient;
use crate::errors::ClickhouseSinkError::{self, SinkTableDoesNotExist};
use clickhouse_rs::types::Complex;
use clickhouse_rs::{Block, ClientHandle};
use dozer_types::errors::internal::BoxedError;
use dozer_types::log::warn;
use dozer_types::models::sink::ClickhouseSinkConfig;
use dozer_types::serde::{Deserialize, Serialize};
use dozer_types::types::{FieldDefinition, FieldType, Schema};

#[derive(Debug, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub struct ClickhouseSchemaColumn {
    name: String,
    type_: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(crate = "dozer_types::serde")]
pub(crate) struct ClickhouseTable {
    pub(crate) database: String,
    pub(crate) name: String,
    pub(crate) engine: String,
    pub(crate) engine_full: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(crate = "dozer_types::serde")]
pub(crate) struct ClickhouseKeyColumnDef {
    pub(crate) column_name: Option<String>,
    pub(crate) constraint_name: Option<String>,
    pub(crate) constraint_schema: String,
}

pub struct ClickhouseSchema {}

impl ClickhouseSchema {
    pub async fn get_clickhouse_table(
        client: ClickhouseClient,
        config: &ClickhouseSinkConfig,
    ) -> Result<ClickhouseTable, ClickhouseSinkError> {
        let mut client = client.get_client_handle().await?;
        let query = format!("DESCRIBE TABLE {}", config.sink_table_name);
        let block: Block<Complex> = client.query(&query).fetch_all().await?;

        if block.row_count() == 0 {
            Err(SinkTableDoesNotExist)
        } else {
            Self::fetch_sink_table_info(client, &config.sink_table_name).await
        }
    }

    pub async fn get_primary_keys(
        client: ClickhouseClient,
        config: &ClickhouseSinkConfig,
    ) -> Result<Vec<String>, BoxedError> {
        let handle = client.get_client_handle().await?;
        let existing_pk =
            Self::fetch_primary_keys(handle, &config.sink_table_name, &config.database).await?;

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

    pub async fn compare_with_dozer_schema(
        client: ClickhouseClient,
        schema: &Schema,
        table: &ClickhouseTable,
    ) -> Result<(), ClickhouseSinkError> {
        let mut client = client.get_client_handle().await?;
        let block: Block<Complex> = client
            .query(&format!(
                "DESCRIBE TABLE {database}.{table_name}",
                table_name = table.name,
                database = table.database
            ))
            .fetch_all()
            .await?;

        let columns: Vec<ClickhouseSchemaColumn> = block
            .rows()
            .map(|row| {
                let column_name: String = row.get("name").unwrap();
                let column_type: String = row.get("type").unwrap();
                ClickhouseSchemaColumn {
                    name: column_name,
                    type_: column_type,
                }
            })
            .collect();

        for field in &schema.fields {
            let Some(column) = columns.iter().find(|column| column.name == field.name) else {
                return Err(ClickhouseSinkError::ColumnNotFound(field.name.clone()));
            };

            let mut expected_type = map_field_to_type(field);

            if field.nullable {
                expected_type = format!("Nullable({expected_type})");
            }

            if field.typ == FieldType::Binary {
                expected_type = format!("Array({expected_type})");
            }

            let column_type = column.type_.clone();
            if expected_type != column_type {
                return Err(ClickhouseSinkError::ColumnTypeMismatch(
                    field.name.clone(),
                    expected_type.to_string(),
                    column_type.to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn fetch_sink_table_info(
        mut handle: ClientHandle,
        sink_table_name: &str,
    ) -> Result<ClickhouseTable, ClickhouseSinkError> {
        let block = handle
            .query(format!(
                "SELECT database, name, engine, engine_full FROM system.tables WHERE name = '{}'",
                sink_table_name
            ))
            .fetch_all()
            .await?;
        let row = block.rows().next().unwrap();
        Ok(ClickhouseTable {
            database: row.get(0)?,
            name: row.get(1)?,
            engine: row.get(2)?,
            engine_full: row.get(3)?,
        })
    }

    async fn fetch_primary_keys(
        mut handle: ClientHandle,
        sink_table_name: &str,
        schema: &str,
    ) -> Result<Vec<String>, ClickhouseSinkError> {
        let block = handle
            .query(format!(
                r#"
                SELECT column_name, constraint_name, constraint_schema 
                FROM INFORMATION_SCHEMA.key_column_usage 
                WHERE table_name = '{}' AND constraint_schema = '{}' and column_name is NOT NULL"#,
                sink_table_name, schema
            ))
            .fetch_all()
            .await?;

        let mut keys = vec![];
        for r in block.rows() {
            let name: Option<String> = r.get(0)?;
            if let Some(name) = name {
                keys.push(name);
            }
        }

        Ok(keys)
    }
}

pub fn map_field_to_type(field: &FieldDefinition) -> String {
    let typ = match field.typ {
        FieldType::UInt => "UInt64",
        FieldType::U128 => "UInt128",
        FieldType::Int => "Int64",
        FieldType::I128 => "Int128",
        FieldType::Float => "Float64",
        FieldType::Boolean => "Boolean",
        FieldType::String => "String",
        FieldType::Text => "String",
        FieldType::Binary => "Array(UInt8)",
        FieldType::Decimal => "Decimal",
        FieldType::Timestamp => "DateTime64(3)",
        FieldType::Date => "Date",
        FieldType::Json => "JSON",
        FieldType::Point => "Point",
        FieldType::Duration => unimplemented!(),
    };

    if field.nullable {
        if field.typ != FieldType::Binary {
            format!("Nullable({})", typ)
        } else {
            warn!("Binary field cannot be nullable, ignoring nullable flag");
            typ.to_string()
        }
    } else {
        typ.to_string()
    }
}
