use super::{
    connection::{Conn, QueryResult},
    conversion::get_field_type_for_mysql_column_type,
};
use crate::{
    connectors::{mysql::helpers::escape_identifier, TableIdentifier, TableInfo},
    errors::MySQLConnectorError,
};
use dozer_types::types::FieldType;
use mysql_async::{from_row, Pool};
use mysql_common::Value;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableDefinition {
    pub table_index: usize,
    pub table_name: String,
    pub database_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnDefinition {
    pub ordinal_position: u32,
    pub name: String,
    pub typ: FieldType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Clone, Debug)]
pub struct SchemaHelper<'a, 'b> {
    conn_url: &'a String,
    conn_pool: &'b Pool,
}

impl SchemaHelper<'_, '_> {
    pub fn new<'a, 'b>(conn_url: &'a String, conn_pool: &'b Pool) -> SchemaHelper<'a, 'b> {
        SchemaHelper {
            conn_url,
            conn_pool,
        }
    }

    pub async fn list_tables(&self) -> Result<Vec<TableIdentifier>, MySQLConnectorError> {
        let mut conn = self.connect().await?;

        let mut rows = conn.exec_iter(
            "
                    SELECT table_name, table_schema FROM information_schema.tables
                    WHERE table_schema = DATABASE()
                    AND table_type = 'BASE TABLE'
                "
            .into(),
            vec![],
        );

        let tables = rows
            .map(|row| {
                let (table_name, table_schema) = from_row(row);

                TableIdentifier {
                    schema: Some(table_schema),
                    name: table_name,
                }
            })
            .await
            .map_err(MySQLConnectorError::QueryResultError)?;

        Ok(tables)
    }

    pub async fn list_columns(
        &self,
        tables: Vec<TableIdentifier>,
    ) -> Result<Vec<TableInfo>, MySQLConnectorError> {
        if tables.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.connect().await?;
        let mut rows = Self::query_information_schema_columns(
            &mut conn,
            &tables,
            &["table_name", "table_schema", "column_name"],
            &[],
        )
        .await?;

        let table_infos = rows
            .reduce(
                Vec::new(),
                |mut table_infos, row: (String, String, String)| {
                    let (table_name, table_schema, column_name) = row;

                    let last_table = table_infos.last_mut();
                    match last_table {
                        Some(TableInfo {
                            name,
                            schema: Some(schema),
                            ..
                        }) if *name == table_name && *schema == table_schema => {
                            last_table.unwrap().column_names.push(column_name);
                        }
                        _ => table_infos.push(TableInfo {
                            schema: Some(table_schema),
                            name: table_name,
                            column_names: vec![column_name],
                        }),
                    }

                    table_infos
                },
            )
            .await
            .map_err(MySQLConnectorError::QueryResultError)?;

        Ok(table_infos)
    }

    pub async fn get_table_definitions(
        &self,
        table_infos: &[TableInfo],
    ) -> Result<Vec<TableDefinition>, MySQLConnectorError> {
        if table_infos.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.connect().await?;
        let mut rows = Self::query_information_schema_columns(
            &mut conn,
            table_infos,
            &[
                "table_name",
                "table_schema",
                "column_name",
                "column_type",
                "is_nullable",
                "column_key",
                "ordinal_position",
            ],
            &[Self::MARIADB_JSON_CHECK],
        )
        .await?;

        let mut table_definitions: Vec<TableDefinition> = Vec::new();
        {
            while let Some(result) = rows.next().await {
                let row = result.map_err(MySQLConnectorError::QueryResultError)?;
                let (
                    table_name,
                    table_schema,
                    column_name,
                    column_type,
                    is_nullable,
                    column_key,
                    ordinal_position,
                    is_mariadb_json,
                ) = from_row::<(String, String, String, String, String, String, u32, bool)>(row);

                let nullable = is_nullable == "YES";
                let primary_key = column_key == "PRI";
                let column_definiton = ColumnDefinition {
                    ordinal_position,
                    name: column_name.clone(),
                    typ: if is_mariadb_json {
                        FieldType::Json
                    } else {
                        get_field_type_for_mysql_column_type(&column_type)?
                    },
                    nullable,
                    primary_key,
                };

                match table_definitions.last_mut() {
                    Some(td) if td.table_name == table_name && td.database_name == table_schema => {
                        td.columns.push(column_definiton);
                    }
                    _ => {
                        let table_index = table_definitions.len();
                        let td = TableDefinition {
                            table_index,
                            table_name,
                            database_name: table_schema,
                            columns: vec![column_definiton],
                        };
                        table_definitions.push(td);
                    }
                }
            }
        }

        Ok(table_definitions)
    }

    const MARIADB_JSON_CHECK: &'static str = "(column_type = 'longtext'
        AND (
            SELECT COUNT(*) > 0
            FROM information_schema.check_constraints
            WHERE (
                constraint_schema = table_schema
                AND table_name = table_name
                AND constraint_name = column_name
                AND check_clause LIKE CONCAT('%json_valid(`', column_name, '`)%')
            )
        )
    ) as is_mariadb_json";

    async fn query_information_schema_columns<'a, 'b, T>(
        conn: &'b mut Conn,
        table_infos: &'a [T],
        select_columns: &[&str],
        additional_select_expressions: &[&str],
    ) -> Result<QueryResult, MySQLConnectorError>
    where
        TableInfoRef<'a>: From<&'a T>,
    {
        let mut query_params: Vec<Value> = Vec::new();
        let query = format!(
            "
                SELECT {}
                FROM information_schema.columns
                WHERE {}
                ORDER BY {}, ordinal_position ASC
            ",
            {
                let mut select = select_columns
                    .iter()
                    .copied()
                    .map(escape_identifier)
                    .collect::<Vec<_>>()
                    .join(", ");

                if !additional_select_expressions.is_empty() {
                    select.push_str(", ");
                    select.push_str(additional_select_expressions.join(", ").as_str());
                }

                select
            },
            // where clause: filter by table name and table schema
            table_infos
                .iter()
                .map(Into::into)
                .map(
                    |TableInfoRef {
                         schema,
                         name,
                         column_names,
                     }| {
                        query_params.push(name.into());
                        let mut condition = "(table_name = ? AND table_schema = ".to_string();
                        if let Some(schema) = schema {
                            query_params.push(schema.into());
                            condition.push('?');
                        } else {
                            condition.push_str("DATABASE()");
                        }
                        if !column_names.is_empty() {
                            condition.push_str(" AND column_name IN (");
                            for (i, column) in column_names.iter().enumerate() {
                                query_params.push(column.into());
                                if i == 0 {
                                    condition.push('?');
                                } else {
                                    condition.push_str(", ?");
                                }
                            }
                            condition.push(')');
                        }
                        condition.push(')');
                        condition
                    }
                )
                .collect::<Vec<String>>()
                .join(" OR "),
            // order by clause: preserve the order of the input tables in the output
            {
                let mut case = String::from("CASE ");

                table_infos.iter().map(Into::into).enumerate().for_each(
                    |(i, TableInfoRef { schema, name, .. })| {
                        case.push_str("WHEN (table_name = ? AND table_schema = ");
                        query_params.push(name.into());

                        if let Some(schema) = schema {
                            query_params.push(schema.into());
                            case.push('?');
                        } else {
                            case.push_str("DATABASE()");
                        }
                        case.push(')');
                        case.push_str(" THEN ? ");
                        query_params.push(i.into());
                    },
                );

                case.push_str("END ASC");
                case
            }
        );

        let rows = conn.exec_iter(query, query_params);

        Ok(rows)
    }

    async fn connect(&self) -> Result<Conn, MySQLConnectorError> {
        Conn::new(self.conn_pool.clone())
            .await
            .map_err(|err| MySQLConnectorError::ConnectionFailure(self.conn_url.clone(), err))
    }
}

struct TableInfoRef<'a> {
    pub schema: Option<&'a str>,
    pub name: &'a str,
    pub column_names: &'a [String],
}

impl<'a> From<&'a TableIdentifier> for TableInfoRef<'a> {
    fn from(value: &'a TableIdentifier) -> Self {
        Self {
            schema: value.schema.as_deref(),
            name: &value.name,
            column_names: &[],
        }
    }
}

impl<'a> From<&'a TableInfo> for TableInfoRef<'a> {
    fn from(value: &'a TableInfo) -> Self {
        Self {
            schema: value.schema.as_deref(),
            name: &value.name,
            column_names: &value.column_names,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnDefinition, SchemaHelper, TableDefinition};
    use crate::connectors::{
        mysql::tests::{create_test_table, mariadb_test_config, mysql_test_config, TestConfig},
        TableIdentifier, TableInfo,
    };
    use dozer_types::types::FieldType;
    use serial_test::serial;

    async fn test_connector_schemas(config: TestConfig) {
        // setup
        let url = &config.url;
        let pool = &config.pool;

        let schema_helper = SchemaHelper::new(url, pool);

        let _ = create_test_table("test1", &config).await;

        // test
        let tables = schema_helper.list_tables().await.unwrap();
        let expected_table = TableIdentifier {
            name: "test1".into(),
            schema: Some("test".into()),
        };
        assert!(
            tables.contains(&expected_table),
            "Missing test table. Existing tables list is {tables:?}"
        );

        let columns = schema_helper
            .list_columns(vec![expected_table])
            .await
            .unwrap();
        assert_eq!(
            columns,
            vec![TableInfo {
                schema: Some("test".into()),
                name: "test1".into(),
                column_names: vec!["c1".into(), "c2".into(), "c3".into()]
            }]
        );

        let schemas = schema_helper.get_table_definitions(&columns).await.unwrap();
        assert_eq!(
            schemas,
            vec![TableDefinition {
                table_index: 0,
                table_name: "test1".into(),
                database_name: "test".into(),
                columns: vec![
                    ColumnDefinition {
                        ordinal_position: 1,
                        name: "c1".into(),
                        typ: FieldType::Int,
                        nullable: false,
                        primary_key: true,
                    },
                    ColumnDefinition {
                        ordinal_position: 2,
                        name: "c2".into(),
                        typ: FieldType::Text,
                        nullable: true,
                        primary_key: false,
                    },
                    ColumnDefinition {
                        ordinal_position: 3,
                        name: "c3".into(),
                        typ: FieldType::Float,
                        nullable: true,
                        primary_key: false,
                    },
                ]
            }]
        );
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_schemas_mysql() {
        test_connector_schemas(mysql_test_config()).await;
    }

    #[tokio::test]
    #[ignore]
    #[serial]
    async fn test_connector_schemas_mariadb() {
        test_connector_schemas(mariadb_test_config()).await;
    }
}
