use crate::{helpers::escape_identifier, BreakingSchemaChange, MySQLConnectorError};

use super::{
    connection::{Conn, QueryResult},
    conversion::get_field_type_for_mysql_column_type,
};
use dozer_ingestion_connector::{dozer_types::types::FieldType, TableIdentifier, TableInfo};
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

#[derive(Debug, Clone, Copy)]
pub struct SchemaHelper<'a> {
    conn_url: &'a String,
    conn_pool: &'a Pool,
}

impl TableDefinition {
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.database_name, self.table_name)
    }
}

impl std::fmt::Display for TableDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.qualified_name().as_str())
    }
}

impl std::fmt::Display for ColumnDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name.as_str())
    }
}

impl SchemaHelper<'_> {
    pub fn new<'a>(conn_url: &'a String, conn_pool: &'a Pool) -> SchemaHelper<'a> {
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

    pub async fn get_table_definitions<'a, T>(
        &self,
        table_infos: &'a [T],
    ) -> Result<Vec<TableDefinition>, MySQLConnectorError>
    where
        TableInfoRef<'a>: From<&'a T>,
    {
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

    pub async fn refresh_column_ordinals(
        &self,
        tables: &mut [&mut TableDefinition],
    ) -> Result<(), MySQLConnectorError> {
        if tables.is_empty() {
            return Ok(());
        }

        let mut conn = self.connect().await?;

        let mut query_params: Vec<Value> = Vec::new();
        let query = format!(
            "
            SELECT {} AS table_index, {} as column_index, ordinal_position
            FROM information_schema.columns
            WHERE {}
            ",
            SqlHelper::table_index_case_expression(tables, &mut query_params),
            SqlHelper::column_index_case_expression(tables, &mut query_params),
            SqlHelper::select_columns_filter_predicate(tables, &mut query_params),
        );

        let mut rows = conn.exec_iter(query, query_params);

        while let Some(result) = rows.next().await {
            let row = result.map_err(MySQLConnectorError::QueryResultError)?;
            let (table_index, column_index, ordinal_position) =
                from_row::<(usize, usize, u32)>(row);

            let table = &mut tables[table_index];
            let column = &mut table.columns[column_index];
            column.ordinal_position = ordinal_position;
        }

        tables.iter_mut().for_each(|table| {
            table
                .columns
                .sort_unstable_by_key(|column| column.ordinal_position)
        });

        Ok(())
    }

    pub async fn refresh_schema_and_check_for_breaking_changes(
        &self,
        tables: &mut [TableDefinition],
    ) -> Result<(), MySQLConnectorError> {
        let new_schema = self.get_table_definitions(tables).await?;

        // Check missing tables
        if new_schema.len() != tables.len() {
            let missing = tables
                .iter()
                .filter(
                    |TableDefinition {
                         table_name,
                         database_name,
                         ..
                     }| {
                        !new_schema.iter().any(|td| {
                            td.table_name.eq(table_name) && td.database_name.eq(database_name)
                        })
                    },
                )
                .collect::<Vec<_>>();
            if missing.len() == 1 {
                Err(BreakingSchemaChange::TableDroppedOrRenamed(
                    missing[0].to_string(),
                ))?
            } else {
                Err(BreakingSchemaChange::MultipleTablesDroppedOrRenamed(
                    missing.iter().map(|td| td.to_string()).collect::<Vec<_>>(),
                ))?
            }
        }

        // Check missing columns and data type changes
        for (old, new) in tables.iter_mut().zip(new_schema) {
            debug_assert_eq!(old.table_index, new.table_index);
            debug_assert_eq!(old.table_name, new.table_name);
            debug_assert_eq!(old.database_name, new.database_name);

            // Check missing columns
            if old.columns.len() != new.columns.len() {
                let missing = old
                    .columns
                    .iter()
                    .filter(
                        |ColumnDefinition {
                             name: column_name, ..
                         }| {
                            !new.columns.iter().any(|cd| cd.name.eq(column_name))
                        },
                    )
                    .collect::<Vec<_>>();
                if missing.len() == 1 {
                    Err(BreakingSchemaChange::ColumnDroppedOrRenamed {
                        column_name: missing[0].to_string(),
                        table_name: old.to_string(),
                    })?
                } else {
                    Err(BreakingSchemaChange::MultipleColumnsDroppedOrRenamed {
                        table_name: old.to_string(),
                        columns: missing.iter().map(|cd| cd.to_string()).collect::<Vec<_>>(),
                    })?
                }
            }

            // Check data type change
            for old_column in old.columns.iter() {
                let new_column = new
                    .columns
                    .iter()
                    .find(|cd| cd.name == old_column.name)
                    .unwrap();

                if old_column.typ != new_column.typ {
                    Err(BreakingSchemaChange::ColumnDataTypeChanged {
                        table_name: old.to_string(),
                        column_name: old_column.to_string(),
                        old_data_type: old_column.typ,
                        new_column_name: new_column.typ,
                    })?
                }
            }

            // TODO: check nullable and primary key change

            // Checks passed; update schema
            *old = new;
        }

        Ok(())
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
            // where clause: filter by table name and table schema and column names
            SqlHelper::select_columns_filter_predicate(table_infos, &mut query_params),
            // order by clause: preserve the order of the input tables in the output
            {
                let table_index_expression =
                    SqlHelper::table_index_case_expression(table_infos, &mut query_params);
                let order_by = format!("{} ASC", table_index_expression);
                order_by
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

struct SqlHelper;

impl SqlHelper {
    fn table_index_case_expression<'a, T>(tables: &'a [T], query_params: &mut Vec<Value>) -> String
    where
        TableInfoRef<'a>: From<&'a T>,
    {
        let mut case = String::from("CASE ");

        tables.iter().map(Into::into).enumerate().for_each(
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

        case.push_str("END");
        case
    }

    fn column_index_case_expression<'a, T>(tables: &'a [T], query_params: &mut Vec<Value>) -> String
    where
        TableInfoRef<'a>: From<&'a T>,
    {
        let mut case = String::from("CASE ");

        tables.iter().map(Into::into).for_each(
            |TableInfoRef {
                 column_names,
                 schema,
                 name: table_name,
             }| {
                column_names
                    .iter()
                    .enumerate()
                    .for_each(|(i, &column_name)| {
                        case.push_str("WHEN (table_name = ? AND table_schema = ");
                        query_params.push(table_name.into());

                        if let Some(schema) = schema {
                            query_params.push(schema.into());
                            case.push('?');
                        } else {
                            case.push_str("DATABASE()");
                        }
                        case.push_str(" AND column_name = ?");
                        query_params.push(column_name.into());
                        case.push(')');

                        case.push_str(" THEN ? ");
                        query_params.push(i.into());
                    })
            },
        );

        case.push_str("END");
        case
    }

    fn select_columns_filter_predicate<'a, T>(
        tables: &'a [T],
        query_params: &mut Vec<Value>,
    ) -> String
    where
        TableInfoRef<'a>: From<&'a T>,
    {
        let predicate = tables
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
                },
            )
            .collect::<Vec<String>>()
            .join(" OR ");
        predicate
    }
}

pub struct TableInfoRef<'a> {
    pub schema: Option<&'a str>,
    pub name: &'a str,
    pub column_names: Vec<&'a String>,
}

impl<'a> From<&'a TableIdentifier> for TableInfoRef<'a> {
    fn from(value: &'a TableIdentifier) -> Self {
        Self {
            schema: value.schema.as_deref(),
            name: &value.name,
            column_names: vec![],
        }
    }
}

impl<'a> From<&'a TableInfo> for TableInfoRef<'a> {
    fn from(value: &'a TableInfo) -> Self {
        Self {
            schema: value.schema.as_deref(),
            name: &value.name,
            column_names: value.column_names.iter().collect(),
        }
    }
}

impl<'a> From<&'a TableDefinition> for TableInfoRef<'a> {
    fn from(value: &'a TableDefinition) -> Self {
        Self {
            schema: Some(value.database_name.as_str()),
            name: &value.table_name,
            column_names: value.columns.iter().map(|cd| &cd.name).collect(),
        }
    }
}

impl<'a> From<&'a &mut TableDefinition> for TableInfoRef<'a> {
    fn from(value: &'a &mut TableDefinition) -> Self {
        Self {
            schema: Some(value.database_name.as_str()),
            name: &value.table_name,
            column_names: value.columns.iter().map(|cd| &cd.name).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnDefinition, SchemaHelper, TableDefinition};
    use crate::tests::{create_test_table, mariadb_test_config, mysql_test_config, TestConfig};
    use dozer_ingestion_connector::{
        dozer_types::types::FieldType, tokio, TableIdentifier, TableInfo,
    };
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
