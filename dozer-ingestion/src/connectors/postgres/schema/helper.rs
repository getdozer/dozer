use std::collections::HashMap;

use crate::connectors::{CdcType, ListOrFilterColumns, SourceSchema, SourceSchemaResult};
use crate::errors::{ConnectorError, PostgresConnectorError, PostgresSchemaError};
use dozer_types::types::{FieldDefinition, FieldType, Schema, SourceDefinition};

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::helper::postgres_type_to_dozer_type;
use crate::errors::PostgresSchemaError::{InvalidColumnType, ValueConversionError};

use postgres_types::Type;

use crate::connectors::postgres::schema::sorter::sort_schemas;
use tokio_postgres::Row;

use PostgresSchemaError::TableTypeNotFound;

#[derive(Debug)]
pub struct SchemaHelper {
    conn_config: tokio_postgres::Config,
}

struct PostgresTableRow {
    schema: String,
    table_name: String,
    field: FieldDefinition,
    is_column_used_in_index: bool,
    replication_type: String,
}

#[derive(Clone, Debug)]
pub struct PostgresTable {
    fields: Vec<FieldDefinition>,
    // Indexes of fields, which are used for replication identity
    // Default - uses PK for identity
    // Index - uses selected index fields for identity
    // Full - all fields are used for identity
    // Nothing - no fields can be used for identity.
    //  Postgres will not return old values in update and delete replication messages
    index_keys: Vec<bool>,
    replication_type: String,
}

pub(crate) type SchemaTableIdentifier = (String, String);
impl PostgresTable {
    pub fn new(replication_type: String) -> Self {
        Self {
            fields: vec![],
            index_keys: vec![],
            replication_type,
        }
    }

    pub fn add_field(&mut self, field: FieldDefinition, is_column_used_in_index: bool) {
        self.fields.push(field);
        self.index_keys.push(is_column_used_in_index);
    }

    pub fn fields(&self) -> &Vec<FieldDefinition> {
        &self.fields
    }

    pub fn is_index_field(&self, index: usize) -> Option<&bool> {
        self.index_keys.get(index)
    }

    pub fn get_field(&self, index: usize) -> Option<&FieldDefinition> {
        self.fields.get(index)
    }

    pub fn replication_type(&self) -> &String {
        &self.replication_type
    }
}

#[derive(Debug, Clone)]
pub struct PostgresTableInfo {
    pub schema: String,
    pub name: String,
    pub relation_id: u32,
    pub columns: Vec<String>,
}

type RowsWithColumnsMap = (Vec<Row>, HashMap<SchemaTableIdentifier, Vec<String>>);

impl SchemaHelper {
    pub fn new(conn_config: tokio_postgres::Config) -> SchemaHelper {
        Self { conn_config }
    }

    pub async fn get_tables(
        &self,
        tables: Option<&[ListOrFilterColumns]>,
    ) -> Result<Vec<PostgresTableInfo>, ConnectorError> {
        let (results, tables_columns_map) = self.get_columns(tables).await?;

        let mut table_columns_map: HashMap<SchemaTableIdentifier, (u32, Vec<String>)> =
            HashMap::new();
        for row in results {
            let schema: String = row.get(8);
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let relation_id: u32 = row.get(4);

            let schema_table_tuple = (schema, table_name);
            let add_column_table = tables_columns_map
                .get(&schema_table_tuple)
                .map_or(true, |columns| {
                    columns.is_empty() || columns.contains(&column_name)
                });

            if add_column_table {
                match table_columns_map.get_mut(&schema_table_tuple) {
                    Some((existing_relation_id, columns)) => {
                        columns.push(column_name);
                        assert_eq!(*existing_relation_id, relation_id);
                    }
                    None => {
                        table_columns_map
                            .insert(schema_table_tuple, (relation_id, vec![column_name]));
                    }
                }
            }
        }

        Ok(if let Some(tables) = tables {
            let mut result = vec![];
            for table in tables {
                result.push(find_table(
                    &table_columns_map,
                    table.schema.as_deref(),
                    &table.name,
                )?);
            }
            result
        } else {
            table_columns_map
                .into_iter()
                .map(
                    |((schema, name), (relation_id, columns))| PostgresTableInfo {
                        name,
                        relation_id,
                        columns,
                        schema,
                    },
                )
                .collect()
        })
    }

    async fn get_columns(
        &self,
        tables: Option<&[ListOrFilterColumns]>,
    ) -> Result<RowsWithColumnsMap, PostgresConnectorError> {
        let mut tables_columns_map: HashMap<SchemaTableIdentifier, Vec<String>> = HashMap::new();
        let client = helper::connect(self.conn_config.clone()).await?;
        let query = if let Some(tables) = tables {
            tables.iter().for_each(|t| {
                if let Some(columns) = t.columns.clone() {
                    tables_columns_map.insert(
                        (
                            t.schema
                                .as_ref()
                                .map_or(DEFAULT_SCHEMA_NAME.to_string(), |s| s.to_string()),
                            t.name.clone(),
                        ),
                        columns,
                    );
                }
            });

            let schemas: Vec<String> = tables
                .iter()
                .map(|t| {
                    t.schema
                        .as_ref()
                        .map_or_else(|| DEFAULT_SCHEMA_NAME.to_string(), |s| s.clone())
                })
                .collect();
            let table_names: Vec<String> = tables.iter().map(|t| t.name.clone()).collect();
            let sql = str::replace(
                SQL,
                ":tables_name_condition",
                "t.table_schema = ANY($1) AND t.table_name = ANY($2)",
            );
            client.query(&sql, &[&schemas, &table_names]).await
        } else {
            let sql = str::replace(SQL, ":tables_name_condition", "t.table_type = 'BASE TABLE'");
            client.query(&sql, &[]).await
        };

        query
            .map_err(PostgresConnectorError::InvalidQueryError)
            .map(|rows| (rows, tables_columns_map))
    }

    pub async fn get_schemas(
        &self,
        tables: &[ListOrFilterColumns],
    ) -> Result<Vec<SourceSchemaResult>, PostgresConnectorError> {
        let (results, tables_columns_map) = self.get_columns(Some(tables)).await?;

        let mut columns_map: HashMap<SchemaTableIdentifier, PostgresTable> = HashMap::new();
        results
            .iter()
            .filter(|row| {
                let schema: String = row.get(8);
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);

                tables_columns_map
                    .get(&(schema, table_name))
                    .map_or(true, |columns| {
                        columns.is_empty() || columns.contains(&column_name)
                    })
            })
            .map(|r| self.convert_row(r))
            .try_for_each(|table_row| -> Result<(), PostgresSchemaError> {
                let row = table_row?;
                columns_map
                    .entry((row.schema, row.table_name))
                    .and_modify(|table| {
                        table.add_field(row.field.clone(), row.is_column_used_in_index)
                    })
                    .or_insert_with(|| {
                        let mut table = PostgresTable::new(row.replication_type);
                        table.add_field(row.field, row.is_column_used_in_index);
                        table
                    });

                Ok(())
            })?;

        let columns_map = sort_schemas(tables, &columns_map)?;

        Ok(Self::map_columns_to_schemas(columns_map))
    }

    fn map_columns_to_schemas(
        postgres_tables: Vec<(SchemaTableIdentifier, PostgresTable)>,
    ) -> Vec<SourceSchemaResult> {
        postgres_tables
            .into_iter()
            .map(|((_, table_name), table)| {
                Self::map_schema(&table_name, table).map_err(|e| {
                    ConnectorError::PostgresConnectorError(
                        PostgresConnectorError::PostgresSchemaError(e),
                    )
                })
            })
            .collect()
    }

    fn map_schema(
        table_name: &str,
        table: PostgresTable,
    ) -> Result<SourceSchema, PostgresSchemaError> {
        let primary_index: Vec<usize> = table
            .index_keys
            .iter()
            .enumerate()
            .filter(|(_, b)| **b)
            .map(|(idx, _)| idx)
            .collect();

        let schema = Schema {
            fields: table.fields.clone(),
            primary_index,
        };

        let cdc_type = match table.replication_type.as_str() {
            "d" => {
                if schema.primary_index.is_empty() {
                    Ok(CdcType::Nothing)
                } else {
                    Ok(CdcType::OnlyPK)
                }
            }
            "i" => Ok(CdcType::OnlyPK),
            "n" => Ok(CdcType::Nothing),
            "f" => Ok(CdcType::FullChanges),
            typ => Err(PostgresSchemaError::UnsupportedReplicationType(
                typ.to_string(),
            )),
        }?;

        let source_schema = SourceSchema::new(schema, cdc_type);
        Self::validate_schema_replication_identity(table_name, &source_schema)?;

        Ok(source_schema)
    }

    fn validate_schema_replication_identity(
        table_name: &str,
        schema: &SourceSchema,
    ) -> Result<(), PostgresSchemaError> {
        if schema.cdc_type == CdcType::OnlyPK && schema.schema.primary_index.is_empty() {
            Err(PostgresSchemaError::PrimaryKeyIsMissingInSchema(
                table_name.to_string(),
            ))
        } else {
            Ok(())
        }
    }
    fn convert_row(&self, row: &Row) -> Result<PostgresTableRow, PostgresSchemaError> {
        let schema: String = row.get(8);
        let table_name: String = row.get(0);
        let table_type: Option<String> = row.get(7);
        if let Some(typ) = table_type {
            if typ != *"BASE TABLE" {
                return Err(PostgresSchemaError::UnsupportedTableType(typ, table_name));
            }
        } else {
            return Err(TableTypeNotFound);
        }

        let column_name: String = row.get(1);
        let is_nullable: bool = row.get(2);
        let is_column_used_in_index: bool = row.get(3);
        let replication_type_int: i8 = row.get(5);
        let type_oid: u32 = row.get(6);

        // TODO: workaround - in case of custom enum
        let typ;     if type_oid == 28862 {
            typ = FieldType::String;
        } else {
            let oid_typ = Type::from_oid(type_oid);
            typ = oid_typ.map_or_else(
                || Err(InvalidColumnType(column_name.clone())),
                postgres_type_to_dozer_type,
            )?;
        }

        let replication_type = String::from_utf8(vec![replication_type_int as u8])
            .map_err(|_e| ValueConversionError("Replication type".to_string()))?;

        Ok(PostgresTableRow {
            schema,
            table_name,
            field: FieldDefinition::new(column_name, typ, is_nullable, SourceDefinition::Dynamic),
            is_column_used_in_index,
            replication_type,
        })
    }
}

pub const DEFAULT_SCHEMA_NAME: &str = "public";

fn find_table(
    table_columns_map: &HashMap<SchemaTableIdentifier, (u32, Vec<String>)>,
    schema_name: Option<&str>,
    table_name: &str,
) -> Result<PostgresTableInfo, PostgresConnectorError> {
    let schema_name = schema_name.unwrap_or(DEFAULT_SCHEMA_NAME);
    let schema_table_identifier = (schema_name.to_string(), table_name.to_string());
    if let Some((relation_id, columns)) = table_columns_map.get(&schema_table_identifier) {
        Ok(PostgresTableInfo {
            schema: schema_table_identifier.0,
            name: schema_table_identifier.1,
            relation_id: *relation_id,
            columns: columns.clone(),
        })
    } else {
        Err(PostgresConnectorError::TablesNotFound(vec![
            schema_table_identifier,
        ]))
    }
}

const SQL: &str = "
SELECT table_info.table_name,
       table_info.column_name,
       CASE WHEN table_info.is_nullable = 'NO' THEN false ELSE true END AS is_nullable,
       CASE
           WHEN pc.relreplident = 'd' OR pc.relreplident = 'i'
               THEN pa.attrelid IS NOT NULL
           WHEN pc.relreplident = 'n' THEN false
           WHEN pc.relreplident = 'f' THEN true
           ELSE false
           END                                                          AS is_column_used_in_index,
       pc.oid,
       pc.relreplident,
       pt.oid                                                           AS type_oid,
       t.table_type,
       t.table_schema
FROM information_schema.columns table_info
         LEFT JOIN information_schema.tables t ON t.table_name = table_info.table_name AND t.table_schema = table_info.table_schema
         LEFT JOIN pg_namespace ns ON t.table_schema = ns.nspname
         LEFT JOIN pg_class pc ON t.table_name = pc.relname AND ns.oid = pc.relnamespace
         LEFT JOIN pg_type pt ON table_info.udt_name = pt.typname
         LEFT JOIN pg_index pi ON pc.oid = pi.indrelid AND
                                  ((pi.indisreplident = true AND pc.relreplident = 'i') OR (pi.indisprimary AND pc.relreplident = 'd'))
         LEFT JOIN pg_attribute pa ON
             pa.attrelid = pi.indrelid
                 AND pa.attnum = ANY (pi.indkey)
                 AND pa.attnum > 0
                 AND pa.attname = table_info.column_name
WHERE :tables_name_condition AND ns.nspname not in ('information_schema', 'pg_catalog')
      and ns.nspname not like 'pg_toast%'
      and ns.nspname not like 'pg_temp_%'
ORDER BY table_info.table_schema,
         table_info.table_catalog,
         table_info.table_name,
         table_info.ordinal_position;";
