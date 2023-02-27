use std::collections::HashMap;

use crate::errors::{ConnectorError, PostgresConnectorError, PostgresSchemaError};
use dozer_types::types::{
    FieldDefinition, ReplicationChangesTrackingType, Schema, SchemaIdentifier, SourceDefinition,
    SourceSchema,
};

use crate::connectors::{ColumnInfo, TableInfo, ValidationResults};

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::helper::postgres_type_to_dozer_type;
use crate::errors::PostgresSchemaError::{
    InvalidColumnType, PrimaryKeyIsMissingInSchema, ValueConversionError,
};

use postgres_types::Type;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio_postgres::Row;
use PostgresSchemaError::TableTypeNotFound;

#[derive(Debug)]
pub struct SchemaHelper {
    conn_config: tokio_postgres::Config,
    schema: String,
}

pub struct PostgresTableRow {
    pub table_name: String,
    pub column_name: String,
    pub field: FieldDefinition,
    pub is_column_used_in_index: bool,
    pub table_id: u32,
    pub replication_type: String,
}

pub struct PostgresTable {
    fields: Vec<FieldDefinition>,
    primary_keys: Vec<bool>,
    table_id: u32,
    replication_type: String,
}

impl PostgresTable {
    pub fn new(table_id: u32, replication_type: String) -> Self {
        Self {
            fields: vec![],
            primary_keys: vec![],
            table_id,
            replication_type,
        }
    }

    fn add_field(&mut self, field: FieldDefinition, is_column_used_in_index: bool) {
        self.fields.push(field);
        self.primary_keys.push(is_column_used_in_index);
    }
}

type RowsWithColumnsMap = (Vec<Row>, HashMap<String, Vec<String>>);

impl SchemaHelper {
    pub fn new(conn_config: tokio_postgres::Config, schema: Option<String>) -> SchemaHelper {
        let schema = schema.map_or("public".to_string(), |s| s);
        Self {
            conn_config,
            schema,
        }
    }

    pub fn get_tables(
        &self,
        tables: Option<&[TableInfo]>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        let (results, tables_columns_map) = self.get_columns(tables)?;

        let mut columns_map: HashMap<String, Vec<ColumnInfo>> = HashMap::new();
        let mut tables_id: HashMap<String, u32> = HashMap::new();
        for row in results {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);
            let table_id: u32 = if let Some(rel_id) = row.get(4) {
                rel_id
            } else {
                let mut s = DefaultHasher::new();
                table_name.hash(&mut s);
                s.finish() as u32
            };

            let add_column_table = tables_columns_map.get(&table_name).map_or(true, |columns| {
                columns.is_empty() || columns.contains(&column_name)
            });

            if add_column_table {
                let vals = columns_map.get(&table_name);
                let mut columns = vals.map_or_else(Vec::new, |columns| columns.clone());

                columns.push(ColumnInfo {
                    name: column_name,
                    data_type: None,
                });

                columns_map.insert(table_name.clone(), columns);
                tables_id.insert(table_name, table_id);
            }
        }

        Ok(columns_map
            .iter()
            .map(|(table_name, columns)| TableInfo {
                name: table_name.clone(),
                table_name: table_name.clone(),
                id: *tables_id.get(&table_name.clone()).unwrap(),
                columns: Some(columns.clone()),
            })
            .collect())
    }

    fn get_columns(
        &self,
        table_name: Option<&[TableInfo]>,
    ) -> Result<RowsWithColumnsMap, PostgresConnectorError> {
        let mut tables_columns_map: HashMap<String, Vec<String>> = HashMap::new();
        let mut client = helper::connect(self.conn_config.clone())?;
        let schema = self.schema.clone();
        let query = if let Some(tables) = table_name {
            tables.iter().for_each(|t| {
                if let Some(columns) = t.columns.clone() {
                    tables_columns_map.insert(
                        t.table_name.clone(),
                        columns.iter().map(|c| c.name.clone()).collect(),
                    );
                }
            });
            let table_names: Vec<String> = tables.iter().map(|t| t.table_name.clone()).collect();
            let sql = str::replace(SQL, ":tables_condition", "= ANY($1) AND table_schema = $2");
            client.query(&sql, &[&table_names, &schema])
        } else {
            let sql = str::replace(SQL, ":tables_condition", TABLES_CONDITION);
            client.query(&sql, &[&schema])
        };

        query
            .map_err(PostgresConnectorError::InvalidQueryError)
            .map(|rows| (rows, tables_columns_map))
    }

    pub fn get_schemas(
        &self,
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<SourceSchema>, PostgresConnectorError> {
        let (results, tables_columns_map) = self.get_columns(tables.as_deref())?;

        let mut columns_map: HashMap<String, PostgresTable> = HashMap::new();
        results
            .iter()
            .filter(|row| {
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);

                tables_columns_map.get(&table_name).map_or(true, |columns| {
                    columns.is_empty() || columns.contains(&column_name)
                })
            })
            .map(|r| self.convert_row(r))
            .try_for_each(|table_row| -> Result<(), PostgresSchemaError> {
                let row = table_row?;
                columns_map
                    .entry(row.table_name)
                    .and_modify(|table| {
                        table.add_field(row.field.clone(), row.is_column_used_in_index)
                    })
                    .or_insert_with(|| {
                        let mut table = PostgresTable::new(row.table_id, row.replication_type);
                        table.add_field(row.field, row.is_column_used_in_index);
                        table
                    });

                Ok(())
            })?;

        Self::map_columns_to_schemas(columns_map)
            .map_err(PostgresConnectorError::PostgresSchemaError)
    }

    pub fn map_columns_to_schemas(
        map: HashMap<String, PostgresTable>,
    ) -> Result<Vec<SourceSchema>, PostgresSchemaError> {
        let mut schemas: Vec<SourceSchema> = Vec::new();
        for (table_name, table) in map.into_iter() {
            let primary_index: Vec<usize> = table
                .primary_keys
                .iter()
                .enumerate()
                .filter(|(_, b)| **b)
                .map(|(idx, _)| idx)
                .collect();

            let schema = Schema {
                identifier: Some(SchemaIdentifier {
                    id: table.table_id,
                    version: 1,
                }),
                fields: table.fields.clone(),
                primary_index,
            };

            let replication_type = match table.replication_type.as_str() {
                "d" => Ok(ReplicationChangesTrackingType::OnlyPK),
                "i" => Ok(ReplicationChangesTrackingType::OnlyPK),
                "n" => Ok(ReplicationChangesTrackingType::Nothing),
                "f" => Ok(ReplicationChangesTrackingType::FullChanges),
                typ => Err(PostgresSchemaError::UnsupportedReplicationType(
                    typ.to_string(),
                )),
            }?;

            schemas.push(SourceSchema::new(table_name, schema, replication_type));
        }

        Self::validate_schema_replication_identity(&schemas)?;

        Ok(schemas)
    }

    pub fn validate_schema_replication_identity(
        schemas: &[SourceSchema],
    ) -> Result<(), PostgresSchemaError> {
        let table_without_primary_index =
            schemas.iter().find(|s| s.schema.primary_index.is_empty());

        match table_without_primary_index {
            Some(s) => Err(PrimaryKeyIsMissingInSchema(s.name.clone())),
            None => Ok(()),
        }
    }

    pub fn validate(
        &self,
        tables: &[TableInfo],
    ) -> Result<ValidationResults, PostgresConnectorError> {
        let (results, tables_columns_map) = self.get_columns(Some(tables))?;

        let mut validation_result: ValidationResults = HashMap::new();
        for row in results {
            let table_name: String = row.get(0);
            let column_name: String = row.get(1);

            let column_should_be_validated = tables_columns_map
                .get(&table_name)
                .map_or(true, |table_info| table_info.contains(&column_name));

            if column_should_be_validated {
                let row_result = self.convert_row(&row).map_or_else(
                    |e| {
                        Err(ConnectorError::PostgresConnectorError(
                            PostgresConnectorError::PostgresSchemaError(e),
                        ))
                    },
                    |_| Ok(()),
                );

                validation_result.entry(table_name.clone()).or_default();
                validation_result
                    .entry(table_name)
                    .and_modify(|r| r.push((Some(column_name), row_result)));
            }
        }

        for table in tables {
            if let Some(columns) = &table.columns {
                let mut existing_columns = HashMap::new();
                if let Some(res) = validation_result.get(&table.table_name) {
                    for (col_name, _) in res {
                        if let Some(name) = col_name {
                            existing_columns.insert(name.clone(), ());
                        }
                    }
                }

                for ColumnInfo { name, .. } in columns {
                    if existing_columns.get(name).is_none() {
                        validation_result
                            .entry(table.table_name.clone())
                            .and_modify(|r| {
                                r.push((
                                    None,
                                    Err(ConnectorError::PostgresConnectorError(
                                        PostgresConnectorError::ColumnNotFound(
                                            name.to_string(),
                                            table.table_name.clone(),
                                        ),
                                    )),
                                ))
                            })
                            .or_default();
                    }
                }
            }
        }

        Ok(validation_result)
    }

    fn convert_row(&self, row: &Row) -> Result<PostgresTableRow, PostgresSchemaError> {
        let table_type: Option<String> = row.get(7);
        if let Some(typ) = table_type {
            if typ != *"BASE TABLE" {
                return Err(PostgresSchemaError::UnsupportedTableType(typ));
            }
        } else {
            return Err(TableTypeNotFound);
        }

        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let is_nullable: bool = row.get(2);
        let is_column_used_in_index: bool = row.get(3);
        let table_id: u32 = if let Some(rel_id) = row.get(4) {
            rel_id
        } else {
            let mut s = DefaultHasher::new();
            table_name.hash(&mut s);
            s.finish() as u32
        };
        let replication_type_int: i8 = row.get(5);
        let type_oid: u32 = row.get(6);
        let typ = Type::from_oid(type_oid);

        let typ = typ.map_or(Err(InvalidColumnType), postgres_type_to_dozer_type)?;

        let replication_type = String::from_utf8(vec![replication_type_int as u8])
            .map_err(|_e| ValueConversionError("Replication type".to_string()))?;

        Ok(PostgresTableRow {
            table_name,
            column_name: column_name.clone(),
            field: FieldDefinition::new(column_name, typ, is_nullable, SourceDefinition::Dynamic),
            is_column_used_in_index,
            table_id,
            replication_type,
        })
    }
}

const TABLES_CONDITION: &str = "IN (SELECT table_name
                           FROM information_schema.tables
                           WHERE table_schema = $1
                           ORDER BY table_name)";

const SQL: &str = "
SELECT table_info.table_name,
       table_info.column_name,
       CASE WHEN table_info.is_nullable = 'NO' THEN false ELSE true END AS is_nullable,
       CASE
           WHEN pc.relreplident = 'd' THEN constraint_info.constraint_type IS NOT NULL
           WHEN pc.relreplident = 'i' THEN pa.attname IS NOT NULL
           WHEN pc.relreplident = 'n' THEN false
           WHEN pc.relreplident = 'f' THEN true
           ELSE false
           END                                                          AS is_column_used_in_index,
       st_user_table.relid,
       pc.relreplident,
       pt.oid                                                           AS type_oid,
       t.table_type
FROM (SELECT table_schema,
             table_catalog,
             table_name,
             column_name,
             is_nullable,
             data_type,
             numeric_precision,
             udt_name,
             character_maximum_length
      FROM information_schema.columns
      WHERE table_name :tables_condition
      ORDER BY table_name) table_info
         LEFT JOIN information_schema.tables t ON t.table_name = table_info.table_name
         LEFT JOIN pg_catalog.pg_statio_user_tables st_user_table ON st_user_table.relname = table_info.table_name
         LEFT JOIN (SELECT constraintUsage.table_name,
                           constraintUsage.column_name,
                           table_constraints.constraint_name,
                           table_constraints.constraint_type
                    FROM information_schema.constraint_column_usage constraintUsage
                             JOIN information_schema.table_constraints table_constraints
                                  ON constraintUsage.table_name = table_constraints.table_name
                                      AND constraintUsage.constraint_name = table_constraints.constraint_name
                                      AND table_constraints.constraint_type = 'PRIMARY KEY') constraint_info
                   ON table_info.table_name = constraint_info.table_name
                       AND table_info.column_name = constraint_info.column_name
         LEFT JOIN pg_class pc ON st_user_table.relid = pc.oid
         LEFT JOIN pg_type pt ON table_info.udt_name = pt.typname
         LEFT JOIN pg_index pi ON st_user_table.relid = pi.indrelid AND pi.indisreplident = true
         LEFT JOIN pg_attribute pa ON pa.attrelid = pi.indrelid AND pa.attnum = ANY (pi.indkey) AND pa.attnum > 0 AND
                                      pa.attname = table_info.column_name
ORDER BY table_info.table_schema,
         table_info.table_catalog,
         table_info.table_name;";
