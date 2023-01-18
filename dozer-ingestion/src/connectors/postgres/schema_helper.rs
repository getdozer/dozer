use std::collections::HashMap;

use crate::errors::{ConnectorError, PostgresConnectorError, PostgresSchemaError};
use dozer_types::types::{
    FieldDefinition, ReplicationChangesTrackingType, Schema, SchemaIdentifier,
    SchemaWithChangesType,
};

use crate::connectors::{TableInfo, ValidationResults};

use crate::connectors::postgres::connection::helper;
use crate::connectors::postgres::helper::postgres_type_to_dozer_type;
use crate::errors::PostgresSchemaError::{
    InvalidColumnType, SchemaReplicationIdentityError, ValueConversionError,
};

use postgres_types::Type;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tokio_postgres::Row;

pub struct SchemaHelper {
    conn_config: tokio_postgres::Config,
    schema: String,
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
        tables: Option<Vec<TableInfo>>,
    ) -> Result<Vec<TableInfo>, ConnectorError> {
        Ok(self
            .get_schemas(tables)?
            .iter()
            .map(|(name, schema, _)| {
                let columns = Some(schema.fields.iter().map(|f| f.name.clone()).collect());
                TableInfo {
                    name: name.clone(),
                    table_name: name.clone(),
                    id: schema.identifier.unwrap().id,
                    columns,
                }
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
                    tables_columns_map.insert(t.table_name.clone(), columns);
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
    ) -> Result<Vec<SchemaWithChangesType>, PostgresConnectorError> {
        let (results, tables_columns_map) = self.get_columns(tables.as_deref())?;

        let mut columns_map: HashMap<String, (Vec<FieldDefinition>, Vec<bool>, u32, String)> =
            HashMap::new();
        results
            .iter()
            .filter(|row| {
                let table_name: String = row.get(0);
                let column_name: String = row.get(1);

                tables_columns_map
                    .get(&table_name)
                    .map_or(true, |table_info| table_info.contains(&column_name))
            })
            .map(|r| self.convert_row(r))
            .try_for_each(|row| -> Result<(), PostgresSchemaError> {
                let (table_name, field_def, is_primary_key, table_id, replication_type) = row?;
                let vals = columns_map.get(&table_name);
                let (mut fields, mut primary_keys, table_id, replication_type) = match vals {
                    Some((fields, primary_keys, table_id, replication_type)) => (
                        fields.clone(),
                        primary_keys.clone(),
                        *table_id,
                        replication_type,
                    ),
                    None => (vec![], vec![], table_id, &replication_type),
                };

                fields.push(field_def);
                primary_keys.push(is_primary_key);
                columns_map.insert(
                    table_name,
                    (fields, primary_keys, table_id, replication_type.clone()),
                );

                Ok(())
            })?;

        Self::map_columns_to_schemas(columns_map)
            .map_err(PostgresConnectorError::PostgresSchemaError)
    }

    pub fn map_columns_to_schemas(
        map: HashMap<String, (Vec<FieldDefinition>, Vec<bool>, u32, String)>,
    ) -> Result<Vec<SchemaWithChangesType>, PostgresSchemaError> {
        let mut schemas: Vec<SchemaWithChangesType> = Vec::new();
        for (table_name, (fields, primary_keys, table_id, replication_type)) in map.into_iter() {
            let primary_index: Vec<usize> = primary_keys
                .iter()
                .enumerate()
                .filter(|(_, b)| **b)
                .map(|(idx, _)| idx)
                .collect();

            let schema = Schema {
                identifier: Some(SchemaIdentifier {
                    id: table_id,
                    version: 1,
                }),
                fields: fields.clone(),
                primary_index,
            };

            let replication_type = match replication_type.as_str() {
                "d" => Ok(ReplicationChangesTrackingType::OnlyPK),
                "i" => Ok(ReplicationChangesTrackingType::OnlyPK),
                "n" => Ok(ReplicationChangesTrackingType::Nothing),
                "f" => Ok(ReplicationChangesTrackingType::FullChanges),
                _ => Err(PostgresSchemaError::UnsupportedReplicationType(
                    replication_type,
                )),
            }?;

            schemas.push((table_name, schema, replication_type));
        }

        Self::validate_schema_replication_identity(&schemas)?;

        Ok(schemas)
    }

    pub fn validate_schema_replication_identity(
        schemas: &[SchemaWithChangesType],
    ) -> Result<(), PostgresSchemaError> {
        let table_without_primary_index = schemas
            .iter()
            .find(|(_table_name, schema, _)| schema.primary_index.is_empty());

        match table_without_primary_index {
            Some((table_name, _, _)) => Err(SchemaReplicationIdentityError(table_name.clone())),
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

                for column_name in columns {
                    if existing_columns.get(column_name).is_none() {
                        validation_result
                            .entry(table.table_name.clone())
                            .and_modify(|r| {
                                r.push((
                                    None,
                                    Err(ConnectorError::PostgresConnectorError(
                                        PostgresConnectorError::ColumnNotFound(
                                            column_name.to_string(),
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

    fn convert_row(
        &self,
        row: &Row,
    ) -> Result<(String, FieldDefinition, bool, u32, String), PostgresSchemaError> {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let is_nullable: bool = row.get(2);
        let is_primary_index: bool = row.get(3);
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
        Ok((
            table_name,
            FieldDefinition::new(column_name, typ, is_nullable),
            is_primary_index,
            table_id,
            replication_type,
        ))
    }
}

const TABLES_CONDITION: &str = "IN (SELECT table_name
                           FROM information_schema.tables
                           WHERE table_schema = $1 AND table_type = 'BASE TABLE'
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
           END                                                          AS is_primary_index,
       st_user_table.relid,
       pc.relreplident,
       pt.oid                                                           AS type_oid
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

#[cfg(test)]
mod tests {
    use crate::connectors::postgres::schema_helper::SchemaHelper;
    use crate::connectors::postgres::test_utils::get_client;
    use crate::connectors::TableInfo;
    use rand::Rng;
    use std::collections::HashSet;
    use std::hash::Hash;

    fn assert_vec_eq<T>(a: Vec<T>, b: Vec<T>) -> bool
    where
        T: Eq + Hash,
    {
        let a: HashSet<_> = a.iter().collect();
        let b: HashSet<_> = b.iter().collect();

        a == b
    }

    #[test]
    #[ignore]
    // fn connector_e2e_get_tables() {
    fn connector_disabled_test_e2e_get_tables() {
        let mut client = get_client();

        let mut rng = rand::thread_rng();

        let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
        let table_name = format!("products_test_{}", rng.gen::<u32>());

        client.create_schema(&schema);
        client.create_simple_table(&schema, &table_name);

        let schema_helper = SchemaHelper::new(client.postgres_config.clone(), Some(schema.clone()));
        let result = schema_helper.get_tables(None).unwrap();

        let table = result.get(0).unwrap();
        assert_eq!(table_name, table.table_name.clone());
        assert!(assert_vec_eq(
            vec![
                "name".to_string(),
                "description".to_string(),
                "weight".to_string(),
                "id".to_string()
            ],
            table.columns.clone().unwrap()
        ));

        client.drop_schema(&schema);
    }

    #[test]
    #[ignore]
    // fn connector_e2e_get_schema_with_selected_columns() {
    fn connector_disabled_test_e2e_get_schema_with_selected_columns() {
        let mut client = get_client();

        let mut rng = rand::thread_rng();

        let schema = format!("schema_helper_test_{}", rng.gen::<u32>());
        let table_name = format!("products_test_{}", rng.gen::<u32>());

        client.create_schema(&schema);
        client.create_simple_table(&schema, &table_name);

        let schema_helper = SchemaHelper::new(client.postgres_config.clone(), Some(schema.clone()));
        let table_info = TableInfo {
            name: table_name.clone(),
            table_name: table_name.clone(),
            id: 0,
            columns: Some(vec!["name".to_string(), "id".to_string()]),
        };
        let result = schema_helper.get_tables(Some(vec![table_info])).unwrap();

        let table = result.get(0).unwrap();
        assert_eq!(table_name, table.table_name.clone());
        assert!(assert_vec_eq(
            vec!["name".to_string(), "id".to_string()],
            table.columns.clone().unwrap()
        ));

        client.drop_schema(&schema);
    }
}
