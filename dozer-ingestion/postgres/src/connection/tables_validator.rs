use std::collections::hash_map::Entry;
use std::collections::HashMap;

use dozer_ingestion_connector::utils::ListOrFilterColumns;

use crate::schema::helper::DEFAULT_SCHEMA_NAME;
use crate::{PostgresConnectorError, PostgresSchemaError};

use super::client::Client;

pub struct TablesValidator<'a> {
    tables: HashMap<(String, String), &'a ListOrFilterColumns>,
    tables_identifiers: Vec<String>,
}

type PostgresTableIdentifier = (String, String);
type PostgresTablesColumns = HashMap<PostgresTableIdentifier, Vec<String>>;
type PostgresTablesWithTypes = HashMap<PostgresTableIdentifier, Option<String>>;

impl<'a> TablesValidator<'a> {
    pub fn new(table_info: &'a [ListOrFilterColumns]) -> Self {
        let mut tables = HashMap::new();
        let mut tables_identifiers = vec![];
        table_info.iter().for_each(|t| {
            let schema = t
                .schema
                .as_ref()
                .map_or(DEFAULT_SCHEMA_NAME.to_string(), |s| s.clone());
            tables.insert((schema.clone(), t.name.clone()), t);
            tables_identifiers.push(format!("{schema}.{}", t.name.clone()))
        });

        Self {
            tables,
            tables_identifiers,
        }
    }

    async fn fetch_tables(
        &self,
        client: &mut Client,
    ) -> Result<HashMap<PostgresTableIdentifier, Option<String>>, PostgresConnectorError> {
        let result = client
            .query(
                "SELECT table_schema, table_name, table_type \
            FROM information_schema.tables \
            WHERE CONCAT(table_schema, '.', table_name) = ANY($1)",
                &[&self.tables_identifiers],
            )
            .await
            .map_err(PostgresConnectorError::InvalidQueryError)?;

        let mut tables = HashMap::new();
        for r in result.iter() {
            let schema_name: String = r
                .try_get(0)
                .map_err(PostgresConnectorError::InvalidQueryError)?;
            let table_name: String = r
                .try_get(1)
                .map_err(PostgresConnectorError::InvalidQueryError)?;
            let table_type: Option<String> = r
                .try_get(2)
                .map_err(PostgresConnectorError::InvalidQueryError)?;

            tables.insert((schema_name, table_name), table_type);
        }

        Ok(tables)
    }

    async fn fetch_columns(
        &self,
        client: &mut Client,
    ) -> Result<PostgresTablesColumns, PostgresConnectorError> {
        let tables_columns = client
            .query(
                "SELECT table_schema, table_name, column_name \
            FROM information_schema.columns \
            WHERE CONCAT(table_schema, '.', table_name) = ANY($1)",
                &[&self.tables_identifiers],
            )
            .await
            .map_err(PostgresConnectorError::InvalidQueryError)?;

        let mut table_columns_map: HashMap<PostgresTableIdentifier, Vec<String>> = HashMap::new();
        tables_columns.iter().for_each(|r| {
            let schema_name: String = r.try_get(0).unwrap();
            let tbl_name: String = r.try_get(1).unwrap();
            let col_name: String = r.try_get(2).unwrap();

            if let Entry::Vacant(e) =
                table_columns_map.entry((schema_name.clone(), tbl_name.clone()))
            {
                let cols = vec![col_name];
                e.insert(cols);
            } else {
                let cols = table_columns_map.get_mut(&(schema_name, tbl_name)).unwrap();
                cols.push(col_name);
            }
        });

        Ok(table_columns_map)
    }

    async fn fetch_data(
        &self,
        client: &mut Client,
    ) -> Result<(PostgresTablesWithTypes, PostgresTablesColumns), PostgresConnectorError> {
        let tables = self.fetch_tables(client).await?;
        let columns = self.fetch_columns(client).await?;

        Ok((tables, columns))
    }

    pub async fn validate(&self, client: &mut Client) -> Result<(), PostgresConnectorError> {
        let (tables, tables_columns) = self.fetch_data(client).await?;

        let missing_columns = self.find_missing_columns(tables_columns)?;
        if !missing_columns.is_empty() {
            let error_columns: Vec<String> = missing_columns
                .iter()
                .map(|(schema, table, column)| {
                    format!("{0} in {1}.{2} table", column, schema, table)
                })
                .collect();

            return Err(PostgresConnectorError::ColumnsNotFound(
                error_columns.join(", "),
            ));
        }

        let missing_tables = self.find_missing_tables(tables)?;
        if !missing_tables.is_empty() {
            return Err(PostgresConnectorError::TablesNotFound(missing_tables));
        }

        Ok(())
    }

    fn find_missing_columns(
        &self,
        tables_columns: PostgresTablesColumns,
    ) -> Result<Vec<(String, String, String)>, PostgresConnectorError> {
        let mut errors = vec![];
        for (key, columns) in tables_columns.iter() {
            let table_info = self
                .tables
                .get(key)
                .ok_or(PostgresConnectorError::TablesNotFound(vec![key.clone()]))?;

            if let Some(column_names) = table_info.columns.clone() {
                for c in column_names {
                    if !columns.contains(&c) {
                        errors.push((key.0.clone(), key.1.clone(), c));
                    }
                }
            }
        }

        Ok(errors)
    }

    fn find_missing_tables(
        &self,
        existing_tables: PostgresTablesWithTypes,
    ) -> Result<Vec<PostgresTableIdentifier>, PostgresConnectorError> {
        let mut missing_tables = vec![];

        for key in self.tables.keys() {
            existing_tables.get(key).map_or_else(
                || {
                    missing_tables.push(key.clone());
                    Ok(())
                },
                |table_type| {
                    table_type
                        .as_ref()
                        .map_or(Err(PostgresSchemaError::TableTypeNotFound), |typ| {
                            if typ.clone() != *"BASE TABLE" {
                                Err(PostgresSchemaError::UnsupportedTableType(
                                    typ.clone(),
                                    format!("{}.{}", key.0, key.1),
                                ))
                            } else {
                                Ok(())
                            }
                        })
                        .map_err(PostgresConnectorError::PostgresSchemaError)
                },
            )?;
        }

        Ok(missing_tables)
    }
}
