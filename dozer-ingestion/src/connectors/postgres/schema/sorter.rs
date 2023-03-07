use std::collections::HashMap;

use crate::connectors::postgres::schema::helper::PostgresTable;
use crate::connectors::{ColumnInfo, TableInfo};
use crate::errors::PostgresSchemaError;
use crate::errors::PostgresSchemaError::ColumnNotFound;
use dozer_types::types::FieldDefinition;

pub fn sort_schemas(
    expected_tables_order: Option<&[TableInfo]>,
    mapped_tables: &HashMap<String, PostgresTable>,
) -> Result<Vec<(String, PostgresTable)>, PostgresSchemaError> {
    expected_tables_order.as_ref().map_or(
        Ok(mapped_tables
            .clone()
            .into_iter()
            .map(|(key, table)| (key, table))
            .collect()),
        |tables| {
            let mut sorted_tables: Vec<(String, PostgresTable)> = Vec::new();
            for table in tables.iter() {
                let postgres_table = mapped_tables.get(&table.name).ok_or(ColumnNotFound)?;

                let sorted_table = table.columns.as_ref().map_or_else(
                    || Ok(postgres_table.clone()),
                    |expected_order| {
                        if expected_order.is_empty() {
                            Ok(postgres_table.clone())
                        } else {
                            let sorted_fields = sort_fields(postgres_table, expected_order)?;
                            let mut new_table = PostgresTable::new(
                                *postgres_table.table_id(),
                                postgres_table.replication_type().clone(),
                            );
                            sorted_fields.into_iter().for_each(|(f, is_index_field)| {
                                new_table.add_field(f, is_index_field)
                            });
                            Ok(new_table)
                        }
                    },
                )?;

                sorted_tables.push((table.name.clone(), sorted_table));
            }

            Ok(sorted_tables)
        },
    )
}

fn sort_fields(
    postgres_table: &PostgresTable,
    expected_order: &[ColumnInfo],
) -> Result<Vec<(FieldDefinition, bool)>, PostgresSchemaError> {
    let mut sorted_fields = Vec::new();

    for c in expected_order {
        let current_index = postgres_table
            .fields()
            .iter()
            .position(|f| c.name == f.name)
            .ok_or(ColumnNotFound)?;

        let field = postgres_table
            .get_field(current_index)
            .ok_or(ColumnNotFound)?;
        let is_index_field = postgres_table
            .is_index_field(current_index)
            .ok_or(ColumnNotFound)?;

        sorted_fields.push((field.clone(), *is_index_field));
    }

    Ok(sorted_fields)
}

#[cfg(test)]
mod tests {
    use dozer_types::types::{FieldType, SourceDefinition};
    use std::collections::HashMap;

    use crate::connectors::postgres::schema::helper::PostgresTable;
    use crate::connectors::postgres::schema::sorter::{sort_fields, sort_schemas};
    use crate::connectors::{ColumnInfo, TableInfo};
    use dozer_types::types::FieldDefinition;

    fn generate_postgres_table() -> PostgresTable {
        let mut postgres_table = PostgresTable::new(1, "d".to_string());
        postgres_table.add_field(
            FieldDefinition {
                name: "second field".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            false,
        );
        postgres_table.add_field(
            FieldDefinition {
                name: "first field".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            true,
        );
        postgres_table.add_field(
            FieldDefinition {
                name: "third field".to_string(),
                typ: FieldType::UInt,
                nullable: false,
                source: SourceDefinition::Dynamic,
            },
            false,
        );
        postgres_table
    }
    #[test]
    fn test_fields_sort() {
        let postgres_table = generate_postgres_table();

        let expected_order = [
            ColumnInfo {
                name: "first field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "second field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "third field".to_string(),
                data_type: None,
            },
        ];

        let result = sort_fields(&postgres_table, &expected_order).unwrap();
        assert_eq!(result.get(0).unwrap().0.name, "first field");
        assert_eq!(result.get(1).unwrap().0.name, "second field");
        assert_eq!(result.get(2).unwrap().0.name, "third field");

        assert!(result.get(0).unwrap().1);
        assert!(!result.get(1).unwrap().1);
        assert!(!result.get(2).unwrap().1);
    }

    #[test]
    fn test_tables_sort_without_columns() {
        let postgres_table = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert("sort_test".to_string(), postgres_table.clone());

        let expected_table_order = &[TableInfo {
            name: "sort_test".to_string(),
            columns: None,
        }];

        let result = sort_schemas(Some(expected_table_order), &mapped_tables).unwrap();
        assert_eq!(
            result.get(0).unwrap().1.fields().get(0).unwrap().name,
            postgres_table.get_field(0).unwrap().name
        );
        assert_eq!(
            result.get(0).unwrap().1.fields().get(1).unwrap().name,
            postgres_table.get_field(1).unwrap().name
        );
        assert_eq!(
            result.get(0).unwrap().1.fields().get(2).unwrap().name,
            postgres_table.get_field(2).unwrap().name
        );

        assert_eq!(
            result.get(0).unwrap().1.is_index_field(0),
            postgres_table.is_index_field(0)
        );
        assert_eq!(
            result.get(0).unwrap().1.is_index_field(1),
            postgres_table.is_index_field(1)
        );
        assert_eq!(
            result.get(0).unwrap().1.is_index_field(2),
            postgres_table.is_index_field(2)
        );
    }

    #[test]
    fn test_tables_sort_with_single_column() {
        let postgres_table = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert("sort_test".to_string(), postgres_table);

        let columns_order = vec![ColumnInfo {
            name: "third field".to_string(),
            data_type: None,
        }];
        let expected_table_order = &[TableInfo {
            name: "sort_test".to_string(),
            columns: Some(columns_order.clone()),
        }];

        let result = sort_schemas(Some(expected_table_order), &mapped_tables).unwrap();
        assert_eq!(
            result.get(0).unwrap().1.fields().get(0).unwrap().name,
            columns_order.get(0).unwrap().name
        );
        assert_eq!(result.get(0).unwrap().1.fields().len(), 1);
    }

    #[test]
    fn test_tables_sort_with_multi_columns() {
        let postgres_table = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert("sort_test".to_string(), postgres_table);

        let columns_order = vec![
            ColumnInfo {
                name: "first field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "second field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "third field".to_string(),
                data_type: None,
            },
        ];
        let expected_table_order = &[TableInfo {
            name: "sort_test".to_string(),
            columns: Some(columns_order.clone()),
        }];

        let result = sort_schemas(Some(expected_table_order), &mapped_tables).unwrap();
        assert_eq!(
            result.get(0).unwrap().1.fields().get(0).unwrap().name,
            columns_order.get(0).unwrap().name
        );
        assert_eq!(
            result.get(0).unwrap().1.fields().get(1).unwrap().name,
            columns_order.get(1).unwrap().name
        );
        assert_eq!(
            result.get(0).unwrap().1.fields().get(2).unwrap().name,
            columns_order.get(2).unwrap().name
        );
        assert_eq!(result.get(0).unwrap().1.fields().len(), 3);
    }

    #[test]
    fn test_tables_sort_with_multi_tables() {
        let postgres_table_1 = generate_postgres_table();
        let postgres_table_2 = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert("sort_test_second".to_string(), postgres_table_1);
        mapped_tables.insert("sort_test_first".to_string(), postgres_table_2);

        let columns_order_1 = vec![
            ColumnInfo {
                name: "first field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "second field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "third field".to_string(),
                data_type: None,
            },
        ];
        let columns_order_2 = vec![
            ColumnInfo {
                name: "third field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "second field".to_string(),
                data_type: None,
            },
            ColumnInfo {
                name: "first field".to_string(),
                data_type: None,
            },
        ];
        let expected_table_order = &[
            TableInfo {
                name: "sort_test_first".to_string(),
                columns: Some(columns_order_1.clone()),
            },
            TableInfo {
                name: "sort_test_second".to_string(),
                columns: Some(columns_order_2.clone()),
            },
        ];

        let result = sort_schemas(Some(expected_table_order), &mapped_tables).unwrap();
        let first_table_after_sort = result.get(0).unwrap();
        let second_table_after_sort = result.get(1).unwrap();

        assert_eq!(
            first_table_after_sort.0,
            expected_table_order.get(0).unwrap().name
        );
        assert_eq!(
            second_table_after_sort.0,
            expected_table_order.get(1).unwrap().name
        );
        assert_eq!(
            first_table_after_sort.1.fields().get(0).unwrap().name,
            columns_order_1.get(0).unwrap().name
        );
        assert_eq!(
            first_table_after_sort.1.fields().get(1).unwrap().name,
            columns_order_1.get(1).unwrap().name
        );
        assert_eq!(
            first_table_after_sort.1.fields().get(2).unwrap().name,
            columns_order_1.get(2).unwrap().name
        );
        assert_eq!(
            second_table_after_sort.1.fields().get(0).unwrap().name,
            columns_order_2.get(0).unwrap().name
        );
        assert_eq!(
            second_table_after_sort.1.fields().get(1).unwrap().name,
            columns_order_2.get(1).unwrap().name
        );
        assert_eq!(
            second_table_after_sort.1.fields().get(2).unwrap().name,
            columns_order_2.get(2).unwrap().name
        );
    }
}
