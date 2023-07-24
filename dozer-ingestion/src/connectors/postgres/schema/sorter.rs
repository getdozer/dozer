use std::collections::HashMap;

use crate::connectors::postgres::schema::helper::PostgresTable;
use crate::connectors::ListOrFilterColumns;
use crate::errors::PostgresSchemaError;
use crate::errors::PostgresSchemaError::ColumnNotFound;
use dozer_types::types::FieldDefinition;

use super::helper::{SchemaTableIdentifier, DEFAULT_SCHEMA_NAME};

pub fn sort_schemas(
    expected_tables_order: &[ListOrFilterColumns],
    mapped_tables: &HashMap<SchemaTableIdentifier, PostgresTable>,
) -> Result<Vec<(SchemaTableIdentifier, PostgresTable)>, PostgresSchemaError> {
    let mut sorted_tables: Vec<(SchemaTableIdentifier, PostgresTable)> = Vec::new();
    for table in expected_tables_order.iter() {
        let table_identifier = (
            table
                .schema
                .clone()
                .unwrap_or(DEFAULT_SCHEMA_NAME.to_string()),
            table.name.clone(),
        );
        let postgres_table = mapped_tables.get(&table_identifier).ok_or(ColumnNotFound)?;

        let sorted_table = table.columns.as_ref().map_or_else(
            || Ok::<PostgresTable, PostgresSchemaError>(postgres_table.clone()),
            |expected_order| {
                if expected_order.is_empty() {
                    Ok(postgres_table.clone())
                } else {
                    let sorted_fields = sort_fields(postgres_table, expected_order)?;
                    let mut new_table =
                        PostgresTable::new(postgres_table.replication_type().clone());
                    sorted_fields
                        .into_iter()
                        .for_each(|(f, is_index_field)| new_table.add_field(f, is_index_field));
                    Ok(new_table)
                }
            },
        )?;

        sorted_tables.push((table_identifier, sorted_table));
    }

    Ok(sorted_tables)
}

fn sort_fields(
    postgres_table: &PostgresTable,
    expected_order: &[String],
) -> Result<Vec<(FieldDefinition, bool)>, PostgresSchemaError> {
    let mut sorted_fields = Vec::new();

    for c in expected_order {
        let current_index = postgres_table
            .fields()
            .iter()
            .position(|f| c == &f.name)
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
    use crate::connectors::ListOrFilterColumns;
    use dozer_types::types::FieldDefinition;

    fn generate_postgres_table() -> PostgresTable {
        let mut postgres_table = PostgresTable::new("d".to_string());
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
            "first field".to_string(),
            "second field".to_string(),
            "third field".to_string(),
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
        mapped_tables.insert(
            ("public".to_string(), "sort_test".to_string()),
            postgres_table.clone(),
        );

        let expected_table_order = &[ListOrFilterColumns {
            name: "sort_test".to_string(),
            schema: Some("public".to_string()),
            columns: None,
        }];

        let result = sort_schemas(expected_table_order, &mapped_tables).unwrap();
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
        mapped_tables.insert(
            ("public".to_string(), "sort_test".to_string()),
            postgres_table,
        );

        let columns_order = vec!["third field".to_string()];
        let expected_table_order = &[ListOrFilterColumns {
            name: "sort_test".to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns_order.clone()),
        }];

        let result = sort_schemas(expected_table_order, &mapped_tables).unwrap();
        assert_eq!(
            &result.get(0).unwrap().1.fields().get(0).unwrap().name,
            columns_order.get(0).unwrap()
        );
        assert_eq!(result.get(0).unwrap().1.fields().len(), 1);
    }

    #[test]
    fn test_tables_sort_with_multi_columns() {
        let postgres_table = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert(
            ("public".to_string(), "sort_test".to_string()),
            postgres_table,
        );

        let columns_order = vec![
            "first field".to_string(),
            "second field".to_string(),
            "third field".to_string(),
        ];
        let expected_table_order = &[ListOrFilterColumns {
            name: "sort_test".to_string(),
            schema: Some("public".to_string()),
            columns: Some(columns_order.clone()),
        }];

        let result = sort_schemas(expected_table_order, &mapped_tables).unwrap();
        assert_eq!(
            &result.get(0).unwrap().1.fields().get(0).unwrap().name,
            columns_order.get(0).unwrap()
        );
        assert_eq!(
            &result.get(0).unwrap().1.fields().get(1).unwrap().name,
            columns_order.get(1).unwrap()
        );
        assert_eq!(
            &result.get(0).unwrap().1.fields().get(2).unwrap().name,
            columns_order.get(2).unwrap()
        );
        assert_eq!(result.get(0).unwrap().1.fields().len(), 3);
    }

    #[test]
    fn test_tables_sort_with_multi_tables() {
        let postgres_table_1 = generate_postgres_table();
        let postgres_table_2 = generate_postgres_table();
        let mut mapped_tables = HashMap::new();
        mapped_tables.insert(
            ("public".to_string(), "sort_test_second".to_string()),
            postgres_table_1,
        );
        mapped_tables.insert(
            ("public".to_string(), "sort_test_first".to_string()),
            postgres_table_2,
        );

        let columns_order_1 = vec![
            "first field".to_string(),
            "second field".to_string(),
            "third field".to_string(),
        ];
        let columns_order_2 = vec![
            "third field".to_string(),
            "second field".to_string(),
            "first field".to_string(),
        ];
        let expected_table_order = &[
            ListOrFilterColumns {
                name: "sort_test_first".to_string(),
                schema: Some("public".to_string()),
                columns: Some(columns_order_1.clone()),
            },
            ListOrFilterColumns {
                name: "sort_test_second".to_string(),
                schema: Some("public".to_string()),
                columns: Some(columns_order_2.clone()),
            },
        ];

        let result = sort_schemas(expected_table_order, &mapped_tables).unwrap();
        let first_table_after_sort = result.get(0).unwrap();
        let second_table_after_sort = result.get(1).unwrap();

        assert_eq!(
            first_table_after_sort.0 .1,
            expected_table_order.get(0).unwrap().name
        );
        assert_eq!(
            second_table_after_sort.0 .1,
            expected_table_order.get(1).unwrap().name
        );
        assert_eq!(
            &first_table_after_sort.1.fields().get(0).unwrap().name,
            columns_order_1.get(0).unwrap()
        );
        assert_eq!(
            &first_table_after_sort.1.fields().get(1).unwrap().name,
            columns_order_1.get(1).unwrap()
        );
        assert_eq!(
            &first_table_after_sort.1.fields().get(2).unwrap().name,
            columns_order_1.get(2).unwrap()
        );
        assert_eq!(
            &second_table_after_sort.1.fields().get(0).unwrap().name,
            columns_order_2.get(0).unwrap()
        );
        assert_eq!(
            &second_table_after_sort.1.fields().get(1).unwrap().name,
            columns_order_2.get(1).unwrap()
        );
        assert_eq!(
            &second_table_after_sort.1.fields().get(2).unwrap().name,
            columns_order_2.get(2).unwrap()
        );
    }
}
