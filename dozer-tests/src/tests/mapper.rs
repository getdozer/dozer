use dozer_types::types::{Field, Operation, Record, SchemaIdentifier};

use crate::sql_tests::SqlMapper;

#[test]
fn test_framework_to_dozer_types() {
    let tables: Vec<(&str, &str)> = vec![(
        "actor",
        "CREATE TABLE actor(
                actor_id integer NOT NULL, 
                name text NOT NULL
            )",
    )];
    let mut mapper = SqlMapper::default();
    mapper.create_tables(tables).unwrap();

    let schema_id = Some(SchemaIdentifier { id: 1, version: 1 });
    let op = mapper.get_operation_from_sql("INSERT INTO actor(actor_id,name) values (1, 'mario');");
    let values = vec![Field::Int(1), Field::String("mario".to_string())];
    let new_values = vec![Field::Int(1), Field::String("dario".to_string())];
    assert_eq!(
        Operation::Insert {
            new: Record {
                schema_id: schema_id.clone(),
                values: values.clone()
            }
        },
        op
    );

    let op = mapper.get_operation_from_sql("UPDATE actor SET name ='dario' WHERE actor_id=1;");
    assert_eq!(
        Operation::Update {
            old: Record {
                schema_id: schema_id.clone(),
                values: values.clone()
            },
            new: Record {
                schema_id: schema_id.clone(),
                values: new_values.clone()
            }
        },
        op
    );

    let op = mapper.get_operation_from_sql("DELETE FROM actor WHERE actor_id=1;");
    assert_eq!(
        Operation::Delete {
            old: Record {
                schema_id: schema_id.clone(),
                values: new_values.clone()
            },
        },
        op
    );

    let sql = mapper
        .map_operation_to_sql(
            &"actor".to_string(),
            Operation::Insert {
                new: Record {
                    schema_id: schema_id.clone(),
                    values: values.clone(),
                },
            },
        )
        .unwrap();
    assert_eq!(sql, "INSERT INTO actor(actor_id,name) values (1,'mario')");

    let sql = mapper
        .map_operation_to_sql(
            &"actor".to_string(),
            Operation::Update {
                old: Record {
                    schema_id: schema_id.clone(),
                    values,
                },
                new: Record {
                    schema_id: schema_id.clone(),
                    values: new_values.clone(),
                },
            },
        )
        .unwrap();
    assert_eq!(sql, "UPDATE actor SET name='dario' WHERE actor_id=1");

    let sql = mapper
        .map_operation_to_sql(
            &"actor".to_string(),
            Operation::Delete {
                old: Record {
                    schema_id,
                    values: new_values,
                },
            },
        )
        .unwrap();
    assert_eq!(sql, "DELETE FROM actor WHERE actor_id=1");
}
