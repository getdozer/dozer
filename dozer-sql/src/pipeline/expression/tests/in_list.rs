use crate::pipeline::expression::tests::test_common::run_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_in_list() {
    let f = run_fct(
        "SELECT 42 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        Schema::empty(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT 42 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::empty(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(true));

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("age"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();
    let f = run_fct(
        "SELECT age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) FROM users",
        schema.clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42) FROM users",
        schema,
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(true));
}

#[test]
fn test_not_in_list() {
    let f = run_fct(
        "SELECT 42 NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        Schema::empty(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT 42 NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::empty(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(false));

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("age"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();
    let f = run_fct(
        "SELECT age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) FROM users",
        schema.clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42) FROM users",
        schema,
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(false));
}
