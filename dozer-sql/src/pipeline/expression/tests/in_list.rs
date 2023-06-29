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

    let f = run_fct(
        "SELECT age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT age FROM users WHERE age > 44",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Null);

    let f = run_fct(
        "SELECT age FROM users WHERE age IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Int(42));
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

    let f = run_fct(
        "SELECT age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42) FROM users",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT age FROM users WHERE age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Int(42));

    let f = run_fct(
        "SELECT age FROM users WHERE age NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::empty()
            .field(
                FieldDefinition::new(
                    String::from("age"),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Null);
}
