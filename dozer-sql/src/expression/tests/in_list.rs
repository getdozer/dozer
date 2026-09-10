use crate::expression::tests::test_common::run_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};

#[test]
fn test_in_list() {
    let f = run_fct(
        "SELECT 42 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        Schema::default(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(false));

    let f = run_fct(
        "SELECT 42 IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::default(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(true));

    let schema = Schema::default()
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
        Schema::default(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(true));

    let f = run_fct(
        "SELECT 42 NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 42)",
        Schema::default(),
        vec![],
    );
    assert_eq!(f, Field::Boolean(false));

    let schema = Schema::default()
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

#[test]
fn test_in_list_with_nulls() {
    let null_field = Field::Null;

    // 1. 42 IN (NULL) -> Field::Null
    let f = run_fct("SELECT 42 IN (NULL)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 2. 42 IN (1, NULL) -> Field::Null
    let f = run_fct("SELECT 42 IN (1, NULL)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 3. 42 IN (1, NULL, 42) -> Field::Boolean(true) (Exact match found)
    let f = run_fct("SELECT 42 IN (1, NULL, 42)", Schema::default(), vec![]);
    assert_eq!(f, Field::Boolean(true));

    // 4. 42 NOT IN (1, NULL) -> Field::Null
    let f = run_fct("SELECT 42 NOT IN (1, NULL)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 5. NULL IN (1, 2, 3) -> Field::Null
    let f = run_fct("SELECT NULL IN (1, 2, 3)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 6. No exact match, list contains NULL -> Field::Null
    let f = run_fct("SELECT 99 IN (1, 2, NULL)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 7. NOT IN (1, 2, 3) -> True (42 is not in list, no NULLs)
    let f = run_fct("SELECT 42 NOT IN (1, 2, 3)", Schema::default(), vec![]);
    assert_eq!(f, Field::Boolean(true));

    // 8. NOT IN (1, 2, 3, 42) -> False (42 is in list)
    let f = run_fct("SELECT 42 NOT IN (1, 2, 3, 42)", Schema::default(), vec![]);
    assert_eq!(f, Field::Boolean(false));

    // 9. NOT IN (1, 2, 3, 42, NULL) -> False (42 matches, so NOT(true) == false)
    let f = run_fct("SELECT 42 NOT IN (1, 2, 3, 42, NULL)", Schema::default(), vec![]);
    assert_eq!(f, Field::Boolean(false));

    // 10. NOT IN (1, 2, 3, NULL) with no match -> Null
    let f = run_fct("SELECT 42 NOT IN (1, 2, 3, NULL)", Schema::default(), vec![]);
    assert_eq!(f, null_field);

    // 11. Column expression evaluates to NULL in record
    let schema = Schema::default()
        .field(
            FieldDefinition::new(
                String::from("age"),
                FieldType::Int,
                true,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();
    let f = run_fct(
        "SELECT age IN (1, 2, 3) FROM users",
        schema.clone(),
        vec![Field::Null],
    );
    assert_eq!(f, null_field);

    // 12. Column expression matches with NULL in list
    let f = run_fct(
        "SELECT age IN (1, NULL, 42) FROM users",
        schema,
        vec![Field::Int(42)],
    );
    assert_eq!(f, Field::Boolean(true));
}
