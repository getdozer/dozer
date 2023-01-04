use crate::pipeline::expression::tests::scalar_common::run_scalar_fct;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema};

#[test]
fn test_abs() {
    let f = run_scalar_fct(
        "SELECT ABS(c) FROM USERS",
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("c"), FieldType::Int, false),
                false,
            )
            .clone(),
        vec![Field::Int(-1)],
    );
    assert_eq!(f, Field::Int(1));
}
