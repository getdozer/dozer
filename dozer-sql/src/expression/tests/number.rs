use crate::expression::tests::test_common::*;
use dozer_types::types::{Field, FieldDefinition, FieldType, Schema, SourceDefinition};
use proptest::prelude::*;
use std::ops::Neg;

#[test]
fn test_abs_logic() {
    proptest!(ProptestConfig::with_cases(1000), |(i_num in 0i64..100000000i64)| {
        let f = run_fct(
            "SELECT ABS(c) FROM USERS",
            Schema::default()
                .field(
                    FieldDefinition::new(
                        String::from("c"),
                        FieldType::Int,
                        false,
                        SourceDefinition::Dynamic,
                    ),
                    false,
                )
                .clone(),
            vec![Field::Int(i_num.neg())],
        );
        assert_eq!(f, Field::Int(i_num));
    });
}
