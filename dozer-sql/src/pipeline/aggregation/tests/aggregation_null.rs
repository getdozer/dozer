use crate::output;
use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    init_input_schema, init_processor, insert_exp, FIELD_100_FLOAT, FIELD_100_INT, ITALY,
};
use crate::pipeline::builder::get_select;
use dozer_core::dag::dag::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::ProcessorFactory;
use dozer_types::types::FieldType::{Float, Int};
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use std::collections::HashMap;

#[test]
fn test_sum_aggregation_null() {
    let schema = init_input_schema(Int, "SUM");
    let (processor, tx) = init_processor(
        "SELECT Country, SUM(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        NULL, 100.0
        -------------
        SUM = 100.0
    */
    let mut inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::Null,
                FIELD_100_INT.clone(),
                FIELD_100_INT.clone(),
            ],
        ),
    };
    let mut out = output!(processor, inp, tx);
    let mut exp = vec![Operation::Insert {
        new: Record::new(None, vec![Field::Null, FIELD_100_INT.clone()]),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_aggregation_alias() {
    let schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("ID"), FieldType::Int, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("Salary"), FieldType::Int, false),
            false,
        )
        .clone();

    let select = get_select("SELECT ID, SUM(Salary) as Salaries FROM Users").unwrap();

    let factory = AggregationProcessorFactory::new(select.projection, select.group_by);
    let out_schema = factory
        .get_output_schema(
            &DEFAULT_PORT_HANDLE,
            &[(DEFAULT_PORT_HANDLE, schema.clone())]
                .into_iter()
                .collect(),
        )
        .unwrap();

    assert_eq!(
        out_schema,
        Schema::empty()
            .field(
                FieldDefinition::new(String::from("ID"), FieldType::Int, false),
                false,
            )
            .field(
                FieldDefinition::new(String::from("Salaries"), FieldType::Int, false),
                false,
            )
            .clone()
    );
}
