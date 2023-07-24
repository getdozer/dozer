use crate::output;

use crate::pipeline::aggregation::tests::aggregation_tests_utils::{
    delete_exp, delete_field, init_input_schema, init_processor, insert_exp, insert_field,
    FIELD_100_INT, FIELD_1_INT, ITALY,
};

use dozer_core::DEFAULT_PORT_HANDLE;

use dozer_types::types::ref_types::ProcessorRecordRef;
use dozer_types::types::FieldType::Int;
use dozer_types::types::{Field, ProcessorOperation, ProcessorRecord};
use std::collections::HashMap;

#[test]
fn test_sum_aggregation_null() {
    let schema = init_input_schema(Int, "SUM");
    let mut processor = init_processor(
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
    let mut record = ProcessorRecord::new();
    record.extend_direct_field(Field::Int(0));
    record.extend_direct_field(Field::Null);
    record.extend_direct_field(FIELD_100_INT.clone());
    record.extend_direct_field(FIELD_100_INT.clone());
    let inp = ProcessorOperation::Insert {
        new: ProcessorRecordRef::new(record),
    };
    let out = output!(processor, inp);
    let mut exp_record = ProcessorRecord::new();
    exp_record.extend_direct_field(Field::Null);
    exp_record.extend_direct_field(FIELD_100_INT.clone());
    let exp = vec![ProcessorOperation::Insert {
        new: ProcessorRecordRef::new(exp_record),
    }];
    assert_eq!(out, exp);
}

#[test]
fn test_sum_aggregation_del_and_insert() {
    let schema = init_input_schema(Int, "COUNT");
    let mut processor = init_processor(
        "SELECT Country, COUNT(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    )
    .unwrap();

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    let mut inp = insert_field(ITALY, FIELD_100_INT);
    let mut out = output!(processor, inp);
    let mut exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Delete last record
    /*
        -------------
        COUNT = 0
    */
    inp = delete_field(ITALY, FIELD_100_INT);
    out = output!(processor, inp);
    exp = vec![delete_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);

    // Insert 100 for segment Italy
    /*
        Italy, 100.0
        -------------
        COUNT = 1
    */
    let inp = insert_field(ITALY, FIELD_100_INT);
    let out = output!(processor, inp);
    let exp = vec![insert_exp(ITALY, FIELD_1_INT)];
    assert_eq!(out, exp);
}
