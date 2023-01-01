use std::collections::HashMap;

use dozer_core::dag::node::{PortHandle, ProcessorFactory};
use dozer_types::types::{FieldDefinition, FieldType, Schema};

use crate::pipeline::{
    builder::get_select,
    product::factory::{build_join_chain, ProductProcessorFactory},
};

#[test]
fn test_product_one() {
    let statement = get_select(
        "SELECT department_id, AVG(salary) \
    FROM Users \
    WHERE salary >= 1000 GROUP BY department_id",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let product = ProductProcessorFactory::new(statement.from[0].clone());
    assert_eq!(product.get_input_ports().len(), 1);
    assert_eq!(product.get_output_ports().len(), 1);
}

#[test]
fn test_product_two() {
    let statement = get_select(
        "SELECT d.name, AVG(salary) \
    FROM Department d JOIN Users u ON d.id=u.department_id \
    WHERE salary >= 1000 GROUP BY d.name",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let product = ProductProcessorFactory::new(statement.from[0].clone());
    assert_eq!(product.get_input_ports().len(), 2);
    assert_eq!(product.get_output_ports().len(), 1);
}

#[test]
fn test_product_three() {
    let statement = get_select(
        "SELECT c.name, d.name, AVG(salary) \
    FROM Country c JOIN Department d ON c.id = d.country_id JOIN Users u ON d.id=u.department_id \
    WHERE salary >= 1000 GROUP BY c.name",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let product = ProductProcessorFactory::new(statement.from[0].clone());
    assert_eq!(product.get_input_ports().len(), 3);
    assert_eq!(product.get_output_ports().len(), 1);
}

#[test]
fn test_join_tables_three() {
    let user_schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("id"), FieldType::Int, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("name"), FieldType::String, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("salary"), FieldType::Float, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("department_id"), FieldType::Int, false),
            false,
        )
        .clone();

    let department_schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("id"), FieldType::Int, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("name"), FieldType::String, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("country_id"), FieldType::String, false),
            false,
        )
        .clone();

    let country_schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("id"), FieldType::Int, false),
            false,
        )
        .field(
            FieldDefinition::new(String::from("name"), FieldType::String, false),
            false,
        )
        .clone();

    let input_schemas = HashMap::from([
        (0 as PortHandle, user_schema),
        (1 as PortHandle, department_schema),
        (2 as PortHandle, country_schema),
    ]);

    let statement = get_select(
        "SELECT c.name, d.name, AVG(salary) \
    FROM Country c JOIN Department d ON c.id = d.country_id JOIN Users u ON d.id=u.department_id \
    WHERE salary >= 1000 GROUP BY c.name",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let join_tables = build_join_chain(&statement.from[0], input_schemas)
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(join_tables.get(&0).unwrap().left, None);
    assert_ne!(join_tables.get(&0).unwrap().right, None);

    //assert_eq!(join_tables.)
}
