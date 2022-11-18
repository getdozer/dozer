use dozer_core::dag::node::StatefulProcessorFactory;

use crate::pipeline::{builder::get_select, product::factory::ProductProcessorFactory};

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
