use dozer_core::dag::node::ProcessorFactory;

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
    // let mut env_man = LmdbEnvironmentManager::create(Path::new("/tmp"), "tmp_join_test")
    //     .unwrap_or_else(|e| panic!("{}", e.to_string()));

    // let env = env_man.as_environment();

    let statement = get_select(
        "SELECT c.name, d.name, AVG(salary) \
    FROM Country c JOIN Department d ON c.id = d.country_id JOIN Users u ON d.id=u.department_id \
    WHERE salary >= 1000 GROUP BY c.name",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let join_tables =
        build_join_chain(&statement.from[0]).unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(join_tables.keys().len(), 3);
}
