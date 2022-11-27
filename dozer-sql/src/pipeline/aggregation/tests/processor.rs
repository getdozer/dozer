use std::collections::HashMap;
use std::path::Path;
use dozer_core::dag::executor_local::DEFAULT_PORT_HANDLE;
use dozer_core::dag::node::ProcessorFactory;
use dozer_core::storage::lmdb_storage::LmdbEnvironmentManager;
use crate::pipeline::aggregation::processor::AggregationProcessorFactory;
use crate::pipeline::aggregation::tests::schema::{get_expected_schema, get_input_schema};
use crate::pipeline::builder::get_select;

#[test]
fn test_schema_building() {
    let select = get_select(
        "SELECT City, Country, People \
        FROM Users \
        WHERE Salary >= 1000 GROUP BY City, Country, People",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let aggregation = AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

    let mut processor = aggregation.build();

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));
    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let schema = get_input_schema();

    let output_schema = processor.update_schema(
        DEFAULT_PORT_HANDLE,
        &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    );

    assert_eq!(output_schema.unwrap(), get_expected_schema());
}