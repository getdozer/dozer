use dozer_core::{
    dag::{channels::ProcessorChannelForwarder, dag::DEFAULT_PORT_HANDLE, node::ProcessorFactory},
    storage::lmdb_storage::LmdbEnvironmentManager,
};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::path::Path;

use dozer_types::ordered_float::OrderedFloat;
use dozer_types::types::SourceDefinition;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};

use crate::pipeline::aggregation::factory::AggregationProcessorFactory;
use crate::pipeline::tests::utils::get_select;

struct TestChannelForwarder {
    operations: Vec<Operation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(
        &mut self,
        op: dozer_types::types::Operation,
        _port: dozer_core::dag::node::PortHandle,
    ) -> Result<(), dozer_core::dag::errors::ExecutionError> {
        self.operations.push(op);
        Ok(())
    }
}

#[test]
fn test_simple_aggregation() {
    let select = get_select(
        "SELECT Country, SUM(Salary) \
        FROM Users \
        WHERE Salary >= 1000 GROUP BY Country",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let schema = Schema::empty()
        .field(
            FieldDefinition::new(
                String::from("ID"),
                FieldType::Int,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("Country"),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                String::from("Salary"),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .clone();

    let processor_factory =
        AggregationProcessorFactory::new(select.projection.clone(), select.group_by, false);

    let mut processor = processor_factory
        .build(
            HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
            HashMap::new(),
        )
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.borrow_mut())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut tx = storage.create_txn().unwrap();
    let mut fw = TestChannelForwarder { operations: vec![] };

    let op = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.5)),
            ],
            None,
        ),
    };

    let _res = processor.process(DEFAULT_PORT_HANDLE, op, &mut fw, &mut tx, &HashMap::new());
}
