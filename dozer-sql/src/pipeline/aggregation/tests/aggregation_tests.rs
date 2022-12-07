use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{channels::ProcessorChannelForwarder, dag::DEFAULT_PORT_HANDLE, node::ProcessorFactory},
    storage::{lmdb_storage::LmdbEnvironmentManager, transactions::SharedTransaction},
};
use dozer_types::{
    ordered_float::OrderedFloat,
    parking_lot::RwLock,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

use crate::pipeline::{aggregation::factory::AggregationProcessorFactory, builder::get_select};

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
            FieldDefinition::new(String::from("ID"), FieldType::Int, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("Country"), FieldType::String, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("Salary"), FieldType::Float, false),
            false,
            false,
        )
        .clone();

    let processor_factory =
        AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

    let mut processor = processor_factory.build(
        HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
        HashMap::new(),
    );

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let binding = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    let mut tx = SharedTransaction::new(&binding);
    let mut fw = TestChannelForwarder { operations: vec![] };

    let op = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.5)),
            ],
        ),
    };

    _ = processor.process(DEFAULT_PORT_HANDLE, op, &mut fw, &mut tx, &HashMap::new());
}
