use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{
        channels::ProcessorChannelForwarder, executor_local::DEFAULT_PORT_HANDLE,
        node::ProcessorFactory,
    },
    storage::{lmdb_storage::LmdbEnvironmentManager, transactions::SharedTransaction},
};
use dozer_types::{
    ordered_float::OrderedFloat,
    parking_lot::RwLock,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

use crate::pipeline::{aggregation::processor::AggregationProcessorFactory, builder::get_select};

pub struct TestChannelForwarder {
    pub(crate) operations: Vec<Operation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(
        &mut self,
        op: Operation,
        _port: dozer_core::dag::node::PortHandle,
    ) -> Result<(), dozer_core::dag::errors::ExecutionError> {
        self.operations.push(op);
        Ok(())
    }
}

#[test]
fn test_simple_aggregation() {
    let select = get_select(
        "SELECT department_id, SUM(salary) \
        FROM Users \
        WHERE salary >= 1000 GROUP BY department_id",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let aggregation = AggregationProcessorFactory::new(select.projection.clone(), select.group_by);

    let mut processor = aggregation.build();

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let binding = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    let mut tx = SharedTransaction::new(&binding);
    let mut fw = TestChannelForwarder { operations: vec![] };

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

    _ = processor.update_schema(
        DEFAULT_PORT_HANDLE,
        &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
    );

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
