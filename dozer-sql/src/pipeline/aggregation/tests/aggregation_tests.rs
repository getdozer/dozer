use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{executor_local::DEFAULT_PORT_HANDLE, node::Processor},
    storage::{lmdb_storage::LmdbEnvironmentManager, transactions::SharedTransaction},
};
use dozer_types::{
    ordered_float::OrderedFloat,
    parking_lot::RwLock,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

use crate::pipeline::{aggregation::processor::AggregationProcessor, builder::get_select};

#[test]
fn test_simple_aggregation() {
    let select = get_select(
        "SELECT Country, SUM(Salary) \
        FROM Users \
        WHERE Salary >= 1 GROUP BY Country",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut processor =
        AggregationProcessor::new(select.projection.clone(), select.group_by.clone());

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    // let binding = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    // let mut tx = SharedTransaction::new(&binding);

    let tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));

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
        .field(
            FieldDefinition::new(String::from("SUM(Salary)"), FieldType::Float, false),
            false,
            false,
        )
        .clone();

    let _output_schema = processor
        .update_schema(
            DEFAULT_PORT_HANDLE,
            &HashMap::from([(DEFAULT_PORT_HANDLE, schema)]),
        )
        .unwrap();

    let inp = Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    };

    let out = processor
        .aggregate(
            &mut SharedTransaction::new(&tx),
            &processor.db.clone().unwrap(),
            inp,
        )
        .unwrap_or_else(|_e| panic!("Error executing aggregate"));

    let exp = vec![Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::String("Italy".to_string()),
                Field::Float(OrderedFloat(100.0)),
            ],
        ),
    }];
    assert_eq!(out, exp);
}
