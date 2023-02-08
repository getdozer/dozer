use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{
        channels::ProcessorChannelForwarder,
        executor_local::DEFAULT_PORT_HANDLE,
        node::{PortHandle, ProcessorFactory},
        record_store::RecordReader,
    },
    storage::{
        common::Database, lmdb_storage::LmdbEnvironmentManager, transactions::SharedTransaction,
    },
};
use dozer_types::{
    ordered_float::OrderedFloat,
    parking_lot::RwLock,
    types::{Field, FieldDefinition, FieldType, Operation, Record, Schema},
};

use crate::pipeline::{
    builder::get_select,
    product::{factory::ProductProcessorFactory, join::get_lookup_key},
};

#[test]
fn test_composite_key() {
    let join_key = get_lookup_key(
        &Record::new(
            None,
            vec![
                Field::Int(11),
                Field::String("Alice".to_string()),
                Field::Int(0),
                Field::Float(OrderedFloat(5.5)),
            ],
        ),
        &[0_usize, 1_usize],
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    assert_eq!(
        join_key,
        vec![
            0_u8, 13_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 11_u8, 65_u8, 108_u8, 105_u8,
            99_u8, 101_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8, 0_u8
        ]
    );
}

// fn test_join_executor() {
//     let record = Record::new(
//         None,
//         vec![
//             Field::Int(11),
//             Field::String("Alice".to_string()),
//             Field::Int(0),
//             Field::Float(OrderedFloat(5.5)),
//         ],
//     );
//     let operator = JoinOperator::new(JoinOperatorType::Inner, 0, vec![0], 0);

//     operator.execute(&record, db, txn, reader, join_tables)
// }

struct TestChannelForwarder {
    operations: Vec<Operation>,
}

impl ProcessorChannelForwarder for TestChannelForwarder {
    fn send(
        &mut self,
        op: dozer_types::types::Operation,
        _port: dozer_core::node::PortHandle,
    ) -> Result<(), dozer_core::errors::ExecutionError> {
        self.operations.push(op);
        Ok(())
    }
}

#[test]
fn test_join_operator() {
    let select = get_select(
        "SELECT u.name, d.name, AVG(salary) \
    FROM Users u JOIN Departments d ON u.department_id = d.id \
    WHERE salary >= 100 GROUP BY department_id",
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let factory = ProductProcessorFactory::new(select.from[0].clone());

    let mut processor = factory.build();

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "product_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let binding = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    let mut tx = SharedTransaction::new(&binding);
    let mut fw = TestChannelForwarder { operations: vec![] };
    let reader_tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    let user_reader = RecordReader::new(reader_tx, Database::new(0));
    let department_reader_tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));
    let department_reader = RecordReader::new(department_reader_tx, Database::new(1));

    let readers = HashMap::from([(0, user_reader), (1, department_reader)]);

    let user_schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("id"), FieldType::Int, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("name"), FieldType::String, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("salary"), FieldType::Float, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("department_id"), FieldType::Int, false),
            false,
            false,
        )
        .clone();

    let department_schema = Schema::empty()
        .field(
            FieldDefinition::new(String::from("id"), FieldType::Int, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(String::from("name"), FieldType::String, false),
            false,
            false,
        )
        .clone();

    _ = processor.update_schema(
        DEFAULT_PORT_HANDLE,
        &HashMap::from([
            (0 as PortHandle, user_schema),
            (1 as PortHandle, department_schema),
        ]),
    );

    // Insert a User record
    _ = processor.process(
        0,
        Operation::Insert {
            new: Record::new(
                None,
                vec![
                    Field::Int(0),
                    Field::String("Mario".to_string()),
                    Field::Float(OrderedFloat(100.0)),
                    Field::Int(0),
                ],
            ),
        },
        &mut fw,
        &mut tx,
        &readers,
    );

    // Insert a Department record
    _ = processor.process(
        1,
        Operation::Insert {
            new: Record::new(None, vec![Field::Int(0), Field::String("IT".to_string())]),
        },
        &mut fw,
        &mut tx,
        &readers,
    );
}
