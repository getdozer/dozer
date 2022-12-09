use std::{collections::HashMap, path::Path, sync::Arc};

use dozer_core::{
    dag::{
        dag::DEFAULT_PORT_HANDLE,
        node::{PortHandle, Processor},
    },
    storage::{common::RenewableRwTransaction, lmdb_storage::LmdbEnvironmentManager},
};
use dozer_types::parking_lot::RwLock;
use dozer_types::types::{Field, FieldDefinition, FieldType, Operation, Record, Schema};
use dozer_types::{parking_lot::RwLock, types::Schema};

use crate::pipeline::{
    aggregation::{factory::get_aggregation_rules, processor::AggregationProcessor},
    builder::get_select,
    errors::PipelineError,
};

type AggregationTransaction = dozer_types::parking_lot::lock_api::RwLock<
    dozer_types::parking_lot::RawRwLock,
    Box<dyn RenewableRwTransaction>,
>;
use dozer_types::ordered_float::OrderedFloat;
use std::{path::Path, sync::Arc};

pub const ITALY: &str = "Italy";
pub const SINGAPORE: &str = "Singapore";
pub const FIELD_100_FLOAT: &Field = &Field::Float(OrderedFloat(100.0));
pub const FIELD_200_FLOAT: &Field = &Field::Float(OrderedFloat(200.0));
pub const FIELD_75_FLOAT: &Field = &Field::Float(OrderedFloat(75.0));
pub const FIELD_50_FLOAT: &Field = &Field::Float(OrderedFloat(50.0));
pub const FIELD_250_DIV_3_FLOAT: &Field = &Field::Float(OrderedFloat(250.0 / 3.0));
pub const FIELD_350_DIV_3_FLOAT: &Field = &Field::Float(OrderedFloat(350.0 / 3.0));

pub(crate) fn init_processor(
    sql: &str,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<(AggregationProcessor, Arc<AggregationTransaction>), PipelineError> {
    let select = get_select(sql)?;

    let input_schema = input_schemas
        .get(&DEFAULT_PORT_HANDLE)
        .unwrap_or_else(|| panic!("Error getting Input Schema"));

    let output_field_rules = get_aggregation_rules(
        &select.projection.clone(),
        &select.group_by.clone(),
        input_schema,
    )
    .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let mut processor = AggregationProcessor::new(output_field_rules, input_schema);

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(storage.as_environment())
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let tx = Arc::new(RwLock::new(storage.create_txn().unwrap()));

    Ok((processor, tx))
}

pub(crate) fn init_input_schema(field_type: FieldType, aggregator_name: &str) -> Schema {
    Schema::empty()
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
            FieldDefinition::new(String::from("Salary"), field_type, false),
            false,
            false,
        )
        .field(
            FieldDefinition::new(format!("{}(Salary)", aggregator_name), field_type, false),
            false,
            false,
        )
        .clone()
}

pub(crate) fn insert_field(country: &str, insert_field: &Field) -> Operation {
    Operation::Insert {
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String(country.to_string()),
                insert_field.clone(),
                insert_field.clone(),
            ],
        ),
    }
}

pub(crate) fn delete_field(country: &str, deleted_field: &Field) -> Operation {
    Operation::Delete {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String(country.to_string()),
                deleted_field.clone(),
                deleted_field.clone(),
            ],
        ),
    }
}

pub(crate) fn update_field(
    old_country: &str,
    new_country: &str,
    old: &Field,
    new: &Field,
) -> Operation {
    Operation::Update {
        old: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String(old_country.to_string()),
                old.clone(),
                old.clone(),
            ],
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String(new_country.to_string()),
                new.clone(),
                new.clone(),
            ],
        ),
    }
}

pub(crate) fn insert_exp(country: &str, inserted_field: &Field) -> Operation {
    Operation::Insert {
        new: Record::new(
            None,
            vec![Field::String(country.to_string()), inserted_field.clone()],
        ),
    }
}

pub(crate) fn delete_exp(country: &str, deleted_field: &Field) -> Operation {
    Operation::Delete {
        old: Record::new(
            None,
            vec![Field::String(country.to_string()), deleted_field.clone()],
        ),
    }
}

pub(crate) fn update_exp(
    old_country: &str,
    new_country: &str,
    old: &Field,
    new: &Field,
) -> Operation {
    Operation::Update {
        old: Record::new(
            None,
            vec![Field::String(old_country.to_string()), old.clone()],
        ),
        new: Record::new(
            None,
            vec![Field::String(new_country.to_string()), new.clone()],
        ),
    }
}

#[macro_export]
macro_rules! output {
    ($processor:expr, $inp:expr, $tx:expr) => {
        $processor
            .aggregate(
                &mut SharedTransaction::new(&$tx),
                &$processor.db.clone().unwrap(),
                $inp,
            )
            .unwrap_or_else(|_e| panic!("Error executing aggregate"))
    };
}

#[macro_export]
macro_rules! update_schema {
    ($processor:expr, $schema:expr, $port:expr) => {
        $processor
            .update_schema($port, &HashMap::from([($port, $schema)]))
            .unwrap();
    };
}
