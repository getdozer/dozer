use crate::pipeline::{
    aggregation::{factory::get_aggregation_rules, processor::AggregationProcessor},
    errors::PipelineError,
    tests::utils::get_select,
};
use dozer_core::{
    dag::{
        node::{PortHandle, Processor},
        DEFAULT_PORT_HANDLE,
    },
    storage::lmdb_storage::{LmdbEnvironmentManager, SharedTransaction},
};
use dozer_types::chrono::{DateTime, NaiveDate, TimeZone, Utc};
use dozer_types::ordered_float::OrderedFloat;
use dozer_types::rust_decimal::Decimal;
use dozer_types::types::{
    Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition, DATE_FORMAT,
};
use std::collections::HashMap;
use std::ops::Div;
use std::path::Path;

pub(crate) fn init_processor(
    sql: &str,
    input_schemas: HashMap<PortHandle, Schema>,
) -> Result<(AggregationProcessor, SharedTransaction), PipelineError> {
    let select = get_select(sql)?;

    let input_schema = input_schemas
        .get(&DEFAULT_PORT_HANDLE)
        .unwrap_or_else(|| panic!("Error getting Input Schema"));

    let output_field_rules = get_aggregation_rules(
        &select.projection.clone(),
        &select.group_by.clone(),
        input_schema,
    )?;

    let mut processor = AggregationProcessor::new(output_field_rules, input_schema.clone());

    let mut storage = LmdbEnvironmentManager::create(Path::new("/tmp"), "aggregation_test")
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    processor
        .init(&mut storage)
        .unwrap_or_else(|e| panic!("{}", e.to_string()));

    let tx = storage.create_txn().unwrap();

    Ok((processor, tx))
}

pub(crate) fn init_input_schema(field_type: FieldType, aggregator_name: &str) -> Schema {
    Schema::empty()
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
                field_type,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                format!("{aggregator_name}(Salary)"),
                field_type,
                false,
                SourceDefinition::Dynamic,
            ),
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
            None,
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
            None,
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
            None,
        ),
        new: Record::new(
            None,
            vec![
                Field::Int(0),
                Field::String(new_country.to_string()),
                new.clone(),
                new.clone(),
            ],
            None,
        ),
    }
}

pub(crate) fn insert_exp(country: &str, inserted_field: &Field) -> Operation {
    Operation::Insert {
        new: Record::new(
            None,
            vec![Field::String(country.to_string()), inserted_field.clone()],
            None,
        ),
    }
}

pub(crate) fn delete_exp(country: &str, deleted_field: &Field) -> Operation {
    Operation::Delete {
        old: Record::new(
            None,
            vec![Field::String(country.to_string()), deleted_field.clone()],
            None,
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
            None,
        ),
        new: Record::new(
            None,
            vec![Field::String(new_country.to_string()), new.clone()],
            None,
        ),
    }
}

pub fn get_decimal_field(val: i64) -> Field {
    Field::Decimal(Decimal::new(val, 0))
}

pub fn get_decimal_div_field(numerator: i64, denominator: i64) -> Field {
    Field::Decimal(Decimal::new(numerator, 0).div(Decimal::new(denominator, 0)))
}

pub fn get_ts_field(val: i64) -> Field {
    Field::Timestamp(DateTime::from(Utc.timestamp_millis(val)))
}

pub fn get_date_field(val: &str) -> Field {
    Field::Date(NaiveDate::parse_from_str(val, DATE_FORMAT).unwrap())
}

#[macro_export]
macro_rules! output {
    ($processor:expr, $inp:expr, $tx:expr) => {
        $processor
            .aggregate(&mut $tx.write(), $processor.db.unwrap(), $inp)
            .unwrap_or_else(|_e| panic!("Error executing aggregate"))
    };
}

pub const ITALY: &str = "Italy";
pub const SINGAPORE: &str = "Singapore";

pub const DATE4: &str = "2015-10-04";
pub const DATE8: &str = "2015-10-08";
pub const DATE16: &str = "2015-10-16";

pub const FIELD_NULL: &Field = &Field::Null;

pub const FIELD_0_FLOAT: &Field = &Field::Float(OrderedFloat(0.0));
pub const FIELD_100_FLOAT: &Field = &Field::Float(OrderedFloat(100.0));
pub const FIELD_150_FLOAT: &Field = &Field::Float(OrderedFloat(150.0));
pub const FIELD_200_FLOAT: &Field = &Field::Float(OrderedFloat(200.0));
pub const FIELD_250_FLOAT: &Field = &Field::Float(OrderedFloat(250.0));
pub const FIELD_350_FLOAT: &Field = &Field::Float(OrderedFloat(350.0));
pub const FIELD_75_FLOAT: &Field = &Field::Float(OrderedFloat(75.0));
pub const FIELD_50_FLOAT: &Field = &Field::Float(OrderedFloat(50.0));
pub const FIELD_250_DIV_3_FLOAT: &Field = &Field::Float(OrderedFloat(250.0 / 3.0));
pub const FIELD_350_DIV_3_FLOAT: &Field = &Field::Float(OrderedFloat(350.0 / 3.0));

pub const FIELD_0_INT: &Field = &Field::Int(0);
pub const FIELD_1_INT: &Field = &Field::Int(1);
pub const FIELD_2_INT: &Field = &Field::Int(2);
pub const FIELD_3_INT: &Field = &Field::Int(3);
pub const FIELD_100_INT: &Field = &Field::Int(100);
pub const FIELD_150_INT: &Field = &Field::Int(150);
pub const FIELD_200_INT: &Field = &Field::Int(200);
pub const FIELD_250_INT: &Field = &Field::Int(250);
pub const FIELD_350_INT: &Field = &Field::Int(350);
pub const FIELD_50_INT: &Field = &Field::Int(50);

pub const FIELD_100_UINT: &Field = &Field::UInt(100);
pub const FIELD_150_UINT: &Field = &Field::UInt(150);
pub const FIELD_200_UINT: &Field = &Field::UInt(200);
pub const FIELD_250_UINT: &Field = &Field::UInt(250);
pub const FIELD_350_UINT: &Field = &Field::UInt(350);
pub const FIELD_50_UINT: &Field = &Field::UInt(50);
