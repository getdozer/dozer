use crate::pipeline::aggregation::processor::FieldRule;
use crate::pipeline::aggregation::sum::IntegerSumAggregator;
use dozer_types::types::{Field, FieldDefinition, FieldType, Record, Schema};

pub fn get_input_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("City".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("Region".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("Country".to_string(), FieldType::String, false),
            true,
            false,
        )
        .field(
            FieldDefinition::new("People".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .clone()
}

pub fn get_expected_schema() -> Schema {
    Schema::empty()
        .field(
            FieldDefinition::new("City".to_string(), FieldType::String, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("Country".to_string(), FieldType::String, false),
            true,
            true,
        )
        .field(
            FieldDefinition::new("Total".to_string(), FieldType::Int, false),
            true,
            false,
        )
        .clone()
}

pub fn get_aggregator_rules() -> Vec<FieldRule> {
    vec![
        FieldRule::Dimension("City".to_string(), true, None),
        FieldRule::Dimension("Country".to_string(), true, None),
        FieldRule::Measure(
            "People".to_string(),
            Box::new(IntegerSumAggregator::new()),
            true,
            Some("Total".to_string()),
        ),
    ]
}

pub fn gen_in_data(city: &str, region: &str, country: &str, people: i64) -> Record {
    Record::new(
        None,
        vec![
            Field::String(city.to_string()),
            Field::String(region.to_string()),
            Field::String(country.to_string()),
            Field::Int(people),
        ],
    )
}

pub fn gen_out_data(city: &str, country: &str, tot: i64) -> Record {
    Record::new(
        None,
        vec![
            Field::String(city.to_string()),
            Field::String(country.to_string()),
            Field::Int(tot),
        ],
    )
}
