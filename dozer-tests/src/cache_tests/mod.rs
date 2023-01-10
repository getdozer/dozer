use csv::StringRecord;
use dozer_types::{
    chrono::DateTime,
    types::{Field, FieldType, Record, Schema},
};

pub mod film;
pub mod filter;
pub mod order;
pub mod skip_and_limit;

pub fn string_record_to_record(record: &StringRecord, schema: &Schema) -> Record {
    let values = record
        .iter()
        .zip(&schema.fields)
        .map(|(value, field)| {
            if field.nullable && value.is_empty() {
                return Field::Null;
            }
            match field.typ {
                FieldType::UInt => Field::UInt(value.parse().unwrap()),
                FieldType::Int => Field::Int(value.parse().unwrap()),
                FieldType::Float => Field::Float(value.parse().unwrap()),
                FieldType::Boolean => Field::Boolean(value.parse().unwrap()),
                FieldType::String => Field::String(value.to_string()),
                FieldType::Text => Field::Text(value.to_string()),
                FieldType::Decimal => Field::Decimal(value.parse().unwrap()),
                FieldType::Date => Field::Date(value.parse().unwrap()),
                FieldType::Timestamp => {
                    Field::Timestamp(DateTime::parse_from_str(value, "%F %T%.6f%#z").unwrap())
                }
                _ => panic!("Unsupported field type"),
            }
        })
        .collect();

    Record::new(schema.identifier, values)
}
