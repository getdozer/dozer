use dozer_types::{
    chrono::{DateTime, FixedOffset},
    serde::{self, Deserialize, Serialize},
    types::{FieldDefinition, FieldType, Schema, SourceDefinition},
};
use mongodb::bson::doc;

#[derive(Debug, Serialize, Deserialize)]
#[serde(crate = "self::serde")]
pub struct Film {
    pub film_id: u64,
    pub title: String,
    pub description: String,
    pub release_year: u64,
    pub language_id: u64,
    pub original_language_id: Option<u64>,
    pub rental_duration: u64,
    pub rental_rate: f64,
    pub length: u64,
    pub replacement_cost: f64,
    pub rating: String,
    pub last_update: DateTime<FixedOffset>,
    pub special_features: String,
}

pub fn film_schema() -> Schema {
    let mut schema = Schema::default();
    schema
        .field(
            FieldDefinition::new(
                "film_id".to_string(),
                FieldType::UInt,
                false,
                SourceDefinition::Dynamic,
            ),
            true,
        )
        .field(
            FieldDefinition::new(
                "title".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "description".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "release_year".to_string(),
                FieldType::UInt,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "language_id".to_string(),
                FieldType::UInt,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "original_language_id".to_string(),
                FieldType::UInt,
                true,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "rental_duration".to_string(),
                FieldType::UInt,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "rental_rate".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "length".to_string(),
                FieldType::UInt,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "replacement_cost".to_string(),
                FieldType::Float,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "rating".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "last_update".to_string(),
                FieldType::Timestamp,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        )
        .field(
            FieldDefinition::new(
                "special_features".to_string(),
                FieldType::String,
                false,
                SourceDefinition::Dynamic,
            ),
            false,
        );
    schema
}
