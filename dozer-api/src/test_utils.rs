use dozer_types::serde_json::{json, Value};
use dozer_types::types::{Field, Record};
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier},
};
use std::sync::Arc;

use dozer_cache::cache::{Cache, CacheOptions, LmdbCache};

pub fn get_schema() -> (Schema, Vec<IndexDefinition>) {
    let fields = vec![
        FieldDefinition {
            name: "film_id".to_string(),
            typ: FieldType::UInt,
            nullable: false,
        },
        FieldDefinition {
            name: "description".to_string(),
            typ: FieldType::String,
            nullable: true,
        },
        FieldDefinition {
            name: "rental_rate".to_string(),
            typ: FieldType::Float,
            nullable: true,
        },
        FieldDefinition {
            name: "release_year".to_string(),
            typ: FieldType::UInt,
            nullable: true,
        },
        FieldDefinition {
            name: "updated_at".to_string(),
            typ: FieldType::Timestamp,
            nullable: true,
        },
    ];
    let secondary_indexes = fields
        .iter()
        .enumerate()
        .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![idx]))
        .collect();
    (
        Schema {
            identifier: Some(SchemaIdentifier {
                id: 3003108387,
                version: 1,
            }),
            fields,
            primary_index: vec![0],
        },
        secondary_indexes,
    )
}

pub fn get_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        name: "films".to_string(),
        path: "/films".to_string(),
        sql: "select film_id, description, rental_rate, release_year, updated_at from film where 1=1;"
            .to_string(),
        index: Some(ApiIndex {
            primary_key: vec!["film_id".to_string()],
        }),
        ..Default::default()
    }
}

pub fn get_films() -> Vec<Value> {
    vec![
        json!({
          "description": "A Amazing Panorama of a Mad Scientist And a Husband who must Meet a Woman in The Outback",
          "rental_rate": null,
          "release_year": 2006,
          "film_id": 268,
          "updated_at": null
        }),
        json!({
          "film_id": 524,
          "release_year": 2006,
          "rental_rate": null,
          "description": "A Intrepid Display of a Pastry Chef And a Cat who must Kill a A Shark in Ancient China",
          "updated_at": null
        }),
    ]
}

pub fn initialize_cache(
    schema_name: &str,
    schema: Option<(dozer_types::types::Schema, Vec<IndexDefinition>)>,
) -> Arc<LmdbCache> {
    let cache = Arc::new(LmdbCache::new(CacheOptions::default()).unwrap());
    let (schema, secondary_indexes) = schema.unwrap_or_else(get_schema);
    cache
        .insert_schema(schema_name, &schema, &secondary_indexes)
        .unwrap();
    let records = get_sample_records(schema);
    for record in records {
        cache.insert(&record).unwrap();
    }
    cache
}

pub fn get_sample_records(schema: Schema) -> Vec<Record> {
    let records_value: Vec<Value> = get_films();
    let mut records = vec![];
    for record_str in records_value {
        let film_id = record_str["film_id"].as_u64();
        let description = record_str["description"].as_str();
        let release_year = record_str["release_year"].as_u64();
        if let (Some(film_id), Some(description), Some(release_year)) =
            (film_id, description, release_year)
        {
            let record = Record::new(
                schema.identifier,
                vec![
                    Field::UInt(film_id),
                    Field::String(description.to_string()),
                    Field::Null,
                    Field::UInt(release_year),
                    Field::Null,
                ],
                None,
            );
            records.push(record);
        }
    }
    records
}
