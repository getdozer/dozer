use dozer_types::serde_json::{json, Value};
use dozer_types::types::{Field, Record};
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{
        FieldDefinition, FieldType, IndexDefinition, Schema, SchemaIdentifier,
        SortDirection::Ascending,
    },
};
use std::sync::Arc;

use dozer_cache::cache::{Cache, LmdbCache};

pub fn get_schema() -> Schema {
    let fields = vec![
        FieldDefinition {
            name: "film_id".to_string(),
            typ: FieldType::Int,
            nullable: true,
        },
        FieldDefinition {
            name: "description".to_string(),
            typ: FieldType::String,
            nullable: true,
        },
        FieldDefinition {
            name: "rental_rate".to_string(),
            typ: FieldType::Null,
            nullable: true,
        },
        FieldDefinition {
            name: "release_year".to_string(),
            typ: FieldType::Int,
            nullable: true,
        },
    ];
    let secondary_indexes = fields
        .iter()
        .enumerate()
        .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![(idx, Ascending)]))
        .collect();
    Schema {
        identifier: Some(SchemaIdentifier {
            id: 3003108387,
            version: 1,
        }),
        fields,
        values: vec![],
        primary_index: vec![0],
        secondary_indexes,
    }
}

pub fn get_schema_with_timestamp() -> Schema {
    let fields = vec![
        FieldDefinition {
            name: "film_id".to_string(),
            typ: FieldType::Int,
            nullable: true,
        },
        FieldDefinition {
            name: "description".to_string(),
            typ: FieldType::String,
            nullable: true,
        },
        FieldDefinition {
            name: "rental_rate".to_string(),
            typ: FieldType::Null,
            nullable: true,
        },
        FieldDefinition {
            name: "release_year".to_string(),
            typ: FieldType::Int,
            nullable: true,
        },
        FieldDefinition {
            name: "created_at".to_string(),
            typ: FieldType::Timestamp,
            nullable: true,
        },
    ];
    let secondary_indexes = fields
        .iter()
        .enumerate()
        .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![(idx, Ascending)]))
        .collect();
    Schema {
        identifier: Some(SchemaIdentifier {
            id: 3003108387,
            version: 1,
        }),
        fields,
        values: vec![],
        primary_index: vec![0],
        secondary_indexes,
    }
}

pub fn get_endpoint() -> ApiEndpoint {
    ApiEndpoint {
        id: None,
        name: "films".to_string(),
        path: "/films".to_string(),
        enable_rest: false,
        enable_grpc: true,
        sql: "select film_id, description, rental_rate, release_year from film where 1=1;"
            .to_string(),
        index: ApiIndex {
            primary_key: vec!["film_id".to_string()],
        },
    }
}

pub fn get_films() -> Vec<Value> {
    vec![
        json!({
          "description": "A Amazing Panorama of a Mad Scientist And a Husband who must Meet a Woman in The Outback",
          "rental_rate": null,
          "release_year": 2006,
          "film_id": 268
        }),
        json!({
          "film_id": 524,
          "release_year": 2006,
          "rental_rate": null,
          "description": "A Intrepid Display of a Pastry Chef And a Cat who must Kill a A Shark in Ancient China"
        }),
    ]
}

pub fn initialize_cache(schema_name: &str) -> Arc<LmdbCache> {
    let cache = Arc::new(LmdbCache::new(true));
    let schema: dozer_types::types::Schema = get_schema();
    let records_value: Vec<Value> = get_films();
    for record_str in records_value {
        let film_id = record_str["film_id"].as_i64();
        let description = record_str["description"].as_str();
        let release_year = record_str["release_year"].as_i64();
        if let (Some(film_id), Some(description), Some(release_year)) =
            (film_id, description, release_year)
        {
            let record = Record::new(
                schema.identifier.clone(),
                vec![
                    Field::Int(film_id),
                    Field::String(description.to_string()),
                    Field::Null,
                    Field::Int(release_year),
                ],
            );
            cache.insert_schema(schema_name, &schema).unwrap();
            cache.insert(&record).unwrap();
        }
    }
    cache
}
