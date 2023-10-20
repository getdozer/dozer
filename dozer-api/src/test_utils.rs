use dozer_tracing::Labels;
use dozer_types::serde_json::{json, Value};
use dozer_types::types::{Field, Record, SchemaWithIndex, SourceDefinition};
use dozer_types::{
    models::api_endpoint::{ApiEndpoint, ApiIndex},
    types::{FieldDefinition, FieldType, IndexDefinition, Schema},
};

use dozer_cache::cache::{CacheRecord, LmdbRwCacheManager, RwCacheManager};

pub fn get_schema() -> SchemaWithIndex {
    let fields = vec![
        FieldDefinition {
            name: "film_id".to_string(),
            typ: FieldType::UInt,
            nullable: false,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "description".to_string(),
            typ: FieldType::String,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "rental_rate".to_string(),
            typ: FieldType::Float,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "release_year".to_string(),
            typ: FieldType::UInt,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
        FieldDefinition {
            name: "updated_at".to_string(),
            typ: FieldType::Timestamp,
            nullable: true,
            source: SourceDefinition::Dynamic,
        },
    ];
    let secondary_indexes = fields
        .iter()
        .enumerate()
        .map(|(idx, _f)| IndexDefinition::SortedInverted(vec![idx]))
        .collect();
    (
        Schema {
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
        index: ApiIndex {
            primary_key: vec!["film_id".to_string()],
            secondary: Default::default(),
        },
        table_name: "film".to_string(),
        conflict_resolution: Default::default(),
        log_reader_options: Default::default(),
        version: None,
    }
}

fn get_films() -> Vec<Value> {
    let mut result = vec![
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
    ];

    for film_id in 1..=50 {
        result.push(json!({
            "film_id": film_id,
            "description": format!("Film {film_id}"),
            "rental_rate": null,
            "release_year": 2006,
            "updated_at": null
        }));
    }
    result
}

pub fn initialize_cache(
    schema_name: &str,
    schema: Option<SchemaWithIndex>,
) -> Box<LmdbRwCacheManager> {
    let cache_manager = LmdbRwCacheManager::new(Default::default()).unwrap();
    let mut labels = Labels::new();
    labels.push(schema_name.to_string(), schema_name.to_string());
    let (schema, secondary_indexes) = schema.unwrap_or_else(get_schema);
    let mut cache = cache_manager
        .create_cache(
            labels.to_non_empty_string().into_owned(),
            labels,
            (schema, secondary_indexes),
            &Default::default(),
            Default::default(),
        )
        .unwrap();
    let records = get_sample_records();
    for record in records {
        cache.insert(&record.record).unwrap();
    }
    cache.commit(&Default::default()).unwrap();
    cache_manager.wait_until_indexing_catchup();

    Box::new(cache_manager)
}

pub fn get_sample_records() -> Vec<CacheRecord> {
    let records_value: Vec<Value> = get_films();
    let mut records = vec![];
    for (record_index, record_str) in records_value.into_iter().enumerate() {
        let film_id = record_str["film_id"].as_u64();
        let description = record_str["description"].as_str();
        let release_year = record_str["release_year"].as_u64();
        if let (Some(film_id), Some(description), Some(release_year)) =
            (film_id, description, release_year)
        {
            let record = Record::new(vec![
                Field::UInt(film_id),
                Field::String(description.to_string()),
                Field::Null,
                Field::UInt(release_year),
                Field::Null,
            ]);
            records.push(CacheRecord::new(record_index as _, 1, record));
        }
    }
    records
}
