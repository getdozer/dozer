use bson::doc;
use csv::StringRecord;
use dozer_cache::cache::{LmdbRwCacheManager, RoCache, RoCacheManager, RwCacheManager};
use dozer_tracing::Labels;
use dozer_types::{chrono::DateTime, types::IndexDefinition};
use mongodb::{options::ClientOptions, Client, Collection, IndexModel};

use crate::{cache_tests::string_record_to_record, init::init, read_csv::read_csv};

use super::{film_schema, Film};

pub async fn load_database(
    secondary_indexes: Vec<IndexDefinition>,
) -> (Box<dyn RoCache>, Collection<Film>) {
    // Initialize tracing and data.
    init();

    // Create cache and insert schema.
    let schema = film_schema();
    let cache_manager = LmdbRwCacheManager::new(Default::default()).unwrap();
    let labels = Labels::default();
    let mut cache = cache_manager
        .create_cache(
            labels.clone(),
            schema.clone(),
            secondary_indexes,
            &Default::default(),
            Default::default(),
        )
        .unwrap();

    // Connect to mongodb and clear collection.
    let mongo_options = ClientOptions::parse("mongodb://localhost:27017")
        .await
        .unwrap();
    let mongo_client = Client::with_options(mongo_options).unwrap();
    let mongo_db = mongo_client.database("dozer-tests:cache");
    let mongo_collection = mongo_db.collection::<Film>("films");
    mongo_collection.delete_many(doc! {}, None).await.unwrap();
    let _ignore_error = mongo_collection.drop_indexes(None).await;

    mongo_collection
        .create_index(
            IndexModel::builder()
                .keys(doc! {"special_features": "text"})
                .build(),
            None,
        )
        .await
        .unwrap();

    // Create reader.
    let mut csv = read_csv("actor", "film").unwrap();

    // Insert records into cache and mongodb.
    for record in csv.records() {
        let record = record.unwrap();

        cache
            .insert(&string_record_to_record(&record, &schema))
            .unwrap();

        mongo_collection
            .insert_one(string_record_to_film(&record), None)
            .await
            .unwrap();
    }
    cache.commit(&Default::default()).unwrap();
    cache_manager.wait_until_indexing_catchup();

    drop(cache);
    (
        cache_manager.open_ro_cache(labels).unwrap().unwrap(),
        mongo_collection,
    )
}

fn string_record_to_film(record: &StringRecord) -> Film {
    let mut record_iter = record.iter();
    Film {
        film_id: record_iter.next().unwrap().parse().unwrap(),
        title: record_iter.next().unwrap().to_string(),
        description: record_iter.next().unwrap().to_string(),
        release_year: record_iter.next().unwrap().parse().unwrap(),
        language_id: record_iter.next().unwrap().parse().unwrap(),
        original_language_id: match record_iter.next().unwrap() {
            "" => None,
            value => Some(value.parse().unwrap()),
        },
        rental_duration: record_iter.next().unwrap().parse().unwrap(),
        rental_rate: record_iter.next().unwrap().parse().unwrap(),
        length: record_iter.next().unwrap().parse().unwrap(),
        replacement_cost: record_iter.next().unwrap().parse().unwrap(),
        rating: record_iter.next().unwrap().to_string(),
        last_update: DateTime::parse_from_str(record_iter.next().unwrap(), "%F %T%.6f%#z").unwrap(),
        special_features: record_iter.next().unwrap().to_string(),
    }
}
