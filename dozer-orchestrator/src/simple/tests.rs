use std::{
    fs,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use dozer_api::CacheEndpoint;
use dozer_cache::cache::{expression::QueryExpression, test_utils, Cache, CacheOptions, LmdbCache};
use dozer_ingestion::ingestion::{IngestionConfig, Ingestor};
use dozer_types::{
    ingestion_types::IngestionMessage,
    models::{
        self,
        api_endpoint::{ApiEndpoint, ApiIndex},
    },
    types::{Field, OperationEvent, Record, Schema},
};
use log::warn;
use serde_json::{json, Value};
use tempdir::TempDir;

use super::executor::{Executor, SinkConfig};

fn single_source_sink_impl(schema: Schema) {
    let source = models::source::Source {
        id: Some("1".to_string()),
        name: "events".to_string(),
        table_name: "events".to_string(),
        columns: None,
        connection: models::connection::Connection {
            db_type: models::connection::DBType::Events,
            authentication: models::connection::Authentication::Events {},
            name: "events".to_string(),
            id: Some("1".to_string()),
        },
        history_type: None,
        refresh_config: models::source::RefreshConfig::RealTime,
    };

    let table_name = "events";
    let cache = Arc::new(LmdbCache::new(CacheOptions::default()).unwrap());
    let cache_endpoint = CacheEndpoint {
        cache: cache.clone(),
        endpoint: ApiEndpoint {
            id: Some("1".to_string()),
            name: table_name.to_string(),
            path: "/events".to_string(),
            sql: "select a, b from events group by a,b;".to_string(),
            index: ApiIndex {
                primary_key: vec!["a".to_string()],
            },
        },
    };

    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    let ingestor2 = ingestor.clone();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let executor_running = running.clone();

    // Initialize a schema.
    ingestor2
        .write()
        .handle_message((
            1,
            IngestionMessage::Schema(table_name.to_string(), schema.clone()),
        ))
        .unwrap();

    let items: Vec<(i64, String, i64)> = vec![
        (1, "yuri".to_string(), 521),
        (2, "mega".to_string(), 521),
        (3, "james".to_string(), 523),
        (4, "james".to_string(), 524),
        (5, "steff".to_string(), 526),
        (6, "mega".to_string(), 527),
        (7, "james".to_string(), 528),
    ];

    let tmp_dir = TempDir::new("example").unwrap_or_else(|_e| panic!("Unable to create temp dir"));
    if tmp_dir.path().exists() {
        fs::remove_dir_all(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to remove old dir"));
    }
    fs::create_dir(tmp_dir.path()).unwrap_or_else(|_e| panic!("Unable to create temp dir"));

    let tmp_path = tmp_dir.path().to_owned();
    let _thread = thread::spawn(move || {
        let executor = Executor::new(
            vec![source],
            vec![cache_endpoint],
            ingestor,
            iterator,
            executor_running,
            tmp_path,
            SinkConfig {
                record_cutoff: 1,
                timeout: 1,
            },
        );
        match executor.run(None, running) {
            Ok(_) => {}
            Err(e) => warn!("Exiting: {:?}", e),
        }
    });

    // Insert each record and query cache
    for (a, b, c) in items {
        let record = Record::new(
            schema.identifier.clone(),
            vec![Field::Int(a), Field::String(b), Field::Int(c)],
        );
        ingestor2
            .write()
            .handle_message((
                1,
                IngestionMessage::OperationEvent(OperationEvent {
                    seq_no: a as u64,
                    operation: dozer_types::types::Operation::Insert { new: record },
                }),
            ))
            .unwrap();
    }

    // Allow for the thread to process the records
    thread::sleep(Duration::from_millis(3000));
    //Shutdown the thread
    r.store(false, Ordering::SeqCst);

    test_query("events".to_string(), json!({}), 7, &cache);
}

#[test]
fn single_source_sink() {
    let mut schema = test_utils::schema_1().0;
    single_source_sink_impl(schema.clone());
    schema.primary_index.clear();
    single_source_sink_impl(schema);
}

fn test_query(schema_name: String, query: Value, count: usize, cache: &LmdbCache) {
    let query = serde_json::from_value::<QueryExpression>(query).unwrap();
    let records = cache.query(&schema_name, &query).unwrap();

    assert_eq!(records.len(), count, "Count must be equal : {:?}", query);
}
