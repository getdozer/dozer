use std::collections::HashMap;
use std::{sync::Arc, thread};

use dozer_ingestion_connector::dozer_types::{
    arrow::array::{Int32Array, StringArray},
    arrow::{datatypes as arrow_types, record_batch::RecordBatch},
    arrow_types::from_arrow::serialize_record_batch,
    arrow_types::to_arrow::DOZER_SCHEMA_KEY,
    grpc_types::{
        ingest::{ingest_service_client::IngestServiceClient, IngestArrowRequest, IngestRequest},
        types,
    },
    json_types::json as dozer_json,
    models::ingestion_types::IngestionMessage,
    models::ingestion_types::{GrpcConfig, GrpcConfigSchemas},
    serde_json,
    serde_json::json,
    serde_json::Value,
    tonic::transport::Channel,
    types::Operation,
    types::{FieldDefinition, FieldType, Schema as DozerSchema, SourceDefinition},
};
use dozer_ingestion_connector::test_util::{create_test_runtime, spawn_connector_all_tables};
use dozer_ingestion_connector::tokio::runtime::Runtime;
use dozer_ingestion_connector::{dozer_types, IngestionIterator};

use crate::{ArrowAdapter, DefaultAdapter};

use super::connector::GrpcConnector;
use super::IngestAdapter;

fn ingest_grpc<T: IngestAdapter>(
    runtime: Arc<Runtime>,
    schemas: Value,
    adapter: String,
    port: u32,
) -> (IngestServiceClient<Channel>, IngestionIterator) {
    let grpc_connector = GrpcConnector::<T>::new(
        "grpc".to_string(),
        GrpcConfig {
            schemas: GrpcConfigSchemas::Inline(schemas.to_string()),
            adapter: Some(adapter),
            port: Some(port),
            host: None,
        },
    );

    let (iterator, _) = spawn_connector_all_tables(runtime.clone(), grpc_connector);

    let retries = 10;
    let url = format!("http://0.0.0.0:{port}");
    let mut res = runtime.block_on(IngestServiceClient::connect(url.clone()));
    for r in 0..retries {
        if res.is_ok() {
            break;
        }
        if r == retries - 1 {
            panic!("failed to connect after {r} times");
        }
        thread::sleep(std::time::Duration::from_millis(300));
        res = runtime.block_on(IngestServiceClient::connect(url.clone()));
    }

    (res.unwrap(), iterator)
}

#[test]
fn ingest_grpc_default() {
    let runtime = create_test_runtime();
    let schemas = json!({
      "users": {
        "schema": {
            "fields": [
            {
                "name": "id",
                "typ": "Int",
                "nullable": false
            },
            {
                "name": "name",
                "typ": "String",
                "nullable": true
            }
            ]
        }
        }
    });

    let (mut ingest_client, mut iterator) =
        ingest_grpc::<DefaultAdapter>(runtime.clone(), schemas, "default".to_string(), 45678);

    // Ingest a record
    runtime
        .block_on(ingest_client.ingest(IngestRequest {
            schema_name: "users".to_string(),
            new: vec![
                types::Value {
                    value: Some(types::value::Value::IntValue(1675)),
                },
                types::Value {
                    value: Some(types::value::Value::StringValue("dario".to_string())),
                },
            ],
            seq_no: 1,
            ..Default::default()
        }))
        .unwrap();

    let msg = iterator.next().unwrap();

    if let IngestionMessage::OperationEvent { op, .. } = msg {
        if let Operation::Insert { new: record } = op {
            assert_eq!(record.values[0].as_int(), Some(1675));
            assert_eq!(record.values[1].as_string(), Some("dario"));
        } else {
            panic!("wrong operation kind");
        }
    } else {
        panic!("wrong message kind");
    }
}

#[test]
#[ignore]
fn test_serialize_arrow_schema() {
    let schema = arrow_types::Schema::new(vec![
        arrow_types::Field::new("id", arrow_types::DataType::Int32, false),
        arrow_types::Field::new(
            "time",
            arrow_types::DataType::Timestamp(
                arrow_types::TimeUnit::Millisecond,
                Some("SGT".into()),
            ),
            false,
        ),
    ]);

    let str = dozer_types::serde_json::to_string(&schema).unwrap();
    println!("{str}");
}

#[test]
fn ingest_grpc_arrow() {
    let runtime = create_test_runtime();
    let schema_str = serde_json::to_string(
        &DozerSchema::default()
            .field(
                FieldDefinition::new(
                    "id".to_string(),
                    FieldType::Int,
                    false,
                    SourceDefinition::Dynamic,
                ),
                true,
            )
            .field(
                FieldDefinition::new(
                    "name".to_string(),
                    FieldType::String,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .field(
                FieldDefinition::new(
                    "json".to_string(),
                    FieldType::Json,
                    false,
                    SourceDefinition::Dynamic,
                ),
                false,
            )
            .clone(),
    )
    .expect("Schema can always be serialized as JSON");

    let schemas = json!([{
      "name": "users",
      "schema": {
        "fields": [
          {
            "name": "id",
            "data_type": "Int32",
            "nullable": false,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          },
          {
            "name": "name",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          },
          {
            "name": "json",
            "data_type": "Utf8",
            "nullable": true,
            "dict_id": 0,
            "dict_is_ordered": false,
            "metadata": {}
          }
        ],
        "metadata": { DOZER_SCHEMA_KEY.to_string() : schema_str }
      }
    }]);

    let (mut ingest_client, mut iterator) =
        ingest_grpc::<ArrowAdapter>(runtime.clone(), schemas, "arrow".to_string(), 45679);

    // Ingest a record
    let schema = arrow_types::Schema::new_with_metadata(
        vec![
            arrow_types::Field::new("id", arrow_types::DataType::Int32, false),
            arrow_types::Field::new("name", arrow_types::DataType::Utf8, false),
            arrow_types::Field::new("json", arrow_types::DataType::Utf8, false),
        ],
        HashMap::from([(DOZER_SCHEMA_KEY.to_string(), schema_str)]),
    );

    let a = Int32Array::from_iter([1675, 1676, 1677]);
    let b = StringArray::from_iter_values(vec!["dario", "mario", "vario"]);
    let c = StringArray::from_iter_values(vec!["[1, 2, 3]", "{\"a\": \"b\"}", "\"s\""]);

    let record_batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(a), Arc::new(b), Arc::new(c)],
    )
    .unwrap();

    runtime
        .block_on(ingest_client.ingest_arrow(IngestArrowRequest {
            schema_name: "users".to_string(),
            records: serialize_record_batch(&record_batch),
            seq_no: 1,
            ..Default::default()
        }))
        .unwrap();

    let msg = iterator.next().unwrap();

    if let IngestionMessage::OperationEvent { op, .. } = msg {
        if let Operation::Insert { new: record } = op {
            assert_eq!(record.values[0].as_int(), Some(1675));
            assert_eq!(record.values[1].as_string(), Some("dario"));
            assert_eq!(
                record.values[2].as_json(),
                Some(&dozer_json!([1_f64, 2_f64, 3_f64]))
            );
        } else {
            panic!("wrong operation kind");
        }
    } else {
        panic!("wrong message kind");
    }
}
