use std::{sync::Arc, thread};

use crate::ingestion::{IngestionConfig, IngestionIterator, Ingestor};
use dozer_types::{
    arrow::array::{Int32Array, StringArray},
    grpc_types::{
        ingest::{ingest_service_client::IngestServiceClient, IngestArrowRequest, IngestRequest},
        types,
    },
    ingestion_types::IngestionMessageKind,
    models::connection::{Connection, ConnectionConfig},
    serde_json::Value,
    types::Operation,
};
use dozer_types::{
    arrow::{datatypes as arrow_types, record_batch::RecordBatch},
    arrow_types::from_arrow::serialize_record_batch,
};

use dozer_types::{
    ingestion_types::{GrpcConfig, GrpcConfigSchemas},
    serde_json::json,
};
use tonic::transport::Channel;

async fn ingest_grpc(
    schemas: Value,
    adapter: String,
    port: u32,
) -> (IngestServiceClient<Channel>, IngestionIterator) {
    let (ingestor, iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    tokio::spawn(async move {
        let grpc_connector = crate::connectors::get_connector(Connection {
            config: Some(ConnectionConfig::Grpc(GrpcConfig {
                schemas: Some(GrpcConfigSchemas::Inline(schemas.to_string())),
                adapter,
                port,
                ..Default::default()
            })),
            name: "grpc".to_string(),
        })
        .unwrap();

        let tables = grpc_connector
            .list_columns(grpc_connector.list_tables().await.unwrap())
            .await
            .unwrap();
        grpc_connector.start(&ingestor, tables).await.unwrap();
    });

    let retries = 10;
    let url = format!("http://0.0.0.0:{port}");
    let mut res = IngestServiceClient::connect(url.clone()).await;
    for r in 0..retries {
        if res.is_ok() {
            break;
        }
        if r == retries - 1 {
            panic!("failed to connect after {r} times");
        }
        thread::sleep(std::time::Duration::from_millis(300));
        res = IngestServiceClient::connect(url.clone()).await;
    }

    (res.unwrap(), iterator)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn ingest_grpc_default() {
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
        ingest_grpc(schemas, "default".to_string(), 45678).await;

    // Ingest a record
    ingest_client
        .ingest(IngestRequest {
            schema_name: "users".to_string(),
            new: Some(types::Record {
                values: vec![
                    types::Value {
                        value: Some(types::value::Value::IntValue(1675)),
                    },
                    types::Value {
                        value: Some(types::value::Value::StringValue("dario".to_string())),
                    },
                ],
                version: 1,
            }),
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let msg = iterator.next().unwrap();
    assert!(msg.identifier.seq_in_tx == 1, "seq_no should be 1");

    if let IngestionMessageKind::OperationEvent(op) = msg.kind {
        if let Operation::Insert { new: record } = op {
            assert!(record.values[0].as_int() == Some(1675));
            assert!(record.values[1].as_string() == Some("dario"));
        } else {
            panic!("wrong operation kind");
        }
    } else {
        panic!("wrong message kind");
    }
}

#[tokio::test]
#[ignore]
async fn test_serialize_arrow_schema() {
    use std::println as info;
    let schema = arrow_types::Schema::new(vec![
        arrow_types::Field::new("id", arrow_types::DataType::Int32, false),
        arrow_types::Field::new(
            "time",
            arrow_types::DataType::Timestamp(
                arrow_types::TimeUnit::Millisecond,
                Some("SGT".to_string()),
            ),
            false,
        ),
    ]);

    let str = dozer_types::serde_json::to_string(&schema).unwrap();
    info!("{str}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn ingest_grpc_arrow() {
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
          }
        ],
        "metadata": {}
      }
    }]);

    let (mut ingest_client, mut iterator) = ingest_grpc(schemas, "arrow".to_string(), 45679).await;

    // Ingest a record
    let schema = arrow_types::Schema::new(vec![
        arrow_types::Field::new("id", arrow_types::DataType::Int32, false),
        arrow_types::Field::new("name", arrow_types::DataType::Utf8, false),
    ]);

    let a = Int32Array::from_iter([1675, 1676, 1677]);
    let b = StringArray::from_iter_values(vec!["dario", "mario", "vario"]);

    let record_batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]).unwrap();

    ingest_client
        .ingest_arrow(IngestArrowRequest {
            schema_name: "users".to_string(),
            records: serialize_record_batch(&record_batch),
            seq_no: 1,
            ..Default::default()
        })
        .await
        .unwrap();

    let msg = iterator.next().unwrap();
    assert!(msg.identifier.seq_in_tx == 1, "seq_no should be 1");

    if let IngestionMessageKind::OperationEvent(op) = msg.kind {
        if let Operation::Insert { new: record } = op {
            assert!(record.values[0].as_int() == Some(1675));
            assert!(record.values[1].as_string() == Some("dario"));
        } else {
            panic!("wrong operation kind");
        }
    } else {
        panic!("wrong message kind");
    }
}
