use std::thread;

use crate::{
    connectors::Connector,
    ingestion::{IngestionConfig, Ingestor},
};
use dozer_types::grpc_types::ingest::{ingest_service_client::IngestServiceClient, IngestRequest};

use dozer_types::{
    ingestion_types::{GrpcConfig, GrpcConfigSchemas},
    serde_json::json,
};
use tokio::runtime::Runtime;

#[test]
fn ingest_grpc() {
    let schemas = json!([{
      "name": "users",
      "schema": {
        "fields": [
          {
            "name": "id",
            "data_type": "Int64",
            "nullable": false
          },
          {
            "name": "name",
            "data_type": "Utf8",
            "nullable": true
          }
        ]
      }
    }]);
    let (ingestor, mut iterator) = Ingestor::initialize_channel(IngestionConfig::default());

    std::thread::spawn(move || {
        let grpc_connector = super::connector::GrpcConnector::new(
            1,
            "grpc".to_string(),
            GrpcConfig {
                schemas: Some(GrpcConfigSchemas::Inline(schemas.to_string())),
                ..Default::default()
            },
        );
        grpc_connector.start(None, &ingestor, None).unwrap();
    });

    Runtime::new().unwrap().block_on(async {
        let retries = 10;
        let mut res = IngestServiceClient::connect("http://[::1]:8085").await;
        for r in 0..retries {
            if res.is_ok() {
                break;
            }
            if r == retries - 1 {
                panic!("failed to connect after {r} times");
            }
            thread::sleep(std::time::Duration::from_millis(300));
            res = IngestServiceClient::connect("http://0.0.0.0:8085").await;
        }

        let mut ingest_client = res.unwrap();

        // Ingest a record
        let res = ingest_client
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
    });

    while let Some((_, op)) = iterator.next() {}
}
