use std::{
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use dozer_api::{
    actix_web::rt::Runtime,
    grpc::common_grpc::common_grpc_service_client::CommonGrpcServiceClient,
    grpc::{common_grpc, types as common_types},
};
use dozer_orchestrator::{
    ingest_connector_types,
    ingest_grpc::{ingest_service_client::IngestServiceClient, IngestRequest},
    simple::SimpleOrchestrator as Dozer,
    Orchestrator,
};
use dozer_types::{models::app_config::Config, serde_yaml};
use tempdir::TempDir;
fn get_config() -> Config {
    let path = TempDir::new("tests").unwrap();
    let mut cfg = serde_yaml::from_str::<Config>(include_str!("./test.yaml")).unwrap();
    cfg.home_dir = path.path().to_str().unwrap().to_string();
    cfg
}

#[test]
fn ingest_and_test() {
    let cfg = get_config();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    // Start Dozer
    thread::spawn(move || {
        let mut dozer = Dozer::new(cfg);
        dozer.run_all(running).unwrap();
    });
    Runtime::new().unwrap().block_on(async {
        // Query Dozer
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
                new: Some(ingest_connector_types::Record {
                    values: vec![
                        ingest_connector_types::Value {
                            value: Some(ingest_connector_types::value::Value::IntValue(1675)),
                        },
                        ingest_connector_types::Value {
                            value: Some(ingest_connector_types::value::Value::StringValue(
                                "dario".to_string(),
                            )),
                        },
                    ],
                    version: 1,
                }),
                seq_no: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(res.into_inner().seq_no, 1);

        // wait for the record to be processed
        thread::sleep(std::time::Duration::from_millis(300));

        // Query common service
        let mut res = CommonGrpcServiceClient::connect("http://0.0.0.0:50051").await;
        for r in 0..retries {
            if res.is_ok() {
                break;
            }
            if r == retries - 1 {
                panic!("failed to connect after {r} times");
            }
            thread::sleep(std::time::Duration::from_millis(500));
            res = CommonGrpcServiceClient::connect("http://0.0.0.0:500051").await;
        }

        let mut common_client = res.unwrap();

        let res = common_client
            .query(common_grpc::QueryRequest {
                endpoint: "users".to_string(),
                query: None,
            })
            .await
            .unwrap();
        let res = res.into_inner();
        let rec = res.records.first().unwrap().clone();
        let val = rec.record.unwrap().values.first().unwrap().clone().value;
        assert!(matches!(val, Some(common_types::value::Value::IntValue(_))));

        if let Some(common_types::value::Value::IntValue(v)) = val {
            assert_eq!(v, 1675);
        }
    });

    r.store(false, std::sync::atomic::Ordering::Relaxed);
}
