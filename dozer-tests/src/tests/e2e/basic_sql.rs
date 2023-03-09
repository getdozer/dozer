use std::{
    sync::{atomic::AtomicBool, Arc},
    thread,
};

use dozer_api::actix_web::rt::Runtime;
use dozer_orchestrator::{simple::SimpleOrchestrator as Dozer, Orchestrator};
use dozer_types::grpc_types::common::{
    common_grpc_service_client::CommonGrpcServiceClient, QueryRequest,
};
use dozer_types::{models::app_config::Config, serde_yaml};
use tempdir::TempDir;
fn get_config() -> Config {
    let path = TempDir::new("tests").unwrap();
    let mut cfg =
        serde_yaml::from_str::<Config>(include_str!("./fixtures/basic_sql.yaml")).unwrap();
    cfg.home_dir = path.path().to_str().unwrap().to_string();
    cfg
}

#[test]
fn test_e2e_sql() {
    let cfg = get_config();
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    // Start Dozer
    thread::spawn(move || {
        let mut dozer = Dozer::new(cfg);
        dozer.run_all(running).unwrap();
    });
    let retries = 10;
    let url = "http://0.0.0.0:7503";
    Runtime::new().unwrap().block_on(async {
        // Query common service
        let mut res = CommonGrpcServiceClient::connect(url).await;
        for r in 0..retries {
            if res.is_ok() {
                break;
            }
            if r == retries - 1 {
                panic!("failed to connect after {r} times");
            }
            thread::sleep(std::time::Duration::from_millis(500));
            res = CommonGrpcServiceClient::connect(url).await;
        }

        thread::sleep(std::time::Duration::from_millis(1000));
        let mut common_client = res.unwrap();

        let res = common_client
            .query(QueryRequest {
                endpoint: "trips".to_string(),
                query: None,
            })
            .await
            .unwrap();
        let res = res.into_inner();
        assert_eq!(res.records.len(), 4);
    });

    r.store(false, std::sync::atomic::Ordering::Relaxed);
}
