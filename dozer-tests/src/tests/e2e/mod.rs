use std::{future::Future, sync::Arc, thread::JoinHandle, time::Duration};

use dozer_api::tonic::transport::Channel;
use dozer_cli::{
    shutdown::{self, ShutdownSender},
    simple::SimpleOrchestrator,
};
use dozer_types::{
    grpc_types::{
        common::common_grpc_service_client::CommonGrpcServiceClient,
        ingest::ingest_service_client::IngestServiceClient,
    },
    models::{api_config::default_api_grpc, config::Config, connection::ConnectionConfig},
    serde_yaml,
};
use tempdir::TempDir;
use tokio::runtime::Runtime;

mod basic;
mod basic_sql;
mod basic_sql_wildcard;
mod left_join;

struct DozerE2eTest {
    _home_dir: TempDir,
    dozer_thread: Option<(ShutdownSender, JoinHandle<()>)>,

    common_service_client: CommonGrpcServiceClient<Channel>,
    ingest_service_client: Option<IngestServiceClient<Channel>>,
}

impl DozerE2eTest {
    async fn new(config_str: &str) -> Self {
        let temp_dir = TempDir::new("tests").unwrap();
        let mut config = serde_yaml::from_str::<Config>(config_str).unwrap();
        config.home_dir = temp_dir.path().to_str().unwrap().to_string();
        config.cache_dir = temp_dir.path().join("cache").to_str().unwrap().to_string();

        let api_grpc = config
            .api
            .as_ref()
            .and_then(|api| api.grpc.clone())
            .unwrap_or_else(default_api_grpc);
        let common_service_url = format!("http://{}:{}", api_grpc.host, api_grpc.port);

        let mut ingest_service_url = None;
        for connection in &config.connections {
            if let Some(ConnectionConfig::Grpc(config)) = &connection.config {
                if ingest_service_url.is_some() {
                    panic!("Found more than one ingest service");
                }
                ingest_service_url = Some(format!("http://{}:{}", config.host, config.port));
            }
        }

        let runtime = Runtime::new().expect("Failed to create runtime");
        let mut dozer = SimpleOrchestrator::new(config, Arc::new(runtime));
        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        let dozer_thread = std::thread::spawn(move || {
            dozer.run_all(shutdown_receiver).unwrap();
        });

        let num_retries = 10;
        let retry_interval = Duration::from_millis(300);
        let common_service_client = retry_async_fn(num_retries, retry_interval, || {
            CommonGrpcServiceClient::connect(common_service_url.clone())
        })
        .await;
        let ingest_service_client = if let Some(ingest_service_url) = ingest_service_url {
            Some(
                retry_async_fn(num_retries, retry_interval, || {
                    IngestServiceClient::connect(ingest_service_url.clone())
                })
                .await,
            )
        } else {
            None
        };

        Self {
            _home_dir: temp_dir,
            dozer_thread: Some((shutdown_sender, dozer_thread)),
            common_service_client,
            ingest_service_client,
        }
    }
}

impl Drop for DozerE2eTest {
    fn drop(&mut self) {
        let (shutdown, join_handle) = self.dozer_thread.take().unwrap();
        shutdown.shutdown();
        join_handle.join().unwrap();
    }
}

async fn retry_async_fn<T, E, F: Future<Output = Result<T, E>>>(
    num_retries: u8,
    retry_interval: Duration,
    f: impl Fn() -> F,
) -> T {
    for _ in 0..num_retries {
        let result = f().await;
        if let Ok(result) = result {
            return result;
        }
        tokio::time::sleep(retry_interval).await;
    }
    panic!("failed to connect after {num_retries} times");
}
