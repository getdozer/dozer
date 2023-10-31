use std::sync::Arc;

use dozer_log::{
    reader::LogReaderOptions,
    tokio::{self, sync::Mutex},
};
use dozer_types::{
    grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient,
    thiserror, tonic,
};

use self::{trigger::Trigger, worker::Worker};

#[derive(Debug)]
pub struct Runtime {
    trigger: Arc<Mutex<Trigger>>,
    worker: Worker,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to connect to app server at {0}: {1}")]
    Connect(String, #[source] tonic::transport::Error),
    #[error("failed to create worker: {0}")]
    CreateWorker(#[from] worker::Error),
}

impl Runtime {
    pub async fn new(
        runtime: Arc<tokio::runtime::Runtime>,
        app_url: String,
        registration_scripts: Vec<String>,
        options: LogReaderOptions,
    ) -> Result<Self, Error> {
        let client = InternalPipelineServiceClient::connect(app_url.clone())
            .await
            .map_err(|e| Error::Connect(app_url, e))?;
        let trigger = Arc::new(Mutex::new(Trigger::new(client, options)));
        let worker = Worker::new(runtime, trigger.clone(), registration_scripts).await?;
        Ok(Self { trigger, worker })
    }

    pub async fn run(self) {
        let mut trigger = self.trigger.lock().await;
        trigger.run(&self.worker).await;
    }
}

#[cfg(test)]
mod tests;
mod trigger;
mod worker;
