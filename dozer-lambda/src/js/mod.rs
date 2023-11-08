use std::sync::Arc;

use dozer_log::{
    errors::ReaderBuilderError,
    reader::LogReaderOptions,
    tokio::{self, sync::Mutex},
};
use dozer_types::{
    grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient,
    models::lambda_config::JavaScriptLambda, thiserror, tonic,
};

use self::{trigger::Trigger, worker::Worker};

#[derive(Debug)]
pub struct Runtime {
    trigger: Trigger,
    worker: Arc<Mutex<Worker>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to connect to app server at {0}: {1}")]
    Connect(String, #[source] tonic::transport::Error),
    #[error("failed to create worker: {0}")]
    CreateWorker(#[from] dozer_deno::RuntimeError),
    #[error("failed to connect to log endpoint {0}: {1}")]
    ConnectLog(String, #[source] ReaderBuilderError),
}

impl Runtime {
    pub async fn new(
        runtime: Arc<tokio::runtime::Runtime>,
        app_url: String,
        lambda_modules: Vec<JavaScriptLambda>,
        options: LogReaderOptions,
    ) -> Result<Self, Error> {
        // Create worker.
        let modules = lambda_modules
            .iter()
            .map(|module| module.module.clone())
            .collect();
        let (worker, lambdas) = Worker::new(runtime, modules).await?;

        // Create trigger.
        let client = InternalPipelineServiceClient::connect(app_url.clone())
            .await
            .map_err(|e| Error::Connect(app_url, e))?;
        let mut trigger = Trigger::new(client, options);

        // Add lambdas to trigger.
        for (module, lambda) in lambda_modules.into_iter().zip(lambdas) {
            trigger
                .add_lambda(module.endpoint.clone(), lambda)
                .await
                .map_err(|e| Error::ConnectLog(module.endpoint, e))?;
        }
        Ok(Self {
            trigger,
            worker: Arc::new(Mutex::new(worker)),
        })
    }

    pub async fn run(mut self) {
        self.trigger.run(&self.worker).await;
    }
}

#[cfg(test)]
mod tests;
mod trigger;
mod worker;
