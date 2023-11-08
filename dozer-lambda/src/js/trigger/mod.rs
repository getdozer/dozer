use std::{num::NonZeroI32, sync::Arc, time::Duration};

use dozer_deno::deno_runtime::deno_core::futures::future::join_all;
use dozer_log::{
    errors::{ReaderBuilderError, ReaderError},
    reader::{LogClient, LogReader, LogReaderOptions},
    replication::LogOperation,
    tokio::{self, sync::Mutex},
};
use dozer_types::{
    grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient,
    log::{error, trace},
    tonic::transport::Channel,
};

use super::worker::Worker;

#[derive(Debug)]
pub struct Trigger {
    client: InternalPipelineServiceClient<Channel>,
    options: LogReaderOptions,
    lambdas: Vec<(LogReader, NonZeroI32)>,
}

impl Trigger {
    pub fn new(client: InternalPipelineServiceClient<Channel>, options: LogReaderOptions) -> Self {
        Self {
            client,
            options,
            lambdas: vec![],
        }
    }

    pub async fn add_lambda(
        &mut self,
        endpoint: String,
        lambda: NonZeroI32,
    ) -> Result<(), ReaderBuilderError> {
        let (client, schema) = LogClient::new(&mut self.client, endpoint).await?;
        let reader = LogReader::new(schema, client, self.options.clone(), 0);
        self.lambdas.push((reader, lambda));
        Ok(())
    }

    pub async fn run(&mut self, worker: &Arc<Mutex<Worker>>) {
        let lambdas = std::mem::take(&mut self.lambdas);
        let handles = lambdas
            .into_iter()
            .map(|(reader, lambda)| trigger_loop(reader, worker.clone(), lambda))
            .collect::<Vec<_>>();
        join_all(handles).await;
    }
}

async fn trigger_loop(mut log_reader: LogReader, worker: Arc<Mutex<Worker>>, func: NonZeroI32) {
    loop {
        if let Err(e) = trigger_once(&mut log_reader, &worker, func).await {
            const RETRY_INTERVAL: Duration = Duration::from_secs(5);
            error!("error reading log: {}. Retrying in {:?}", e, RETRY_INTERVAL);
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }
}

async fn trigger_once(
    log_reader: &mut LogReader,
    worker: &Mutex<Worker>,
    func: NonZeroI32,
) -> Result<(), ReaderError> {
    let op_and_pos = log_reader.read_one().await?;
    if let LogOperation::Op { op } = op_and_pos.op {
        let field_names = log_reader
            .schema
            .schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>();
        trace!(
            "triggering lambda {} with op position {} from endpoint {}",
            func,
            op_and_pos.pos,
            log_reader.schema.path
        );
        worker
            .lock()
            .await
            .call_lambda(func, op_and_pos.pos, op, field_names)
            .await
    }
    Ok(())
}
