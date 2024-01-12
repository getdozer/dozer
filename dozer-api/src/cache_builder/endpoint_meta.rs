use dozer_cache::dozer_log::{reader::LogClient, schemas::EndpointSchema};
use dozer_tracing::Labels;
use dozer_types::{
    grpc_types::internal::internal_pipeline_service_client::InternalPipelineServiceClient,
    tonic::transport::Channel,
};

use crate::{cache_alias_and_labels, errors::ApiInitError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMeta {
    pub table_name: String,
    pub log_id: String,
    pub schema: EndpointSchema,
}

impl EndpointMeta {
    pub async fn load_from_client(
        client: &mut InternalPipelineServiceClient<Channel>,
        table_name: String,
    ) -> Result<(Self, LogClient), ApiInitError> {
        // We establish the log stream first to avoid tonic auto-reconnecting without us knowing.
        let (log_client, schema) = LogClient::new(client, table_name.clone()).await?;
        let log_id = client.get_id(()).await?.into_inner().id;

        Ok((
            Self {
                table_name,
                log_id,
                schema,
            },
            log_client,
        ))
    }

    pub fn cache_alias_and_labels(&self, extra_labels: Labels) -> (String, Labels) {
        let (alias, mut labels) = cache_alias_and_labels(self.table_name.clone());
        labels.extend(extra_labels);
        (alias, labels)
    }

    pub fn cache_name(&self) -> String {
        format!("{}_{}", self.log_id, self.table_name)
    }
}
