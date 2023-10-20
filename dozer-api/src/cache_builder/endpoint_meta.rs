use dozer_cache::dozer_log::{reader::LogClient, schemas::EndpointSchema};
use dozer_tracing::Labels;
use dozer_types::{
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, BuildRequest,
    },
    serde_json,
    tonic::transport::Channel,
};

use crate::{cache_alias_and_labels, errors::ApiInitError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointMeta {
    pub name: String,
    pub log_id: String,
    pub schema: EndpointSchema,
}

impl EndpointMeta {
    pub async fn load_from_client(
        client: &mut InternalPipelineServiceClient<Channel>,
        endpoint: String,
    ) -> Result<(Self, LogClient), ApiInitError> {
        // We establish the log stream first to avoid tonic auto-reconnecting without us knowing.
        let log_client = LogClient::new(client, endpoint.clone()).await?;
        let log_id = client.get_id(()).await?.into_inner().id;
        let build = client
            .describe_build(BuildRequest {
                endpoint: endpoint.clone(),
            })
            .await?
            .into_inner();
        let schema = serde_json::from_str(&build.schema_string)?;

        Ok((
            Self {
                name: endpoint,
                log_id,
                schema,
            },
            log_client,
        ))
    }

    pub fn cache_alias_and_labels(&self, extra_labels: Labels) -> (String, Labels) {
        let (alias, mut labels) = cache_alias_and_labels(self.name.clone());
        labels.extend(extra_labels);
        (alias, labels)
    }
}
