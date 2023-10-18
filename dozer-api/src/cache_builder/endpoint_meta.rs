use dozer_cache::dozer_log::schemas::EndpointSchema;
use dozer_tracing::Labels;
use dozer_types::{
    grpc_types::internal::{
        internal_pipeline_service_client::InternalPipelineServiceClient, BuildRequest,
    },
    serde_json,
    tonic::transport::Channel,
};

use crate::{cache_alias_and_labels, errors::ApiInitError};

#[derive(Debug, Clone)]
pub struct EndpointMeta {
    pub name: String,
    pub log_id: String,
    pub build_name: String,
    pub schema: EndpointSchema,
    pub descriptor_bytes: Vec<u8>,
}

impl EndpointMeta {
    pub async fn new(
        client: &mut InternalPipelineServiceClient<Channel>,
        endpoint: String,
    ) -> Result<Self, ApiInitError> {
        let log_id = client.get_id(()).await?.into_inner().id;
        let build = client
            .describe_build(BuildRequest {
                endpoint: endpoint.clone(),
            })
            .await?
            .into_inner();
        let schema = serde_json::from_str(&build.schema_string)?;

        Ok(Self {
            name: endpoint,
            log_id,
            build_name: build.name,
            schema,
            descriptor_bytes: build.descriptor_bytes,
        })
    }

    pub fn cache_alias_and_labels(&self, extra_labels: Labels) -> (String, Labels) {
        let (alias, mut labels) =
            cache_alias_and_labels(self.name.clone(), self.build_name.clone());
        labels.extend(extra_labels);
        (alias, labels)
    }
}
