use crate::server;
use dozer_orchestrator::internal_grpc::GetAppConfigRequest;
use dozer_types::models::app_config::Config;

use super::{
    utils::{init_db_with_config, init_internal_pipeline_client, reset_db},
    AdminCliConfig,
};

pub struct CliProcess {
    pub config: AdminCliConfig,
}
impl CliProcess {
    pub async fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let internal_pipeline_config = self.config.to_owned().internal.pipeline;
        let client_connect_result = init_internal_pipeline_client(internal_pipeline_config).await;
        let mut dozer_config: Config = Config::default();
        if let Ok(mut client) = client_connect_result {
            let response = client.get_config(GetAppConfigRequest {}).await?;
            let config = response.into_inner();
            dozer_config = config.data.unwrap();
        }
        reset_db();
        init_db_with_config(dozer_config);
        server::get_server(
            self.config.to_owned().host,
            self.config.to_owned().port as u16,
        )
        .await?;
        Ok(())
    }
}
