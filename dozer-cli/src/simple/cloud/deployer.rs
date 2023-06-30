use crate::errors::CloudError;

use crate::simple::token_layer::TokenLayer;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;

use crate::errors::CloudError::GRPCCallError;
use dozer_types::grpc_types::cloud::{
    Secret, StartRequest, StartUpdate, StopRequest, StopResponse,
};
use dozer_types::log::warn;

use crate::progress_printer::ProgressPrinter;

pub async fn deploy_app(
    client: &mut DozerCloudClient<TokenLayer>,
    app_id: &str,
    num_api_instances: i32,
    steps: &mut ProgressPrinter,
    secrets: Option<Vec<Secret>>,
) -> Result<(), CloudError> {
    let mut response = client
        .start_dozer(StartRequest {
            app_id: app_id.to_string(),
            num_api_instances,
            secrets: secrets.unwrap_or(vec![]),
        })
        .await?
        .into_inner();

    while let Some(StartUpdate {
        result,
        current_step: _,
        total_steps: _,
    }) = response.message().await.map_err(GRPCCallError)?
    {
        match result {
            Some(r) => {
                if r.success {
                    steps.complete_step(Some(&format!(
                        "Deployed {}\nEndpoint: {}",
                        r.app_id, r.api_endpoint
                    )));
                } else {
                    match r.error {
                        Some(error) => warn!("Deployment failed. Error: {}", &error),
                        None => warn!("Deployment failed"),
                    }
                }
            }
            None => {
                steps.start_next_step();
            }
        }
    }

    Ok::<(), CloudError>(())
}

pub async fn stop_app(
    client: &mut DozerCloudClient<TokenLayer>,
    app_id: &str,
) -> Result<StopResponse, CloudError> {
    let result = client
        .stop_dozer(StopRequest {
            app_id: app_id.to_string(),
        })
        .await?
        .into_inner();

    Ok(result)
}
