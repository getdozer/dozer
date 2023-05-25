use crate::errors::CloudError;
use crate::errors::CloudError::GRPCCallError;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;
use dozer_types::grpc_types::cloud::StartUpdate;
use dozer_types::grpc_types::cloud::{StartRequest, StopRequest, StopResponse};
use dozer_types::indicatif::ProgressBar;
use dozer_types::log::{info, warn};

pub async fn deploy_app(
    client: &mut DozerCloudClient<tonic::transport::Channel>,
    app_id: &str,
    num_api_instances: i32,
) -> Result<(), CloudError> {
    let mut response = client
        .start_dozer(StartRequest {
            app_id: app_id.to_string(),
            num_api_instances,
        })
        .await?
        .into_inner();

    let pb = ProgressBar::new(0);

    while let Some(StartUpdate {
        result,
        current_step,
        total_steps,
    }) = response.message().await.map_err(GRPCCallError)?
    {
        match result {
            Some(r) => {
                if r.success {
                    info!("Deployed {}", &r.app_id);
                    info!("Endpoint: http://{}", r.api_endpoint);
                } else {
                    match r.error {
                        Some(error) => warn!("Deployment failed. Error: {}", &error),
                        None => warn!("Deployment failed"),
                    }
                }
            }
            None => {
                pb.set_length(total_steps.into());
                pb.set_position(current_step.into());
            }
        }
    }

    Ok::<(), CloudError>(())
}

pub async fn stop_app(
    client: &mut DozerCloudClient<tonic::transport::Channel>,
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
