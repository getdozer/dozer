use crate::errors::DeployError;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;
use dozer_types::grpc_types::cloud::StartRequest;
use dozer_types::grpc_types::cloud::StartUpdate;
use dozer_types::indicatif::{ProgressBar, ProgressStyle};
use dozer_types::log::{info, warn};

pub async fn deploy_app(
    client: &mut DozerCloudClient<tonic::transport::Channel>,
    app_id: &str,
) -> Result<(), DeployError> {
    let mut response = client
        .start_dozer(StartRequest {
            app_id: app_id.to_string(),
        })
        .await?
        .into_inner();

    let pb = attach_progress();

    while let Some(StartUpdate {
        result,
        current_step,
        total_steps,
    }) = response.message().await?
    {
        match result {
            Some(r) => {
                if r.success == true {
                    info!("Deployed {}", &r.id);
                    match r.api_endpoint {
                        None => {}
                        Some(endpoint) => info!("Endpoint: http://{endpoint}"),
                    }
                } else {
                    match r.error {
                        Some(error) => warn!("Deployment failed. Error: {}", &error),
                        None => warn!("Deployment failed"),
                    }
                }
            }
            None => {
                pb.set_message("Deployment in progress");
                pb.set_position(current_step.into());
                pb.set_length(total_steps.into());
            }
        }
    }

    Ok::<(), DeployError>(())
}

fn attach_progress() -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.red} {msg}: {pos}")
            .unwrap()
            // For more spinners check out the cli-spinners project:
            // https://github.com/sindresorhus/cli-spinners/blob/master/spinners.json
            .tick_strings(&[
                "▹▹▹▹▹",
                "▸▹▹▹▹",
                "▹▸▹▹▹",
                "▹▹▸▹▹",
                "▹▹▹▸▹",
                "▹▹▹▹▸",
                "▪▪▪▪▪",
            ]),
    );
    pb
}
