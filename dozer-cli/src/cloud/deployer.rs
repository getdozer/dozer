use crate::errors::CloudError;

use crate::cloud::progress_printer::ProgressPrinter;
use crate::cloud::token_layer::TokenLayer;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;
use dozer_types::grpc_types::cloud::DeploymentStatus;
use dozer_types::grpc_types::cloud::GetDeploymentStatusRequest;

use crate::cloud_app_context::CloudAppContext;
use dozer_types::grpc_types::cloud::DeployAppRequest;
use dozer_types::grpc_types::cloud::File;
use dozer_types::grpc_types::cloud::Secret;
use dozer_types::log::{info, warn};

pub async fn deploy_app(
    client: &mut DozerCloudClient<TokenLayer>,
    app_id: &Option<String>,
    secrets: Vec<Secret>,
    allow_incompatible: bool,
    files: Vec<File>,
    follow: bool,
) -> Result<(), CloudError> {
    let response = client
        .deploy_application(DeployAppRequest {
            app_id: app_id.clone(),
            secrets,
            allow_incompatible,
            files,
        })
        .await?
        .into_inner();

    let app_id = response.app_id;
    let deployment_id = response.deployment_id;
    let url = response.deployment_url;
    info!("Deploying new application with App Id: {app_id}, Deployment Id: {deployment_id}");
    info!("Follow the deployment progress at {url}");

    if follow {
        print_progress(client, app_id, deployment_id).await?;
    }

    Ok::<(), CloudError>(())
}

async fn print_progress(
    client: &mut DozerCloudClient<TokenLayer>,
    app_id: String,
    deployment_id: String,
) -> Result<(), CloudError> {
    let mut current_step = 0;
    let mut printer = ProgressPrinter::new();
    let request = GetDeploymentStatusRequest {
        app_id: app_id.clone(),
        deployment_id,
    };
    loop {
        let response = client
            .get_deployment_status(request.clone())
            .await?
            .into_inner();

        if response.status == DeploymentStatus::Success as i32 {
            info!("Deployment completed successfully");
            info!("You can get API requests samples with `dozer cloud api-request-samples`");

            CloudAppContext::save_app_id(app_id)?;

            break;
        } else if response.status == DeploymentStatus::Failed as i32 {
            warn!("Deployment failed!");
            break;
        } else {
            let steps = response.steps.clone();
            let completed_steps = response
                .steps
                .into_iter()
                .filter(|s| {
                    s.status == DeploymentStatus::Success as i32 && s.step_index >= current_step
                })
                .map(|s| (s.step_index, s.step_text))
                .collect::<Vec<(u32, String)>>();

            for (step_no, text) in completed_steps.iter() {
                printer.complete_step(*step_no, text)
            }

            if !completed_steps.is_empty() {
                current_step = completed_steps.last().unwrap().0 + 1;
                let text = steps[current_step as usize].step_text.clone();
                printer.start_step(current_step, &text);
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Ok::<(), CloudError>(())
}
