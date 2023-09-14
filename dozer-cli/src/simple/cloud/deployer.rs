use crate::errors::CloudError;

use crate::simple::token_layer::TokenLayer;
use dozer_types::grpc_types::cloud::dozer_cloud_client::DozerCloudClient;

use crate::errors::CloudError::GRPCCallError;
use dozer_types::grpc_types::cloud::{Secret, StopRequest, StopResponse};
use dozer_types::log::{error, info};

use crate::cloud_app_context::CloudAppContext;
use crate::progress_printer::ProgressPrinter;
use dozer_types::grpc_types::cloud::AppResponse;
use dozer_types::grpc_types::cloud::DeployAppRequest;
use dozer_types::grpc_types::cloud::DeployAppResponse;
use dozer_types::grpc_types::cloud::DeployStep;
use dozer_types::grpc_types::cloud::File;
use dozer_types::grpc_types::cloud::InfraRequest;

pub async fn deploy_app(
    client: &mut DozerCloudClient<TokenLayer>,
    app_id: &Option<String>,
    num_api_instances: i32,
    secrets: Vec<Secret>,
    allow_incompatible: bool,
    files: Vec<File>,
) -> Result<(), CloudError> {
    let mut response = client
        .deploy_application(DeployAppRequest {
            app_id: app_id.clone(),
            secrets,
            allow_incompatible,
            files,
            infra: Some(InfraRequest { num_api_instances }),
        })
        .await?
        .into_inner();

    let mut printer = ProgressPrinter::new();
    while let Some(DeployAppResponse {
        result,
        completed: _,
        error: error_message,
    }) = response.message().await.map_err(GRPCCallError)?
    {
        if let Some(message) = error_message {
            error!("{}", message);
        } else {
            match result {
                Some(dozer_types::grpc_types::cloud::deploy_app_response::Result::AppCreated(
                    AppResponse {
                        app_id: new_app_id, ..
                    },
                )) => {
                    if let Some(app_id) = app_id {
                        info!("Updating {app_id} application");
                    } else {
                        info!("Deploying new application {new_app_id}");
                        CloudAppContext::save_app_id(new_app_id)?;
                    }
                }
                Some(dozer_types::grpc_types::cloud::deploy_app_response::Result::Step(
                    DeployStep {
                        text,
                        is_completed,
                        current_step,
                        ..
                    },
                )) => {
                    if is_completed {
                        printer.complete_step(current_step, &text);
                    } else {
                        printer.start_step(current_step, &text);
                    }
                }
                Some(dozer_types::grpc_types::cloud::deploy_app_response::Result::LogMessage(
                    message,
                )) => {
                    message.split('\n').for_each(|line| {
                        info!("{}", line);
                    });
                }
                None => {}
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
