use crate::cli::cloud::{
    default_num_replicas, ApiCommand, Cloud, DeployCommandArgs, ListCommandArgs, LogCommandArgs,
    SecretsCommand, VersionCommand,
};
use crate::cloud_app_context::CloudAppContext;
use crate::cloud_helper::list_files;
use crate::errors::CloudError::GRPCCallError;
use crate::errors::{CloudError, CloudLoginError, OrchestrationError};
use crate::progress_printer::{
    get_delete_steps, get_deploy_steps, get_update_steps, ProgressPrinter,
};
use crate::simple::cloud::deployer::{deploy_app, stop_app};
use crate::simple::cloud::login::CredentialInfo;
use crate::simple::cloud::monitor::monitor_app;
use crate::simple::token_layer::TokenLayer;
use crate::simple::SimpleOrchestrator;
use crate::CloudOrchestrator;
use dozer_types::grpc_types::cloud::{
    dozer_cloud_client::DozerCloudClient, CreateAppRequest, CreateSecretRequest, DeleteAppRequest,
    DeleteSecretRequest, GetSecretRequest, GetStatusRequest, ListAppRequest, ListSecretsRequest,
    LogMessageRequest, UpdateAppRequest, UpdateSecretRequest,
};
use dozer_types::grpc_types::cloud::{
    DeploymentStatus, SetCurrentVersionRequest, SetNumApiInstancesRequest, UpsertVersionRequest,
};
use dozer_types::log::info;
use dozer_types::prettytable::{row, table};
use futures::{select, FutureExt, StreamExt};
use tonic::transport::Endpoint;
use tower::ServiceBuilder;

use super::cloud::login::LoginSvc;
use super::cloud::version::{get_version_status, version_is_up_to_date, version_status_table};

async fn get_cloud_client(cloud: &Cloud) -> Result<DozerCloudClient<TokenLayer>, CloudError> {
    let credential = CredentialInfo::load(cloud.profile.to_owned())?;
    info!("Connecting to cloud service \"{}\"", credential.target_url);
    let target_url = credential.target_url.clone();
    let endpoint = Endpoint::from_shared(target_url.to_owned())?;
    let channel = Endpoint::connect(&endpoint).await?;
    let channel = ServiceBuilder::new()
        .layer_fn(|channel| TokenLayer::new(channel, credential.clone()))
        .service(channel);
    let client = DozerCloudClient::new(channel);
    Ok(client)
}

impl CloudOrchestrator for SimpleOrchestrator {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(
        &mut self,
        cloud: Cloud,
        deploy: DeployCommandArgs,
    ) -> Result<(), OrchestrationError> {
        let app_id = if cloud.app_id.is_some() {
            cloud.app_id.clone()
        } else {
            let app_id_from_context = CloudAppContext::get_app_id(&self.config.cloud);
            match app_id_from_context {
                Ok(id) => Some(id),
                Err(_) => None,
            }
        };

        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let files = list_files()?;
            let (app_id_to_start, mut steps) = match app_id {
                None => {
                    let mut steps = ProgressPrinter::new(get_deploy_steps());
                    // 1. CREATE application
                    steps.start_next_step();
                    let response = client
                        .create_application(CreateAppRequest { files })
                        .await
                        .map_err(GRPCCallError)?
                        .into_inner();

                    steps.complete_step(Some(&format!(
                        "Application created with id: {:?}",
                        &response.app_id
                    )));

                    CloudAppContext::save_app_id(response.app_id.clone())?;

                    (response.app_id, steps)
                }
                Some(app_id) => {
                    let mut steps = ProgressPrinter::new(get_update_steps());
                    // 1. update application
                    steps.start_next_step();
                    client
                        .update_application(UpdateAppRequest {
                            app_id: app_id.clone(),
                            files,
                        })
                        .await
                        .map_err(GRPCCallError)?
                        .into_inner();

                    steps.complete_step(Some(&format!("Updated {}", &app_id)));

                    (app_id, steps)
                }
            };

            // 2. START application
            deploy_app(
                &mut client,
                &app_id_to_start,
                deploy.num_replicas.unwrap_or_else(default_num_replicas),
                &mut steps,
                deploy.secrets,
            )
            .await
        })?;
        Ok(())
    }

    fn delete(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;

            let mut steps = ProgressPrinter::new(get_delete_steps());

            steps.start_next_step();

            stop_app(&mut client, &app_id).await?;

            steps.start_next_step();
            let _delete_result = client
                .delete_application(DeleteAppRequest {
                    app_id: app_id.clone(),
                })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            steps.complete_step(Some(&format!("Deleted {}", &app_id)));

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn list(&mut self, cloud: Cloud, list: ListCommandArgs) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let response = client
                .list_applications(ListAppRequest {
                    limit: list.limit,
                    offset: list.offset,
                    name: list.name,
                    uuid: list.uuid,
                })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            let mut table = table!();

            for app in response.apps {
                if let Some(app_data) = app.app {
                    table.add_row(row![app.app_id, app_data.convert_to_table()]);
                }
            }

            table.printstd();

            info!(
                "Total apps: {}",
                response
                    .pagination
                    .map_or_else(|| 0, |pagination| pagination.total)
            );

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn status(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let response = client
                .get_status(GetStatusRequest { app_id })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            let mut table = table!();

            table.add_row(row!["Api endpoint", response.api_endpoint,]);

            let mut deployment_table = table!();
            deployment_table.set_titles(row!["Deployment", "App", "Api", "Version"]);

            for status in response.deployments.iter() {
                let deployment = status.deployment;

                fn mark(status: bool) -> &'static str {
                    if status {
                        "🟢"
                    } else {
                        "🟠"
                    }
                }

                fn number(number: Option<i32>) -> String {
                    if let Some(n) = number {
                        n.to_string()
                    } else {
                        "-".to_string()
                    }
                }

                let mut version = "".to_string();
                for (loop_version, loop_deployment) in response.versions.iter() {
                    if loop_deployment == &deployment {
                        if Some(*loop_version) == response.current_version {
                            version = format!("v{loop_version} (current)");
                        } else {
                            version = format!("v{loop_version}");
                        }
                        break;
                    }
                }

                deployment_table.add_row(row![
                    deployment,
                    mark(status.app_running),
                    format!(
                        "{}/{}",
                        number(status.api_available),
                        number(status.api_desired)
                    ),
                    version
                ]);
            }

            table.add_row(row!["Deployments", deployment_table]);

            table.printstd();
            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn monitor(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);

        monitor_app(app_id, cloud.target_url, self.runtime.clone())
            .map_err(crate::errors::OrchestrationError::CloudError)
    }

    fn trace_logs(&mut self, cloud: Cloud, logs: LogCommandArgs) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;

            let status = client
                .get_status(GetStatusRequest {
                    app_id: app_id.clone(),
                })
                .await?
                .into_inner();

            // Show log of the latest deployment for now.
            let Some(deployment) = logs.deployment.or_else(|| latest_deployment(&status.deployments)) else {
                info!("No deployments found");
                return Ok(());
            };
            let mut response = client
                .on_log_message(LogMessageRequest {
                    app_id,
                    deployment,
                    follow: logs.follow,
                    include_migrate: true,
                    include_app: true,
                    include_api: true,
                })
                .await?
                .into_inner()
                .fuse();

            let mut ctrlc = std::pin::pin!(tokio::signal::ctrl_c().fuse());
            loop {
                select! {
                    message = response.next() => {
                        if let Some(message) = message {
                            let message = message?;
                            for line in message.message.lines() {
                                info!("[{}] {line}", message.from);
                            }
                        } else {
                            break;
                        }
                    }
                    _ = ctrlc => {
                        break;
                    }
                };
            }

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn login(&mut self, cloud: Cloud, company_name: String) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let login_svc = LoginSvc::new(company_name, cloud.target_url).await?;
            login_svc.login().await?;
            Ok::<(), CloudLoginError>(())
        })?;
        Ok(())
    }

    fn execute_secrets_command(
        &mut self,
        cloud: Cloud,
        command: SecretsCommand,
    ) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;

            match command {
                SecretsCommand::Create { name, value } => {
                    client
                        .create_secret(CreateSecretRequest {
                            app_id,
                            name,
                            value,
                        })
                        .await?;

                    info!("Secret created");
                }
                SecretsCommand::Update { name, value } => {
                    client
                        .update_secret(UpdateSecretRequest {
                            app_id,
                            name,
                            value,
                        })
                        .await?;

                    info!("Secret updated");
                }
                SecretsCommand::Delete { name } => {
                    client
                        .delete_secret(DeleteSecretRequest { app_id, name })
                        .await?;

                    info!("Secret deleted")
                }
                SecretsCommand::Get { name } => {
                    let response = client
                        .get_secret(GetSecretRequest { app_id, name })
                        .await?
                        .into_inner();

                    info!("Secret \"{}\" exist", response.name);
                }
                SecretsCommand::List {} => {
                    let response = client
                        .list_secrets(ListSecretsRequest { app_id })
                        .await?
                        .into_inner();

                    info!("Secrets:");
                    let mut table = table!();

                    for secret in response.secrets {
                        table.add_row(row![secret]);
                    }

                    table.printstd();
                }
            }
            Ok::<_, CloudError>(())
        })?;

        Ok(())
    }
}

impl SimpleOrchestrator {
    pub fn version(
        &mut self,
        cloud: Cloud,
        version: VersionCommand,
    ) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(&self.config.cloud)?);
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;

            match version {
                VersionCommand::Create { deployment } => {
                    let status = client
                        .get_status(GetStatusRequest {
                            app_id: app_id.clone(),
                        })
                        .await?
                        .into_inner();
                    let latest_version = status.versions.into_values().max().unwrap_or(0);

                    client
                        .upsert_version(UpsertVersionRequest {
                            app_id,
                            version: latest_version + 1,
                            deployment,
                        })
                        .await?;
                }
                VersionCommand::SetCurrent { version } => {
                    client
                        .set_current_version(SetCurrentVersionRequest { app_id, version })
                        .await?;
                }
                VersionCommand::Status { version } => {
                    let status = client
                        .get_status(GetStatusRequest { app_id })
                        .await?
                        .into_inner();
                    let Some(deployment) = status.versions.get(&version) else {
                        info!("Version {} does not exist", version);
                        return Ok(());
                    };
                    let api_available = get_api_available(&status.deployments, *deployment);

                    let version_status =
                        get_version_status(&status.api_endpoint, version, api_available).await;
                    let mut table = table!();

                    if let Some(current_version) = status.current_version {
                        if current_version != version {
                            let current_api_available = get_api_available(
                                &status.deployments,
                                status.versions[&current_version],
                            );

                            table.add_row(row![
                                format!("v{version}"),
                                version_status_table(&version_status)
                            ]);

                            let current_version_status = get_version_status(
                                &status.api_endpoint,
                                current_version,
                                current_api_available,
                            )
                            .await;
                            table.add_row(row![
                                format!("v{current_version} (current)"),
                                version_status_table(&current_version_status)
                            ]);

                            table.printstd();

                            if version_is_up_to_date(&version_status, &current_version_status) {
                                info!("Version {} is up to date", version);
                            } else {
                                info!("Version {} is not up to date", version);
                            }
                        } else {
                            table.add_row(row![
                                format!("v{version} (current)"),
                                version_status_table(&version_status)
                            ]);
                            table.printstd();
                        }
                    } else {
                        table.add_row(row![
                            format!("v{version}"),
                            version_status_table(&version_status)
                        ]);
                        table.printstd();
                        info!("No current version");
                    };
                }
            }

            Ok::<_, CloudError>(())
        })?;
        Ok(())
    }

    pub fn api(&self, cloud: Cloud, api: ApiCommand) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let app_id = CloudAppContext::get_app_id(&self.config.cloud)?;

            let mut client = get_cloud_client(&cloud).await?;

            match api {
                ApiCommand::SetNumReplicas { num_replicas } => {
                    let status = client
                        .get_status(GetStatusRequest {
                            app_id: app_id.clone(),
                        })
                        .await?
                        .into_inner();
                    // Update the latest deployment for now.
                    let Some(deployment) = latest_deployment(&status.deployments) else {
                        info!("No deployments found");
                        return Ok(());
                    };
                    client
                        .set_num_api_instances(SetNumApiInstancesRequest {
                            app_id,
                            deployment,
                            num_api_instances: num_replicas,
                        })
                        .await?;
                }
            }
            Ok::<_, CloudError>(())
        })?;
        Ok(())
    }
}

fn latest_deployment(deployments: &[DeploymentStatus]) -> Option<u32> {
    deployments.iter().map(|status| status.deployment).max()
}

fn get_api_available(deployments: &[DeploymentStatus], deployment: u32) -> i32 {
    deployments
        .iter()
        .find(|status| status.deployment == deployment)
        .expect("Deployment should be found in deployments")
        .api_available
        .unwrap_or(1)
}
