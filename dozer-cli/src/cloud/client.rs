use crate::cli::cloud::{
    Cloud, DeployCommandArgs, ListCommandArgs, LogCommandArgs, SecretsCommand, VersionCommand,
};
use crate::cli::init_dozer;
use crate::cloud::cloud_app_context::CloudAppContext;
use crate::cloud::cloud_helper::list_files;

use crate::cloud::deployer::{deploy_app, stop_app};
use crate::cloud::login::CredentialInfo;
use crate::cloud::monitor::monitor_app;
use crate::cloud::token_layer::TokenLayer;
use crate::cloud::DozerGrpcCloudClient;
use crate::errors::OrchestrationError::FailedToReadOrganisationName;
use crate::errors::{map_tonic_error, CliError, CloudError, CloudLoginError, OrchestrationError};
use crate::simple::SimpleOrchestrator;
use dozer_types::constants::{DEFAULT_CLOUD_TARGET_URL, LOCK_FILE};
use dozer_types::grpc_types::cloud::{
    dozer_cloud_client::DozerCloudClient, CreateSecretRequest, DeleteAppRequest,
    DeleteSecretRequest, GetSecretRequest, GetStatusRequest, ListAppRequest, ListSecretsRequest,
    LogMessageRequest, UpdateSecretRequest,
};
use dozer_types::grpc_types::cloud::{
    DeploymentInfo, DeploymentStatusWithHealth, File, ListDeploymentRequest,
    SetCurrentVersionRequest, UpsertVersionRequest,
};
use dozer_types::log::info;
use dozer_types::models::config::Config;
use dozer_types::prettytable::{row, table};
use futures::{select, FutureExt, StreamExt};
use std::io;
use std::sync::Arc;
use tonic::transport::Endpoint;
use tower::ServiceBuilder;

use super::login::LoginSvc;
use super::version::{get_version_status, version_is_up_to_date, version_status_table};

pub async fn get_grpc_cloud_client(
    cloud: &Cloud,
    cloud_config: Option<&dozer_types::models::cloud::Cloud>,
) -> Result<DozerCloudClient<TokenLayer>, CloudError> {
    let profile_name = match &cloud.profile {
        None => cloud_config.as_ref().and_then(|c| c.profile.clone()),
        Some(_) => cloud.profile.clone(),
    };
    let credential = CredentialInfo::load(profile_name)?;
    let target_url = cloud
        .target_url
        .as_ref()
        .unwrap_or(&credential.target_url)
        .clone();
    info!("Connecting to cloud service \"{}\"", target_url);
    let endpoint = Endpoint::from_shared(target_url.to_owned())?;
    let channel = Endpoint::connect(&endpoint).await?;
    let channel = ServiceBuilder::new()
        .layer_fn(|channel| TokenLayer::new(channel, credential.clone()))
        .service(channel);
    let client = DozerCloudClient::new(channel);
    Ok(client)
}

pub struct CloudClient {
    config: Config,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl CloudClient {
    pub fn new(config: Config, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self { config, runtime }
    }
}

impl DozerGrpcCloudClient for CloudClient {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(
        &mut self,
        cloud: Cloud,
        deploy: DeployCommandArgs,
        config_paths: Vec<String>,
    ) -> Result<(), OrchestrationError> {
        let app_id = if cloud.app_id.is_some() {
            cloud.app_id.clone()
        } else {
            let app_id_from_context = CloudAppContext::get_app_id(self.config.cloud.as_ref());
            match app_id_from_context {
                Ok(id) => Some(id),
                Err(_) => None,
            }
        };

        let dozer = init_dozer(
            self.runtime.clone(),
            self.config.clone(),
            Default::default(),
        )?;
        let cloud_config = self.config.cloud.as_ref();
        let lockfile_path = dozer.lockfile_path();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;
            let mut files = list_files(config_paths)?;
            if deploy.locked {
                let lockfile_contents = tokio::fs::read_to_string(lockfile_path)
                    .await
                    .map_err::<OrchestrationError, _>(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        CloudError::LockfileNotFound.into()
                    } else {
                        CliError::Io(e).into()
                    }
                })?;
                let lockfile = File {
                    name: LOCK_FILE.to_owned(),
                    content: lockfile_contents,
                };
                files.push(lockfile);
            }

            // 2. START application
            deploy_app(
                &mut client,
                &app_id,
                deploy.secrets,
                deploy.allow_incompatible,
                files,
                deploy.follow,
            )
            .await?;
            Ok::<(), OrchestrationError>(())
        })?;
        Ok(())
    }

    fn delete(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        // Get app_id from command line argument if there, otherwise take it from the cloud config file
        // if the app_id is from the cloud config file then set `delete_cloud_file` to true and use it later
        // to delete the file after deleting the app

        let (app_id, delete_cloud_file) = if let Some(app_id) = cloud.app_id.clone() {
            // if the app_id on command line is equal to the one in the cloud config file then file can be deleted
            if app_id == CloudAppContext::get_app_id(self.config.cloud.as_ref())? {
                (app_id, true)
            } else {
                (app_id, false)
            }
        } else {
            (
                CloudAppContext::get_app_id(self.config.cloud.as_ref())?,
                true,
            )
        };

        let mut double_check = String::new();
        println!("Are you sure to delete the application {}? (y/N)", app_id);
        io::stdin()
            .read_line(&mut double_check)
            .map_err(FailedToReadOrganisationName)?;
        let response = double_check.trim().to_string().to_uppercase();

        if response == "Y" {
            info!("Deleting application {}", app_id);
        } else {
            info!("The application {} was not deleted", app_id);
            return Ok(());
        }

        let cloud_config = self.config.cloud.as_ref();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            stop_app(&mut client, &app_id).await?;

            // steps.start_next_step();
            let delete_result = client
                .delete_application(DeleteAppRequest {
                    app_id: app_id.clone(),
                })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            if delete_result.success {
                info!("Deleted {}", &app_id);

                if delete_cloud_file {
                    let _ = CloudAppContext::delete_config_file();
                }
            }

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn list(&mut self, cloud: Cloud, list: ListCommandArgs) -> Result<(), OrchestrationError> {
        let cloud_config = self.config.cloud.as_ref();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;
            let response = client
                .list_applications(ListAppRequest {
                    limit: list.limit,
                    offset: list.offset,
                    name: list.name,
                    uuid: list.uuid,
                    order_by: None,
                    desc: None,
                })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            let mut table = table!();

            for app in response.apps {
                table.add_row(row![app.app_id, app.app_name]);
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
            .unwrap_or(CloudAppContext::get_app_id(self.config.cloud.as_ref())?);
        let cloud_config = self.config.cloud.as_ref();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;
            let response = client
                .get_status(GetStatusRequest { app_id })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            let mut table = table!();

            table.add_row(row!["Api endpoint", response.data_endpoint,]);

            let mut deployment_table = table!();
            deployment_table.set_titles(row![
                "Deployment",
                "App",
                "Api",
                "Version",
                "Phase",
                "Error"
            ]);

            for status in response.deployments.iter() {
                let deployment = status.deployment.as_ref().expect("deployment is expected");
                fn mark(status: bool) -> &'static str {
                    if status {
                        "🟢"
                    } else {
                        "🟠"
                    }
                }

                let mut version = "".to_string();
                for (loop_version, loop_deployment) in response.versions.iter() {
                    if loop_deployment == &deployment.deployment {
                        if Some(*loop_version) == response.current_version {
                            version = format!("v{loop_version} (current)");
                        } else {
                            version = format!("v{loop_version}");
                        }
                        break;
                    }
                }

                deployment_table.add_row(row![
                    deployment.deployment,
                    format!("Deployment Status: {:?}", deployment.status),
                    format!("Version: {}", version),
                ]);
                for r in status.resources.iter() {
                    deployment_table.add_row(row![
                        "",
                        format!("{}: {}", r.typ, mark(r.available == r.desired)),
                        format!("{}/{}", r.available.unwrap_or(0), r.desired.unwrap_or(0)),
                    ]);
                }
            }

            table.add_row(row!["Deployments", deployment_table]);

            table.printstd();
            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn monitor(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        monitor_app(&cloud, self.config.cloud.as_ref(), self.runtime.clone())
            .map_err(crate::errors::OrchestrationError::CloudError)
    }

    fn trace_logs(&mut self, cloud: Cloud, logs: LogCommandArgs) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(self.config.cloud.as_ref())?);
        let cloud_config = self.config.cloud.as_ref();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            let res = client
                .list_deployments(ListDeploymentRequest {
                    app_id: app_id.clone(),
                })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            // Show log of the latest deployment for now.
            let Some(deployment) = logs
                .deployment
                .or_else(|| latest_deployment(&res.deployments))
            else {
                info!("No deployments found");
                return Ok(());
            };
            let mut response = client
                .on_log_message(LogMessageRequest {
                    app_id,
                    deployment,
                    follow: logs.follow,
                    include_build: !logs.ignore_build,
                    include_app: !logs.ignore_app,
                    include_api: !logs.ignore_api,
                })
                .await
                .map_err(map_tonic_error)?
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

    fn login(
        &mut self,
        cloud: Cloud,
        organisation_slug: Option<String>,
        profile: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Result<(), OrchestrationError> {
        info!("Organisation and client details can be created in https://dashboard.dev.getdozer.io/login \n");
        let organisation_slug = match organisation_slug {
            None => {
                let mut organisation_slug = String::new();
                println!("Please enter your organisation slug:");
                io::stdin()
                    .read_line(&mut organisation_slug)
                    .map_err(FailedToReadOrganisationName)?;
                organisation_slug.trim().to_string()
            }
            Some(name) => name,
        };

        self.runtime.block_on(async move {
            let login_svc = LoginSvc::new(
                organisation_slug,
                cloud
                    .target_url
                    .unwrap_or(DEFAULT_CLOUD_TARGET_URL.to_string()),
            )
            .await?;
            login_svc.login(profile, client_id, client_secret).await?;
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
            .unwrap_or(CloudAppContext::get_app_id(self.config.cloud.as_ref())?);
        let cloud_config = self.config.cloud.as_ref();

        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            match command {
                SecretsCommand::Create { name, value } => {
                    client
                        .create_secret(CreateSecretRequest {
                            app_id,
                            name,
                            value,
                        })
                        .await
                        .map_err(map_tonic_error)?;

                    info!("Secret created");
                }
                SecretsCommand::Update { name, value } => {
                    client
                        .update_secret(UpdateSecretRequest {
                            app_id,
                            name,
                            value,
                        })
                        .await
                        .map_err(map_tonic_error)?;

                    info!("Secret updated");
                }
                SecretsCommand::Delete { name } => {
                    client
                        .delete_secret(DeleteSecretRequest { app_id, name })
                        .await
                        .map_err(map_tonic_error)?;

                    info!("Secret deleted")
                }
                SecretsCommand::Get { name } => {
                    let response = client
                        .get_secret(GetSecretRequest { app_id, name })
                        .await
                        .map_err(map_tonic_error)?
                        .into_inner();

                    info!("Secret \"{}\" exist", response.name);
                }
                SecretsCommand::List {} => {
                    let response = client
                        .list_secrets(ListSecretsRequest { app_id })
                        .await
                        .map_err(map_tonic_error)?
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

impl CloudClient {
    pub fn version(
        &mut self,
        cloud: Cloud,
        version: VersionCommand,
    ) -> Result<(), OrchestrationError> {
        let app_id = cloud
            .app_id
            .clone()
            .unwrap_or(CloudAppContext::get_app_id(self.config.cloud.as_ref())?);

        let cloud_config = self.config.cloud.as_ref();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            match version {
                VersionCommand::Create { deployment } => {
                    let status = client
                        .get_status(GetStatusRequest {
                            app_id: app_id.clone(),
                        })
                        .await
                        .map_err(map_tonic_error)?
                        .into_inner();
                    let latest_version = status.versions.into_values().max().unwrap_or(0);

                    client
                        .upsert_version(UpsertVersionRequest {
                            app_id,
                            version: latest_version + 1,
                            deployment,
                        })
                        .await
                        .map_err(map_tonic_error)?;
                }
                VersionCommand::SetCurrent { version } => {
                    client
                        .set_current_version(SetCurrentVersionRequest { app_id, version })
                        .await?;
                }
                VersionCommand::Status { version } => {
                    let status = client
                        .get_status(GetStatusRequest { app_id })
                        .await
                        .map_err(map_tonic_error)?
                        .into_inner();
                    let Some(deployment) = status.versions.get(&version) else {
                        info!("Version {} does not exist", version);
                        return Ok(());
                    };
                    let api_available = get_api_available(&status.deployments, *deployment);

                    let version_status =
                        get_version_status(&status.data_endpoint, version, api_available).await;
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
                                &status.data_endpoint,
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
}

fn latest_deployment(deployments: &[DeploymentInfo]) -> Option<u32> {
    deployments.iter().map(|status| status.deployment).max()
}

fn get_api_available(deployments: &[DeploymentStatusWithHealth], deployment: u32) -> i32 {
    let info = deployments
        .iter()
        .find(|status| {
            status
                .deployment
                .as_ref()
                .expect("deployment is expected")
                .deployment
                == deployment
        })
        .expect("Deployment should be found in deployments");

    info.resources
        .clone()
        .into_iter()
        .find(|r| r.typ == "api")
        .and_then(|r| r.available)
        .unwrap_or(1)
}
