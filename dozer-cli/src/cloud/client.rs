use crate::cli::cloud::{
    Cloud, DeployCommandArgs, ListCommandArgs, LogCommandArgs, SecretsCommand, VersionCommand,
};
use crate::cli::get_base_dir;
use crate::cloud::cloud_app_context::CloudAppContext;
use crate::cloud::cloud_helper::list_files;

use crate::cloud::deployer::deploy_app;
use crate::cloud::login::CredentialInfo;
use crate::cloud::monitor::monitor_app;
use crate::cloud::token_layer::TokenLayer;
use crate::cloud::DozerGrpcCloudClient;
use crate::console_helper::{get_colored_text, PURPLE};
use crate::errors::OrchestrationError::FailedToReadOrganisationName;
use crate::errors::{
    map_tonic_error, CliError, CloudContextError, CloudError, CloudLoginError, OrchestrationError,
};
use crate::simple::orchestrator::lockfile_path;
use dozer_types::constants::{DEFAULT_CLOUD_TARGET_URL, LOCK_FILE};
use dozer_types::grpc_types::api_explorer::api_explorer_service_client::ApiExplorerServiceClient;
use dozer_types::grpc_types::api_explorer::GetApiTokenRequest;
use dozer_types::grpc_types::cloud::{
    dozer_cloud_client::DozerCloudClient, CreateSecretRequest, DeleteAppRequest,
    DeleteSecretRequest, GetEndpointCommandsSamplesRequest, GetSecretRequest, ListAppRequest,
    ListSecretsRequest, LogMessageRequest, UpdateSecretRequest,
};
use dozer_types::grpc_types::cloud::{
    CreateAppRequest, DeleteVersionRequest, DeploymentInfo, DeploymentStatus, File, GetAppRequest,
    ListDeploymentRequest, RmAliasRequest, SetAliasRequest, SetCurrentVersionRequest,
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
async fn establish_cloud_service_channel(
    cloud: &Cloud,
    cloud_config: Option<dozer_types::models::cloud::Cloud>,
) -> Result<TokenLayer, CloudError> {
    let profile_name = match cloud.profile.as_ref() {
        None => cloud_config.as_ref().and_then(|f| f.profile.clone()),
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
    Ok(channel)
}

pub async fn get_grpc_cloud_client(
    cloud: &Cloud,
    cloud_config: Option<dozer_types::models::cloud::Cloud>,
) -> Result<DozerCloudClient<TokenLayer>, CloudError> {
    let client = DozerCloudClient::new(establish_cloud_service_channel(cloud, cloud_config).await?);
    Ok(client)
}

pub async fn get_explorer_client(
    cloud: &Cloud,
    cloud_config: Option<dozer_types::models::cloud::Cloud>,
) -> Result<ApiExplorerServiceClient<TokenLayer>, CloudError> {
    let client =
        ApiExplorerServiceClient::new(establish_cloud_service_channel(cloud, cloud_config).await?);
    Ok(client)
}

pub struct CloudClient {
    cloud: crate::cli::cloud::Cloud,
    config: Option<Config>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl CloudClient {
    pub fn new(
        cloud: crate::cli::cloud::Cloud,
        config: Option<Config>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            cloud,
            config,
            runtime,
        }
    }

    pub fn get_app_id(&self) -> Option<(String, bool)> {
        // Get app_id from command line argument if there, otherwise take it from the cloud config file
        // if the app_id is from the cloud config file then set `from_cloud_file` to true and use it later
        // to perform cleanup in case of delete and other operations

        if let Some(app_id) = self.cloud.app_id.clone() {
            return Some((app_id, false));
        }
        self.config.as_ref().and_then(|config| {
            config.cloud.app_id.clone().map(|app_id| (app_id, true)).or(
                CloudAppContext::get_app_id(&config.cloud)
                    .ok()
                    .map(|app_id| (app_id, false)),
            )
        })
    }

    fn get_cloud_config(&self) -> Option<dozer_types::models::cloud::Cloud> {
        self.config.as_ref().map(|config| config.cloud.clone())
    }
}

impl DozerGrpcCloudClient for CloudClient {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(
        &mut self,
        deploy: DeployCommandArgs,
        config_paths: Vec<String>,
    ) -> Result<(), OrchestrationError> {
        let app_id = self.get_app_id().map(|a| a.0);
        let cloud = self.cloud.clone();
        let base_dir = get_base_dir()?;
        let lockfile_path = lockfile_path(base_dir);
        self.runtime.clone().block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, self.get_cloud_config()).await?;
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
                    content: lockfile_contents.into(),
                };
                files.push(lockfile);
            }

            // 2. START application
            deploy_app(
                &mut client,
                // &mut explorer_client,
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
    fn create(&mut self, config_paths: Vec<String>) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self.get_app_id().map(|a| a.0);
        let cloud = self.cloud.clone();
        if let Some(app_id) = app_id {
            return Err(CloudContextError::AppIdAlreadyExists(app_id).into());
        };

        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;
            let files = list_files(config_paths)?;
            let response = client
                .create_application(CreateAppRequest { files })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            CloudAppContext::save_app_id(response.app_id.clone())?;
            info!("Application created with id {}", response.app_id);
            Ok::<(), OrchestrationError>(())
        })
    }

    fn delete(&mut self) -> Result<(), OrchestrationError> {
        let (app_id, delete_cloud_file) = self
            .get_app_id()
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
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

        let cloud_config = self.get_cloud_config();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

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

    fn list(&mut self, list: ListCommandArgs) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let cloud = self.cloud.clone();
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

    fn status(&mut self) -> Result<(), OrchestrationError> {
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud_config = self.get_cloud_config();
        let cloud = self.cloud.clone();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;
            let response = client
                .get_application(GetAppRequest { app_id })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            let mut table = table!();
            table.set_titles(row!["Deployment", "Version", "Status"]);

            for deployment in response.deployments.iter() {
                fn deployment_status(status: i32) -> &'static str {
                    match status {
                        _ if status == DeploymentStatus::Pending as i32 => {
                            DeploymentStatus::Pending.as_str_name()
                        }
                        _ if status == DeploymentStatus::Running as i32 => {
                            DeploymentStatus::Running.as_str_name()
                        }
                        _ if status == DeploymentStatus::Success as i32 => {
                            DeploymentStatus::Success.as_str_name()
                        }
                        _ if status == DeploymentStatus::Failed as i32 => {
                            DeploymentStatus::Failed.as_str_name()
                        }
                        _ => "UNRECOGNIZED",
                    }
                }

                table.add_row(row![
                    deployment.deployment_id,
                    deployment.version,
                    deployment_status(deployment.status),
                ]);
            }

            table.printstd();
            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn monitor(&mut self) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
        monitor_app(&cloud, app_id, cloud_config, self.runtime.clone())
            .map_err(crate::errors::OrchestrationError::CloudError)
    }

    fn trace_logs(&mut self, logs: LogCommandArgs) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            let res = client
                .list_deployments(ListDeploymentRequest {
                    app_id: app_id.clone(),
                })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            // Show log of the latest version for now.
            let Some(version) = logs.version.or_else(|| latest_version(&res.deployments)) else {
                info!("No active version found");
                return Ok(());
            };
            let mut response = client
                .on_log_message(LogMessageRequest {
                    app_id,
                    version,
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
        &self,
        organisation_slug: Option<String>,
        profile: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    ) -> Result<(), OrchestrationError> {
        info!(
            "Organisation and client details can be created in https://cloud.getdozer.io/login \n"
        );
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
        let cloud = self.cloud.clone();
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
        command: SecretsCommand,
    ) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
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
    pub fn version(&mut self, version: VersionCommand) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config).await?;

            match version {
                VersionCommand::SetCurrent { version } => {
                    client
                        .set_current_version(SetCurrentVersionRequest { app_id, version })
                        .await?;
                }
                VersionCommand::Delete { version } => {
                    client
                        .delete_version(DeleteVersionRequest { app_id, version })
                        .await?;
                }
                VersionCommand::Alias { alias, version } => {
                    client
                        .set_alias(SetAliasRequest {
                            app_id,
                            version,
                            alias,
                        })
                        .await?;
                }
                VersionCommand::RmAlias { alias } => {
                    client.rm_alias(RmAliasRequest { app_id, alias }).await?;
                }
            }

            Ok::<_, CloudError>(())
        })?;
        Ok(())
    }

    pub fn print_api_request_samples(
        &self,
        endpoint: Option<String>,
    ) -> Result<(), OrchestrationError> {
        let cloud_config = self.get_cloud_config();
        let app_id = self
            .get_app_id()
            .map(|a| a.0)
            .ok_or_else(|| CloudContextError::AppIdNotFound)?;
        let cloud = self.cloud.clone();
        self.runtime.block_on(async move {
            let mut client = get_grpc_cloud_client(&cloud, cloud_config.clone()).await?;
            let mut explorer_client = get_explorer_client(&cloud, cloud_config).await?;

            let response = client
                .get_endpoint_commands_samples(GetEndpointCommandsSamplesRequest {
                    app_id: app_id.clone(),
                    endpoint,
                })
                .await
                .map_err(map_tonic_error)?
                .into_inner();

            let token_response = explorer_client
                .get_api_token(GetApiTokenRequest {
                    app_id: Some(app_id),
                    ttl: Some(3600),
                })
                .await?
                .into_inner();

            let mut rows = vec![];
            let token = match token_response.token {
                Some(token) => token,
                None => {
                    info!("Replace $DOZER_TOKEN with your API authorization token");
                    "$DOZER_TOKEN".to_string()
                }
            };

            for sample in response.samples {
                rows.push(get_colored_text(
                    &format!(
                        "\n##################### {} command ###########################\n",
                        sample.r#type
                    ),
                    PURPLE,
                ));
                rows.push(sample.command.replace("{token}", &token).to_string());
            }

            info!("{}", rows.join("\n"));

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }
}

fn latest_version(deployments: &[DeploymentInfo]) -> Option<u32> {
    deployments.iter().map(|status| status.version).max()
}
