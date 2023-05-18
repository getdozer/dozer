use crate::cli::cloud::{Cloud, ListCommandArgs};
use crate::cloud_helper::list_files;
use crate::errors::CloudError::GRPCCallError;
use crate::errors::{CloudError, OrchestrationError};
use crate::simple::cloud::deployer::{deploy_app, stop_app};
use crate::simple::cloud::monitor::monitor_app;
use crate::simple::SimpleOrchestrator;
use crate::CloudOrchestrator;
use dozer_types::grpc_types::cloud::{
    dozer_cloud_client::DozerCloudClient, CreateAppRequest, DeleteAppRequest, GetStatusRequest,
    ListAppRequest, LogMessageRequest, UpdateAppRequest,
};
use dozer_types::log::info;
use dozer_types::prettytable::{row, table};

async fn get_cloud_client(
    cloud: &Cloud,
) -> Result<DozerCloudClient<tonic::transport::Channel>, tonic::transport::Error> {
    info!("Cloud service url: {:?}", &cloud.target_url);

    DozerCloudClient::connect(cloud.target_url.clone()).await
}

impl CloudOrchestrator for SimpleOrchestrator {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        // let username = match deploy.username {
        //     Some(u) => u,
        //     None => String::new(),
        // };
        // let _password = match deploy.password {
        //     Some(p) => p,
        //     None => String::new(),
        // };
        // info!("Authenticating for username: {:?}", username);
        // info!("Local dozer configuration path: {:?}", config_path);
        // calling the target url with the config fetched
        self.runtime.block_on(async move {
            // 1. CREATE application
            let mut client = get_cloud_client(&cloud).await?;
            let files = list_files()?;
            let response = client
                .create_application(CreateAppRequest { files })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            info!("Application created with id: {:?}", &response.id);
            // 2. START application
            deploy_app(&mut client, &response.id).await
        })?;
        Ok(())
    }

    fn update(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let files = list_files()?;
            let response = client
                .update_application(UpdateAppRequest {
                    id: app_id.clone(),
                    files,
                })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            info!("Updated {}", &response.id);

            deploy_app(&mut client, &app_id).await
        })?;

        Ok(())
    }

    fn delete(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            info!("Stopping application");
            stop_app(&mut client, &app_id).await?;
            info!("Application stopped");

            info!("Deleting application");
            let _delete_result = client
                .delete_application(DeleteAppRequest { id: app_id.clone() })
                .await
                .map_err(GRPCCallError)?
                .into_inner();
            info!("Deleted {}", &app_id);

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn list(&mut self, cloud: Cloud, list: &ListCommandArgs) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let response = client
                .list_applications(ListAppRequest {
                    limit: list.limit,
                    offset: list.offset,
                    name: list.name.clone(),
                    uuid: list.uuid.clone(),
                })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            let mut table = table!();

            for app in response.apps {
                if let Some(app_data) = app.app {
                    table.add_row(row![app.id, app_data.convert_to_table()]);
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

    fn status(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        self.runtime.block_on(async move {
            let mut client = get_cloud_client(&cloud).await?;
            let response = client
                .get_status(GetStatusRequest { app_id })
                .await
                .map_err(GRPCCallError)?
                .into_inner();

            let mut table = table!();

            table.add_row(row!["State", response.state]);
            match response.api_endpoint {
                None => {}
                Some(endpoint) => {
                    table.add_row(row!["API endpoint", format!("http://{}", endpoint)]);
                }
            }

            match response.rest_port {
                None => {}
                Some(port) => {
                    table.add_row(row!["REST Port", port.to_string()]);
                }
            }

            match response.grpc_port {
                None => {}
                Some(port) => {
                    table.add_row(row!["GRPC Port", port]);
                }
            }

            table.printstd();
            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }

    fn monitor(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        monitor_app(app_id, cloud.target_url, self.runtime.clone())
            .map_err(crate::errors::OrchestrationError::CloudError)
    }

    fn trace_logs(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        let target_url = cloud.target_url;

        self.runtime.block_on(async move {
            let mut client: DozerCloudClient<tonic::transport::Channel> =
                DozerCloudClient::connect(target_url).await?;
            let mut response = client
                .on_log_message(LogMessageRequest { app_id })
                .await?
                .into_inner();

            if let Some(next_message) = response.message().await? {
                info!("{:?}", next_message);
            }

            Ok::<(), CloudError>(())
        })?;

        Ok(())
    }
}
