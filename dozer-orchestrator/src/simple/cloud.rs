use crate::cli::cloud::Cloud;
use crate::cloud_helper::list_files;
use crate::errors::{DeployError, OrchestrationError};
use crate::simple::SimpleOrchestrator;
use crate::CloudOrchestrator;
use dozer_types::grpc_types::cloud::{
    dozer_cloud_client::DozerCloudClient, CreateAppRequest, GetStatusRequest, ListAppRequest,
    StartRequest,
};
use dozer_types::log::info;
use dozer_types::prettytable::{row, table};

impl CloudOrchestrator for SimpleOrchestrator {
    // TODO: Deploy Dozer application using local Dozer configuration
    fn deploy(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        let target_url = cloud.target_url;
        // let username = match deploy.username {
        //     Some(u) => u,
        //     None => String::new(),
        // };
        // let _password = match deploy.password {
        //     Some(p) => p,
        //     None => String::new(),
        // };
        info!("Deployment target url: {:?}", target_url);
        // info!("Authenticating for username: {:?}", username);
        // info!("Local dozer configuration path: {:?}", config_path);
        // calling the target url with the config fetched
        self.runtime.block_on(async move {
            // 1. CREATE application
            let mut client: DozerCloudClient<tonic::transport::Channel> =
                DozerCloudClient::connect(target_url).await?;
            let files = list_files()?;
            let response = client
                .create_application(CreateAppRequest { files })
                .await?
                .into_inner();

            info!("Application created with id: {:?}", &response.id);
            // 2. START application
            info!("Deploying application");
            let deploy_result = client
                .start_dozer(StartRequest {
                    id: response.id.clone(),
                })
                .await?
                .into_inner();
            info!("Deployed {}", &response.id);
            match deploy_result.api_endpoint {
                None => {}
                Some(endpoint) => info!("Endpoint: http://{endpoint}"),
            }

            Ok::<(), DeployError>(())
        })?;
        Ok(())
    }

    fn list(&mut self, cloud: Cloud) -> Result<(), OrchestrationError> {
        let target_url = cloud.target_url;

        self.runtime.block_on(async move {
            // 1. CREATE application
            let mut client: DozerCloudClient<tonic::transport::Channel> =
                DozerCloudClient::connect(target_url).await?;
            let response = client
                .list_applications(ListAppRequest {
                    limit: None,
                    offset: None,
                })
                .await?
                .into_inner();

            let mut table = table!();

            for app in response.apps {
                table.add_row(row![app.id, app.app.unwrap().convert_to_table()]);
            }

            table.printstd();

            Ok::<(), DeployError>(())
        })?;

        Ok(())
    }

    fn status(&mut self, cloud: Cloud, app_id: String) -> Result<(), OrchestrationError> {
        let target_url = cloud.target_url;

        self.runtime.block_on(async move {
            // 1. CREATE application
            let mut client: DozerCloudClient<tonic::transport::Channel> =
                DozerCloudClient::connect(target_url).await?;
            let response = client
                .get_status(GetStatusRequest { id: app_id })
                .await?
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
            Ok::<(), DeployError>(())
        })?;

        Ok(())
    }
}
