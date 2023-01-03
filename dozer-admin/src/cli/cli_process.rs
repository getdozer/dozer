use std::{fs::File, path::Path, process::Command};

use super::{
    utils::{init_db_with_config, init_internal_pipeline_client, kill_process_at, reset_db},
    AdminCliConfig,
};
use crate::server;
use dozer_orchestrator::internal_grpc::GetAppConfigRequest;
use dozer_types::{models::app_config::Config, serde_yaml};

pub struct CliProcess {
    pub config: AdminCliConfig,
}
impl CliProcess {
    fn get_internal_config(&mut self) {
        // priority read config from home_dir
        let home_dir = self.config.home_dir.to_owned();
        let path = Path::new(&home_dir).join("internal_config/config.yaml");
        if path.exists() {
            let f = File::open(path).expect("Couldn't open file");
            let reader_result = serde_yaml::from_reader::<File, serde_yaml::Value>(f);
            if let Ok(config) = reader_result {
                let pipeline_internal_value = &config["pipeline_internal"];
                let pipeline_internal =
                    serde_yaml::from_value(pipeline_internal_value.to_owned()).unwrap_or_default();
                let api_internal_value = &config["api_internal"];
                let api_internal =
                    serde_yaml::from_value(api_internal_value.to_owned()).unwrap_or_default();
                self.config.api_internal = api_internal;
                self.config.pipeline_internal = pipeline_internal;
            }
        }
    }

    fn start_ui_server(&self) {
        let ui_path = Path::new(&self.config.ui_path);
        if ui_path.exists() {
            // execute command serve
            Command::new("serve")
                .arg("-s")
                .arg(&self.config.ui_path)
                .spawn()
                .expect("Start ui server failed");
        }
    }
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.get_internal_config();
        let internal_pipeline_config = self.config.to_owned().pipeline_internal;
        let client_connect_result = init_internal_pipeline_client(internal_pipeline_config).await;
        let mut dozer_config: Config = Config::default();
        if let Ok(mut client) = client_connect_result {
            let response = client.get_config(GetAppConfigRequest {}).await?;
            let config = response.into_inner();
            dozer_config = config.data.unwrap();
        }
        reset_db();
        init_db_with_config(dozer_config);
        kill_process_at(3000);
        kill_process_at(self.config.to_owned().port as u16);

        // start ui
        self.start_ui_server();
        server::start_admin_server(self.config.to_owned()).await?;
        Ok(())
    }
}
