use dozer_types::{models::app_config::Config, serde_yaml};

use crate::{
    db::{application::AppDbService, pool::DbPool},
    server::dozer_admin_grpc::{
        ApplicationInfo, CreateAppRequest, CreateAppResponse, ErrorResponse, GetAppRequest,
        GetAppResponse, ListAppRequest, ListAppResponse, Pagination, StartPipelineRequest,
        StartPipelineResponse, UpdateAppRequest, UpdateAppResponse,
    },
};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
pub struct AppService {
    db_pool: DbPool,
}
impl AppService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}
impl AppService {
    pub fn get_app(&self, _input: GetAppRequest) -> Result<GetAppResponse, ErrorResponse> {
        todo!()
    }
    pub fn create(&self, input: CreateAppRequest) -> Result<CreateAppResponse, ErrorResponse> {
        let app_info = ApplicationInfo {
            name: input.app_name,
            ..Default::default()
        };
        let app_info =
            AppDbService::save(app_info, self.db_pool.clone()).map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(CreateAppResponse {
            data: Some(app_info),
        })
    }
    pub fn list(&self, input: ListAppRequest) -> Result<ListAppResponse, ErrorResponse> {
        let data: (Vec<ApplicationInfo>, Pagination) =
            AppDbService::list(self.db_pool.clone(), input.limit, input.offset).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        Ok(ListAppResponse {
            data: data.0,
            pagination: Some(data.1),
        })
    }
    pub fn update_app(&self, input: UpdateAppRequest) -> Result<UpdateAppResponse, ErrorResponse> {
        let updated_app: ApplicationInfo =
            AppDbService::update(self.db_pool.clone(), input.app_id, input.name).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        Ok(UpdateAppResponse {
            data: Some(updated_app),
        })
    }

    pub fn start_pipeline(
        &self,
        input: StartPipelineRequest,
    ) -> Result<StartPipelineResponse, ErrorResponse> {
        let app_detail =
            AppDbService::detail(self.db_pool.clone(), input.app_id).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        let path = Path::new("./.dozer").join("api_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();
        let config = Config::try_from(app_detail.to_owned()).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let yaml_path = path.join(format!(
            "dozer-config-{:}-{:}.yaml",
            app_detail.app.name, app_detail.app.id
        ));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&yaml_path)
            .expect("Couldn't open file");
        serde_yaml::to_writer(f, &config).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let dozer_log_path = path;
        let dozer_log_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(dozer_log_path.join(format!(
                "logs-{:}-{:}.txt",
                app_detail.app.name, app_detail.app.id
            )))
            .expect("Couldn't open file");
        let errors_log_file = dozer_log_file.try_clone().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        // kill process at port 8080 50051 lsof -t -i:8080 | xargs -r kill
        let mut check_ports_used = Command::new("lsof");
        check_ports_used.args(["-t", "-i:50051"]);
        let check_port_result = check_ports_used
            .output()
            .expect("failed to execute process");
        let check_port_result_str = String::from_utf8(check_port_result.stdout).unwrap();
        if !check_port_result_str.is_empty() {
            let ports: Vec<String> = check_port_result_str
                .split('\n')
                .into_iter()
                .map(|s| s.to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let _clear_grpc_port_command = Command::new("kill")
                .args(["-9", &ports[ports.len() - 1]])
                .output()
                .unwrap();
        }

        let path_to_bin = concat!(env!("OUT_DIR"), "/dozer");
        let _execute_cli_output = Command::new(path_to_bin)
            .arg("-c")
            .arg(yaml_path.as_path().to_str().unwrap())
            .stdout(Stdio::from(dozer_log_file))
            .stderr(Stdio::from(errors_log_file))
            .spawn()
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(StartPipelineResponse { success: true })
    }
}
