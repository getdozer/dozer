use dozer_types::{constants::DEFAULT_HOME_DIR, serde_yaml};

use crate::{
    db::{app::AppDbService, pool::DbPool},
    server::dozer_admin_grpc::{
        App, CreateAppRequest, CreateAppResponse, ErrorResponse, GetAppRequest, GetAppResponse,
        ListAppRequest, ListAppResponse, Pagination, StartPipelineRequest, StartPipelineResponse,
        UpdateAppRequest, UpdateAppResponse,
    },
};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
pub struct AppService {
    db_pool: DbPool,
    dozer_path: String,
}
impl AppService {
    pub fn new(db_pool: DbPool, dozer_path: String) -> Self {
        Self {
            db_pool,
            dozer_path,
        }
    }
}
impl AppService {
    pub fn get_app(&self, input: GetAppRequest) -> Result<GetAppResponse, ErrorResponse> {
        if let Some(app_id) = input.app_id {
            let app =
                AppDbService::by_id(self.db_pool.clone(), app_id.to_owned()).map_err(|err| {
                    ErrorResponse {
                        message: err.to_string(),
                    }
                })?;

            Ok(GetAppResponse { app: Some(app) })
        } else {
            return Err(ErrorResponse {
                message: "app_id is missing".to_string(),
            });
        }
    }
    pub fn create(&self, input: CreateAppRequest) -> Result<CreateAppResponse, ErrorResponse> {
        let generated_id = uuid::Uuid::new_v4().to_string();

        //validate config
        let _res = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&input.config)
            .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        let app_info = App {
            name: input.app_name,
            id: generated_id,
            config: input.config,
            ..Default::default()
        };
        let app_info =
            AppDbService::save(app_info, self.db_pool.clone()).map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(CreateAppResponse {
            app: Some(app_info),
        })
    }
    pub fn list(&self, input: ListAppRequest) -> Result<ListAppResponse, ErrorResponse> {
        let data: (Vec<App>, Pagination) =
            AppDbService::list(self.db_pool.clone(), input.limit, input.offset).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                }
            })?;
        Ok(ListAppResponse {
            apps: data.0,
            pagination: Some(data.1),
        })
    }
    pub fn update_app(&self, input: UpdateAppRequest) -> Result<UpdateAppResponse, ErrorResponse> {
        let updated_app: App = AppDbService::update(self.db_pool.clone(), input.app_id, input.name)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(UpdateAppResponse {
            app: Some(updated_app),
        })
    }

    pub fn start_pipeline(
        &self,
        input: StartPipelineRequest,
    ) -> Result<StartPipelineResponse, ErrorResponse> {
        let response: GetAppResponse = self.get_app(GetAppRequest {
            app_id: Some(input.app_id),
        })?;
        let config: dozer_types::models::app_config::Config =
            serde_yaml::from_str(&response.app.unwrap().config).map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let path = Path::new(DEFAULT_HOME_DIR).join("api_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();

        let yaml_path = path.join(format!(
            "dozer-config-{:}-{:}.yaml",
            config.app_name,
            config.to_owned().id.unwrap()
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
                config.app_name,
                config.id.unwrap()
            )))
            .expect("Couldn't open file");
        let errors_log_file = dozer_log_file.try_clone().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        // assumption 2 bin: dozer-admin + dozer always in same dir
        let path_to_bin = &self.dozer_path;
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
