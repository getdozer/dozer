use dozer_types::{
    constants::DEFAULT_HOME_DIR,
    models::{
        api_config::ApiConfig, api_endpoint::ApiEndpoint, app_config::Config, source::Source,
    },
    serde_yaml,
};

use crate::{
    cli::utils::kill_process_at,
    db::{application::AppDbService, persistable::Persistable, pool::DbPool},
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
    pub fn get_app(&self, input: GetAppRequest) -> Result<GetAppResponse, ErrorResponse> {
        if let Some(app_id) = input.app_id {
            let app_by_id =
                AppDbService::by_id(self.db_pool.clone(), app_id.to_owned()).map_err(|err| {
                    ErrorResponse {
                        message: err.to_string(),
                    }
                })?;
            let api_config =
                ApiConfig::by_id(self.db_pool.clone(), app_by_id.id, app_id.to_owned()).map_err(
                    |err| ErrorResponse {
                        message: err.to_string(),
                    },
                )?;
            let sources = Source::list(self.db_pool.clone(), app_id.to_owned(), None, None)
                .map_err(|err| ErrorResponse {
                    message: err.to_string(),
                })?;
            let endpoints = ApiEndpoint::list(self.db_pool.clone(), app_id.to_owned(), None, None)
                .map_err(|err| ErrorResponse {
                    message: err.to_string(),
                })?;
            let connections = dozer_types::models::connection::Connection::list(
                self.db_pool.clone(),
                app_id.to_owned(),
                None,
                None,
            )
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
            Ok(GetAppResponse {
                data: Some(Config {
                    id: Some(app_id),
                    app_name: app_by_id.name,
                    api: Some(api_config),
                    connections: connections.0,
                    sources: sources.0,
                    endpoints: endpoints.0,
                }),
            })
        } else {
            // get default app
            todo!()
        }
    }
    pub fn create(&self, input: CreateAppRequest) -> Result<CreateAppResponse, ErrorResponse> {
        let generated_id = uuid::Uuid::new_v4().to_string();

        let app_info = ApplicationInfo {
            name: input.app_name,
            id: generated_id,
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
        let app_detail_result: GetAppResponse = self.get_app(GetAppRequest {
            app_id: Some(input.app_id),
        })?;
        let app_detail: dozer_types::models::app_config::Config = app_detail_result.data.unwrap();
        let path = Path::new(DEFAULT_HOME_DIR).join("api_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();
        let config = app_detail.to_owned();
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

        // kill process at port 8080 50051 lsof -t -i:8080 | xargs -r kill
        if let Some(api_config) = app_detail.api {
            kill_process_at(api_config.grpc.unwrap_or_default().port as u16);
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
