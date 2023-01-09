use crate::{
    cli::utils::kill_process_at,
    db::{application::AppDbService, persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        ApplicationInfo, CreateAppRequest, CreateAppResponse, ErrorResponse, GetAppRequest,
        GetAppResponse, ListAppRequest, ListAppResponse, Pagination, StartPipelineRequest,
        StartPipelineResponse, UpdateAppRequest, UpdateAppResponse,
    },
};
use dozer_types::{
    constants::DEFAULT_HOME_DIR,
    models::{
        api_config::ApiConfig, api_endpoint::ApiEndpoint, app_config::Config, source::Source,
    },
    serde_yaml,
};
use std::path::Path;
use std::process::Command;
use std::{fs, process::Stdio};
pub struct AppService {
    db_pool: DbPool,
}
impl AppService {
    pub fn new(db_pool: DbPool, dozer_path: String) -> Self {
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
                    home_dir: app_by_id.home_dir,
                    // TODO: Get this from db
                    flags: Default::default(),
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
        let path = Path::new(DEFAULT_HOME_DIR).join("yaml_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();
        let config = app_detail.to_owned();
        let yaml_file_name = format!(
            "dozer-config-{:}-{:}.yaml",
            config.app_name,
            config.to_owned().id.unwrap()
        );
        let yaml_path = path.join(yaml_file_name.clone());
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&yaml_path)
            .expect("Couldn't open file");
        serde_yaml::to_writer(f, &config).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let dozer_log_path = path;
        let log_file_path = dozer_log_path.join(format!(
            "logs-{:}-{:}.txt",
            config.app_name,
            config.id.to_owned().unwrap()
        ));
        let dozer_log_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(log_file_path)
            .expect("Couldn't open file");

        let docker_name = format!("dozer-{:}-{:}", config.app_name, config.id.unwrap());
        // termiate docker if exists:
        let docker_terminate_command = Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(docker_name.to_owned())
            .output()
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        // start docker:
        let docker_image_address = "public.ecr.aws/k7k6x1d4/dozer:latest";
        let pull_docker_command = Command::new("docker")
            .arg("pull")
            .arg(docker_image_address)
            .output()
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        // run docker image:
        let binding = fs::canonicalize(&yaml_path).unwrap().to_owned();
        let absolute_path = binding.to_str().unwrap();
        let rest_port = config.api.to_owned().unwrap().rest.unwrap_or_default().port;
        let grpc_port = config.api.to_owned().unwrap().grpc.unwrap_or_default().port;
        let internal_grpc_port = config
            .api
            .to_owned()
            .unwrap_or_default()
            .pipeline_internal
            .unwrap_or_default()
            .port;

        let run_docker_command = Command::new("docker")
            .arg("run")
            .arg("--name")
            .arg(docker_name.to_owned())
            .arg("-d")
            .arg("-p")
            .arg(format!("{:}:{:}", rest_port, rest_port)) // rest port
            .arg("-p")
            .arg(format!("{:}:{:}", grpc_port, grpc_port)) // grpc port
            .arg("-p")
            .arg(format!("{:}:{:}", internal_grpc_port, internal_grpc_port)) // internal grpc port
            .arg("-v")
            .arg(format!("{:}:/usr/dozer/dozer-config.yaml", absolute_path))
            .arg(docker_image_address)
            .arg("dozer")
            .output()
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        if !run_docker_command.status.success() {
            if let Ok(err) = String::from_utf8(run_docker_command.stderr) {
                return Err(ErrorResponse { message: err });
            }
        }
        if let Ok(output) = String::from_utf8(run_docker_command.stdout) {
            // write to logs
            let stream_logs_to_file = Command::new("docker")
                .arg("logs")
                .arg("-f")
                .arg(output.trim())
                .stdout(Stdio::from(dozer_log_file))
                .spawn()
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;
        }

        Ok(StartPipelineResponse {
            success: run_docker_command.status.success(),
        })
    }
}
