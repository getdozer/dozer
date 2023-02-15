use dozer_types::{constants::DEFAULT_HOME_DIR, serde_yaml};

use crate::{
    db::{
        app::{Application, NewApplication},
        pool::DbPool,
        schema::apps::{self, dsl::*},
    },
    server::dozer_admin_grpc::{
        AppResponse, CreateAppRequest, ErrorResponse, GetAppRequest, ListAppRequest,
        ListAppResponse, Pagination, StartPipelineRequest, StartPipelineResponse, UpdateAppRequest,
    },
};
use diesel::prelude::*;
use diesel::{insert_into, QueryDsl, RunQueryDsl};
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

use super::constants;
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
    pub fn get_app(&self, input: GetAppRequest) -> Result<AppResponse, ErrorResponse> {
        if let Some(app_id) = input.app_id {
            let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
            let app: Application =
                apps.find(app_id)
                    .first(&mut db)
                    .map_err(|op| ErrorResponse {
                        message: op.to_string(),
                    })?;

            let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&app.config)
                .map_err(|err| ErrorResponse {
                    message: err.to_string(),
                })?;
            Ok(AppResponse {
                id: app.id,
                app: Some(c),
            })
        } else {
            Err(ErrorResponse {
                message: "app_id is missing".to_string(),
            })
        }
    }

    pub fn create(&self, input: CreateAppRequest) -> Result<AppResponse, ErrorResponse> {
        let generated_id = uuid::Uuid::new_v4().to_string();

        //validate config
        let res = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&input.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let new_app = NewApplication {
            name: res.app_name,
            id: generated_id.clone(),
            config: input.config,
        };

        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let _inserted = insert_into(apps)
            .values(&new_app)
            .execute(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        // query

        let result: Application =
            apps.find(generated_id)
                .first(&mut db)
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;

        let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&result.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        Ok(AppResponse {
            id: result.id,
            app: Some(c),
        })
    }

    pub fn list(&self, input: ListAppRequest) -> Result<ListAppResponse, ErrorResponse> {
        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let offset = input.offset.unwrap_or(constants::OFFSET);
        let limit = input.limit.unwrap_or(constants::LIMIT);
        let results: Vec<Application> = apps
            .offset(offset.into())
            .order_by(apps::created_at.asc())
            .limit(limit.into())
            .load(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let total: i64 = apps
            .count()
            .get_result(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        let applications: Vec<AppResponse> = results
            .iter()
            .map(|result| {
                let c =
                    serde_yaml::from_str::<dozer_types::models::app_config::Config>(&result.config)
                        .unwrap();

                AppResponse {
                    id: result.id.clone(),
                    app: Some(c),
                }
            })
            .collect();

        Ok(ListAppResponse {
            apps: applications,
            pagination: Some(Pagination {
                limit,
                total: total.try_into().unwrap(),
                offset,
            }),
        })
    }
    pub fn update_app(&self, request: UpdateAppRequest) -> Result<AppResponse, ErrorResponse> {
        //validate config
        let res = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&request.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        let _ = diesel::update(apps)
            .filter(id.eq(request.id.to_owned()))
            .set((name.eq(res.app_name), config.eq(request.config)))
            .execute(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        // load back
        let app: Application =
            apps.find(request.id)
                .first(&mut db)
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;

        let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&app.config)
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

        Ok(AppResponse {
            id: app.id,
            app: Some(c),
        })
    }

    pub fn start_pipeline(
        &self,
        input: StartPipelineRequest,
    ) -> Result<StartPipelineResponse, ErrorResponse> {
        let response: AppResponse = self.get_app(GetAppRequest {
            app_id: Some(input.app_id),
        })?;
        let c: dozer_types::models::app_config::Config = response.app.unwrap();

        let path = Path::new(DEFAULT_HOME_DIR).join("api_config");
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
        fs::create_dir_all(&path).unwrap();

        let yaml_path = path.join(format!("dozer-config-{:}.yaml", response.id));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&yaml_path)
            .expect("Couldn't open file");
        serde_yaml::to_writer(f, &c).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let dozer_log_path = path;
        let dozer_log_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(dozer_log_path.join(format!("logs-{:}-{:}.txt", c.app_name, response.id)))
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
