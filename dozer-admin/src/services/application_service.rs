use super::constants;
use super::graph;
use crate::server::dozer_admin_grpc::StopRequest;
use crate::server::dozer_admin_grpc::StopResponse;
use crate::{
    db::{
        app::{Application, NewApplication},
        pool::DbPool,
        schema::apps::{self, dsl::*},
    },
    server::dozer_admin_grpc::{
        AppResponse, CreateAppRequest, ErrorResponse, GenerateGraphRequest, GenerateGraphResponse,
        GenerateYamlRequest, GenerateYamlResponse, GetAppRequest, ListAppRequest, ListAppResponse,
        Pagination, ParseRequest, ParseResponse, StartRequest, StartResponse, UpdateAppRequest,
    },
};
use diesel::prelude::*;
use diesel::{insert_into, QueryDsl, RunQueryDsl};
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::wrapped_statement_to_pipeline;
use dozer_orchestrator::Orchestrator;
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;
use std::collections::HashMap;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
pub struct AppService {
    db_pool: DbPool,
    apps: Arc<RwLock<HashMap<String, Arc<AtomicBool>>>>,
}
impl AppService {
    pub fn new(db_pool: DbPool) -> Self {
        Self {
            db_pool,
            apps: Arc::new(RwLock::new(HashMap::new())),
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

    pub fn parse(&self, input: ParseRequest) -> Result<ParseResponse, ErrorResponse> {
        let context = wrapped_statement_to_pipeline(&input.sql).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        Ok(ParseResponse {
            used_sources: context.used_sources.clone(),
            output_tables: context.output_tables_map.keys().cloned().collect(),
        })
    }

    pub fn generate(
        &self,
        input: GenerateGraphRequest,
    ) -> Result<GenerateGraphResponse, ErrorResponse> {
        //validate config
        let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&input.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let context = match &c.sql {
            Some(sql) => Some(
                wrapped_statement_to_pipeline(sql).map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?,
            ),
            None => None,
        };
        let g = graph::generate(context, &c)?;

        Ok(GenerateGraphResponse { graph: Some(g) })
    }

    pub fn generate_yaml(
        &self,
        input: GenerateYamlRequest,
    ) -> Result<GenerateYamlResponse, ErrorResponse> {
        //validate config

        let app = input.app.unwrap();
        let mut connections = vec![];
        let mut sources = vec![];
        let mut endpoints = vec![];

        for c in app.connections.iter() {
            connections.push(serde_yaml::to_string(c).unwrap());
        }

        for c in app.sources.iter() {
            sources.push(serde_yaml::to_string(c).unwrap());
        }

        for c in app.endpoints.iter() {
            endpoints.push(serde_yaml::to_string(c).unwrap());
        }

        Ok(GenerateYamlResponse {
            connections,
            sources,
            endpoints,
        })
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

    pub fn start_dozer(&self, input: StartRequest) -> Result<StartResponse, ErrorResponse> {
        let generated_id = uuid::Uuid::new_v4().to_string();

        //validate config
        let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&input.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        let running = Arc::new(AtomicBool::new(true));
        let r = running.clone();
        thread::spawn(move || {
            let mut dozer = Dozer::new(c);
            dozer.run_all(running).unwrap();
        });
        self.apps.write().insert(generated_id.clone(), r);
        Ok(StartResponse {
            success: true,
            id: generated_id,
        })
    }

    pub fn stop_dozer(&self, input: StopRequest) -> Result<StopResponse, ErrorResponse> {
        let i = input.id;
        self.apps.read().get(&i).map_or(
            Err(ErrorResponse {
                message: "Cannot find dozer with id : {i}".to_string(),
            }),
            |r| {
                r.store(false, Ordering::Relaxed);
                Ok(StopResponse { success: true })
            },
        )
    }
}
