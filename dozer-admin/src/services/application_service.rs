use super::constants;
use super::graph;

use crate::db::{
    app::{Application, NewApplication},
    pool::DbPool,
    schema::apps::{self, dsl::*},
};
use diesel::prelude::*;
use diesel::{insert_into, QueryDsl, RunQueryDsl};
use dozer_orchestrator::pipeline::{LogSinkSettings, PipelineBuilder};
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::wrapped_statement_to_pipeline;
use dozer_orchestrator::Orchestrator;
use dozer_types::grpc_types::admin::StopResponse;
use dozer_types::grpc_types::admin::{
    AppResponse, CreateAppRequest, ErrorResponse, GenerateGraphRequest, GenerateGraphResponse,
    GenerateYamlRequest, GenerateYamlResponse, GetAppRequest, ListAppRequest, ListAppResponse,
    Pagination, ParseRequest, ParseResponse, ParseYamlRequest, ParseYamlResponse, StartRequest,
    StartResponse, UpdateAppRequest,
};
use dozer_types::grpc_types::admin::{ParsePipelineRequest, ParsePipelineResponse, StopRequest};
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;
use std::collections::HashMap;
use std::path::Path;

use crate::db::connection::DbConnection;
use dozer_types::indicatif::MultiProgress;
use dozer_types::log::{error, info};
use dozer_types::models::connection::Connection;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

use crate::db::schema::connections::dsl::connections;

use dozer_core::dag_schemas::DagSchemas;
use dozer_types::grpc_types::admin;

#[derive(Clone)]
pub struct AppService {
    db_pool: DbPool,
    pub apps: Arc<RwLock<HashMap<String, Arc<AtomicBool>>>>,
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
                config: app.config,
            })
        } else {
            Err(ErrorResponse {
                message: "app_id is missing".to_string(),
            })
        }
    }

    pub fn parse_sql(&self, input: ParseRequest) -> Result<ParseResponse, ErrorResponse> {
        let context = wrapped_statement_to_pipeline(&input.sql).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        Ok(ParseResponse {
            used_sources: context.used_sources.clone(),
            output_tables: context.output_tables_map.keys().cloned().collect(),
        })
    }

    pub async fn parse_pipeline(
        &self,
        input: ParsePipelineRequest,
    ) -> Result<ParsePipelineResponse, ErrorResponse> {
        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;

        let app: Application =
            apps.find(input.app_id)
                .first(&mut db)
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;

        let c = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&app.config)
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

        let db_conns: Vec<DbConnection> = connections.load(&mut db).map_err(|err| {
            error!("Error fetching connections: {}", err);
            ErrorResponse {
                message: err.to_string(),
            }
        })?;

        let mut conns = vec![];
        for db_conn in db_conns {
            let connection = Connection::try_from(db_conn).map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

            conns.push(connection);
        }

        let result: Result<
            (
                Vec<std::string::String>,
                HashMap<std::string::String, admin::TableInfo>,
            ),
            ErrorResponse,
        > = thread::spawn(move || {
            let pipeline_home_dir = AsRef::<Path>::as_ref(&c.home_dir);
            let builder = PipelineBuilder::new(
                &conns,
                &c.sources,
                Some(&input.sql),
                &c.endpoints,
                pipeline_home_dir,
                MultiProgress::new(),
            );

            let calculated_sources = builder.calculate_sources().map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

            let settings = LogSinkSettings {
                pipeline_dir: Default::default(),
                file_buffer_capacity: 0,
            };
            let dag = builder.build(settings).unwrap();

            let dag_schemas = DagSchemas::new(dag).unwrap();
            let sink_schemas = dag_schemas.get_sink_schemas();

            let mut transformed_sources = HashMap::new();
            for (schema_name, schema) in sink_schemas {
                let endpoint = c.endpoints.iter().find(|e| e.name == schema_name);
                if let Some(endpoint_info) = endpoint {
                    if calculated_sources
                        .transformed_sources
                        .contains(&endpoint_info.table_name)
                    {
                        let mut columns = vec![];
                        for field in schema.fields {
                            columns.push(admin::ColumnInfo {
                                column_name: field.name,
                                is_nullable: field.nullable,
                            });
                        }
                        transformed_sources.insert(
                            endpoint_info.table_name.clone(),
                            admin::TableInfo {
                                columns,
                                table_name: endpoint_info.table_name.clone(),
                            },
                        );
                    }
                }
            }
            Ok((calculated_sources.original_sources, transformed_sources))
        })
        .join()
        .map_err(|_op| ErrorResponse {
            message: "Parsing pipeline failed".to_string(),
        })?;

        match result {
            Ok((used_sources, transformed_sources)) => Ok(ParsePipelineResponse {
                used_sources,
                transformed_sources,
            }),
            Err(e) => Err(e),
        }
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
        let mut connections_list = vec![];
        let mut sources = vec![];
        let mut endpoints = vec![];

        for c in app.connections.iter() {
            connections_list.push(serde_yaml::to_string(c).unwrap());
        }

        for c in app.sources.iter() {
            sources.push(serde_yaml::to_string(c).unwrap());
        }

        for c in app.endpoints.iter() {
            endpoints.push(serde_yaml::to_string(c).unwrap());
        }

        Ok(GenerateYamlResponse {
            connections: connections_list,
            sources,
            endpoints,
        })
    }

    pub fn parse_yaml(&self, input: ParseYamlRequest) -> Result<ParseYamlResponse, ErrorResponse> {
        let app = serde_yaml::from_str::<dozer_types::models::app_config::Config>(&input.config)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(ParseYamlResponse { app: Some(app) })
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
            config: result.config,
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
                    config: result.config.clone(),
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

        info!("{:?}", request.config);
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
            config: app.config,
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
