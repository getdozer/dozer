use super::constants;
use super::graph;

use crate::db::{
    app::{Application, NewApplication},
    pool::DbPool,
    schema::apps::{self, dsl::*},
};
use diesel::prelude::*;
use diesel::{insert_into, QueryDsl, RunQueryDsl};
use dozer_api::grpc::internal::internal_pipeline_client::InternalPipelineClient;
use dozer_orchestrator::shutdown;
use dozer_orchestrator::shutdown::ShutdownSender;
use dozer_orchestrator::simple::SimpleOrchestrator as Dozer;
use dozer_orchestrator::wrapped_statement_to_pipeline;
use dozer_orchestrator::Orchestrator;
use dozer_types::grpc_types::admin::{
    AppResponse, CreateAppRequest, ErrorResponse, GenerateGraphRequest, GenerateGraphResponse,
    GenerateYamlRequest, GenerateYamlResponse, GetAppRequest, ListAppRequest, ListAppResponse,
    Pagination, ParseRequest, ParseResponse, ParseYamlRequest, ParseYamlResponse, StartRequest,
    StartResponse, UpdateAppRequest,
};
use dozer_types::grpc_types::admin::{
    File, ListFilesResponse, LogMessage, StatusUpdate, StopRequest,
};
use dozer_types::grpc_types::admin::{StatusUpdateRequest, StopResponse};
use dozer_types::parking_lot::RwLock;
use dozer_types::serde_yaml;
use tokio::runtime::Runtime;

use std::collections::HashMap;

use dozer_types::grpc_types::admin::SaveFilesRequest;
use dozer_types::grpc_types::admin::SaveFilesResponse;
use dozer_types::log::warn;
use dozer_types::models::api_config::GrpcApiOptions;
use glob::glob;
use notify::event::ModifyKind::Data;
use notify::EventKind::Modify;
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};
use tokio::time::interval;
use tonic::{Code, Status};

#[derive(Clone)]
pub struct AppService {
    db_pool: DbPool,
    pub apps: Arc<RwLock<HashMap<String, ShutdownSender>>>,
    runtime: Arc<Runtime>,
}
impl AppService {
    pub fn new(db_pool: DbPool) -> Self {
        let runtime = Runtime::new().expect("Failed to create runtime");
        Self {
            db_pool,
            apps: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(runtime),
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

    pub fn parse_sql(&self, input: ParseRequest) -> Result<ParseResponse, ErrorResponse> {
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
        let mut dozer = Dozer::new(c, self.runtime.clone());
        let (shutdown_sender, shutdown_receiver) = shutdown::new(&dozer.runtime);
        thread::spawn(move || {
            let _guard = dozer_tracing::init_telemetry_closure(
                Some(&dozer.config.app_name.clone()),
                None,
                || -> Result<(), ErrorResponse> {
                    dozer.run_all(shutdown_receiver).unwrap();

                    Ok(())
                },
            );
        });
        self.apps
            .write()
            .insert(generated_id.clone(), shutdown_sender);
        Ok(StartResponse {
            success: true,
            id: generated_id,
        })
    }

    pub fn stop_dozer(&self, input: StopRequest) -> Result<StopResponse, ErrorResponse> {
        let i = input.id;
        self.apps.write().remove(&i).map_or(
            Err(ErrorResponse {
                message: "Cannot find dozer with id : {i}".to_string(),
            }),
            |shutdown| {
                shutdown.shutdown();
                Ok(StopResponse { success: true })
            },
        )
    }

    pub fn list_files(&self) -> Result<ListFilesResponse, ErrorResponse> {
        let mut files = vec![];
        let files_glob = glob("*.yaml").map_err(|e| ErrorResponse {
            message: e.to_string(),
        })?;

        for entry in files_glob {
            match entry {
                Ok(path) => {
                    files.push(File {
                        name: format!("{:?}", path.display()),
                        content: fs::read_to_string(path).map_err(|e| ErrorResponse {
                            message: e.to_string(),
                        })?,
                    });
                }
                Err(e) => {
                    return Err(ErrorResponse {
                        message: e.to_string(),
                    })
                }
            }
        }

        Ok(ListFilesResponse { files })
    }

    pub fn save_files(
        &self,
        request: SaveFilesRequest,
    ) -> Result<SaveFilesResponse, ErrorResponse> {
        for file in request.files {
            let mut fs_file = fs::File::create(file.name).unwrap();
            fs_file.write_all(file.content.as_bytes()).unwrap();
        }

        Ok(SaveFilesResponse {})
    }

    pub async fn read_logs(log_tx: tokio::sync::mpsc::Sender<Result<LogMessage, Status>>) {
        let mut position = 0;

        let (tx, rx) = std::sync::mpsc::channel();

        match RecommendedWatcher::new(tx, Config::default()) {
            Ok(mut watcher) => {
                let p = std::path::Path::new("./log/dozer.log");
                let start_watch_result = watcher.watch(p.as_ref(), RecursiveMode::Recursive);

                if start_watch_result.is_err() {
                    let _ = log_tx
                        .send(Err(Status::new(Code::Internal, "Failed to start watch")))
                        .await;
                    return;
                }

                for res in rx.into_iter().flatten() {
                    if let Event {
                        kind: Modify(Data(_)),
                        ..
                    } = res
                    {
                        let file_result = fs::File::open("./log/dozer.log");
                        if let Ok(mut file) = file_result {
                            let seek_result = file.seek(SeekFrom::Start(position));
                            match seek_result {
                                Ok(_) => {
                                    if let Ok(metadata) = file.metadata() {
                                        position = metadata.len();
                                        let reader = BufReader::new(file);
                                        for message in reader.lines().flatten() {
                                            let log_message = LogMessage { message };

                                            if (log_tx.send(Ok(log_message)).await).is_err() {
                                                // receiver dropped
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Seek error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let _ = log_tx
                    .send(Err(Status::new(Code::Internal, e.to_string())))
                    .await;
            }
        }
    }

    pub async fn stream_status_update(
        request: StatusUpdateRequest,
        tx: tokio::sync::mpsc::Sender<Result<StatusUpdate, Status>>,
    ) {
        let mut retries_left = 100;
        let mut retry_interval = interval(Duration::from_millis(1000));

        let grpc_options = GrpcApiOptions {
            host: request.host.unwrap_or("0.0.0.0".to_string()),
            port: request.port.unwrap_or(50053),
            ..Default::default()
        };

        while retries_left > 0 {
            if let Ok(mut internal_pipeline_client) =
                InternalPipelineClient::new(&grpc_options).await
            {
                if let Ok((mut status_updates_receiver, future)) =
                    internal_pipeline_client.stream_status_update().await
                {
                    tokio::spawn(future);

                    while let Ok(msg) = status_updates_receiver.recv().await {
                        let status_msg = StatusUpdate {
                            source: msg.source,
                            r#type: msg.r#type,
                            count: msg.count,
                        };
                        if (tx.send(Ok(status_msg)).await).is_err() {
                            // receiver dropped
                            retries_left = 0;
                            break;
                        }
                    }
                }
            } else {
                retries_left -= 1;

                retry_interval.tick().await;
            }
        }
    }
}
