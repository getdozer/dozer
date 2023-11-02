use std::{pin::pin, time::Duration};

use deno_runtime::deno_core::futures::future::{join, select, Either};

use super::*;

#[test]
fn test_lambda_runtime() {
    // env_logger::init();
    let tokio_runtime = Arc::new(
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
    );
    tokio_runtime.block_on(test_lambda_runtime_impl(tokio_runtime.clone()));
}

async fn test_lambda_runtime_impl(tokio_runtime: Arc<tokio::runtime::Runtime>) {
    let (app_url, app_server) = mock::start_mock_internal_pipeline_server().await;
    let lambda_modules = vec![JavaScriptLambda {
        endpoint: mock::mock_endpoint(),
        module: "src/js/test_lambda.js".to_string(),
    }];
    let lambda_runtime = Runtime::new(tokio_runtime, app_url, lambda_modules, Default::default());
    let (lambda_runtime, app_server) = match select(pin!(lambda_runtime), app_server).await {
        Either::Left((lambda_runtime, app_server)) => (lambda_runtime.unwrap(), app_server),
        Either::Right((app_server, _)) => {
            panic!("unexpected app server error: {:?}", app_server);
        }
    };
    tokio::time::timeout(
        Duration::from_millis(10),
        join(lambda_runtime.run(), app_server),
    )
    .await
    .unwrap_err();
}

mod mock {
    use std::{
        future::Future,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    };

    use deno_runtime::deno_core::futures::{stream::BoxStream, FutureExt, StreamExt};
    use dozer_log::{
        replication::{self, LogOperation},
        schemas::EndpointSchema,
        tokio::net::TcpListener,
    };
    use dozer_types::{
        bincode,
        grpc_types::internal::{
            internal_pipeline_service_server::{
                InternalPipelineService, InternalPipelineServiceServer,
            },
            storage_response::Storage,
            BuildRequest, BuildResponse, DescribeApplicationResponse, GetIdResponse, LocalStorage,
            LogRequest, LogResponse, StorageRequest, StorageResponse,
        },
        serde_json,
        tonic::{
            self, async_trait,
            transport::{server::TcpIncoming, Server},
            Request, Response, Status, Streaming,
        },
        types::{Field, FieldDefinition, FieldType, Operation, Record, Schema, SourceDefinition},
    };

    pub async fn start_mock_internal_pipeline_server() -> (
        String,
        impl Future<Output = Result<(), tonic::transport::Error>> + Unpin,
    ) {
        let listener = TcpListener::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        (
            url,
            Server::builder()
                .add_service(InternalPipelineServiceServer::new(
                    MockInternalPipelineServer,
                ))
                .serve_with_incoming(incoming)
                .boxed(),
        )
    }

    struct MockInternalPipelineServer;

    pub fn mock_endpoint() -> String {
        "mock".to_string()
    }

    fn mock_build_response() -> BuildResponse {
        let schema = EndpointSchema {
            path: "mock".to_string(),
            schema: Schema {
                fields: vec![FieldDefinition {
                    name: "mock_field".to_string(),
                    typ: FieldType::UInt,
                    nullable: false,
                    source: SourceDefinition::Dynamic,
                }],
                primary_index: vec![0],
            },
            secondary_indexes: vec![],
            enable_token: false,
            enable_on_event: true,
            connections: Default::default(),
        };
        BuildResponse {
            schema_string: serde_json::to_string(&schema).unwrap(),
        }
    }

    fn err_not_found<T>(endpoint: &str) -> Result<T, Status> {
        Err(Status::not_found(format!(
            "Endpoint {} not found",
            endpoint
        )))
    }

    #[async_trait]
    impl InternalPipelineService for MockInternalPipelineServer {
        async fn get_id(&self, _: Request<()>) -> Result<Response<GetIdResponse>, Status> {
            Ok(Response::new(GetIdResponse {
                id: "mock".to_string(),
            }))
        }

        async fn describe_storage(
            &self,
            request: Request<StorageRequest>,
        ) -> Result<Response<StorageResponse>, Status> {
            let endpoint = request.into_inner().endpoint;
            if endpoint == mock_endpoint() {
                Ok(Response::new(StorageResponse {
                    storage: Some(Storage::Local(LocalStorage {
                        root: "mock".to_string(),
                    })),
                }))
            } else {
                err_not_found(&endpoint)
            }
        }

        async fn describe_application(
            &self,
            _: Request<()>,
        ) -> Result<Response<DescribeApplicationResponse>, Status> {
            Ok(Response::new(DescribeApplicationResponse {
                endpoints: [(mock_endpoint(), mock_build_response())]
                    .into_iter()
                    .collect(),
            }))
        }

        async fn describe_build(
            &self,
            request: Request<BuildRequest>,
        ) -> Result<Response<BuildResponse>, Status> {
            let endpoint = request.into_inner().endpoint;
            if endpoint == mock_endpoint() {
                Ok(Response::new(mock_build_response()))
            } else {
                err_not_found(&endpoint)
            }
        }

        type GetLogStream = BoxStream<'static, Result<LogResponse, Status>>;

        async fn get_log(
            &self,
            requests: Request<Streaming<LogRequest>>,
        ) -> Result<Response<Self::GetLogStream>, Status> {
            let response = requests.into_inner().map(|request| {
                request.and_then(|request| {
                    if request.endpoint == mock_endpoint() {
                        let operations = (request.start..request.end)
                            .map(|index| LogOperation::Op {
                                op: Operation::Insert {
                                    new: Record {
                                        values: vec![Field::UInt(index)],
                                        lifetime: None,
                                    },
                                },
                            })
                            .collect();
                        Ok(LogResponse {
                            data: bincode::serialize(&replication::LogResponse::Operations(
                                operations,
                            ))
                            .unwrap(),
                        })
                    } else {
                        err_not_found(&request.endpoint)
                    }
                })
            });
            Ok(Response::new(response.boxed()))
        }
    }
}
