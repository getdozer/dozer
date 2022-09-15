use crate::db::pool::DbPool;
use crate::db::{connection_db_svc, models as DBModels};
use crate::server::dozer_api_grpc::{
    create_connection_request, test_connection_request, ConnectionDetails, ConnectionInfo,
    CreateConnectionRequest, CreateConnectionResponse, ErrorResponse, GetAllConnectionRequest,
    GetAllConnectionResponse, GetConnectionDetailsRequest, GetConnectionDetailsResponse,
    GetSchemaRequest, GetSchemaResponse, Pagination, PostgresAuthentication, TableInfo,
    TestConnectionRequest, TestConnectionResponse,
};
use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres;
use dozer_ingestion::connectors::postgres::connector::PostgresConnector;
use serde_json;

pub struct ConnectionSvc {
    db_connection: DbPool,
}
impl ConnectionSvc {
    pub fn new(db_connection: DbPool) -> Self {
        Self { db_connection }
    }

    fn _initialize_connector(
        &self,
        postgres_connection: PostgresAuthentication,
    ) -> PostgresConnector {
        let conn_str = format!(
            "host={} port={} user={} dbname={} password={}",
            postgres_connection.host,
            postgres_connection.port,
            postgres_connection.user,
            postgres_connection.database,
            postgres_connection.password
        );
        println!("====_initialize_connector {:?}", conn_str);
        let postgres_config = postgres::connector::PostgresConfig {
            name: postgres_connection.name,
            conn_str: conn_str.clone(),
            tables: None,
        };
        let postgres_connection = PostgresConnector::new(postgres_config);
        return postgres_connection;
    }

    async fn _get_connection_details(
        &self,
        connection_id: String,
    ) -> Result<
        (
            Vec<dozer_shared::types::TableInfo>,
            DBModels::connection::Connection,
        ),
        ErrorResponse,
    > {
        let db = self.db_connection.get();
        if db.is_err() {
            return Err(ErrorResponse {
                message: db.err().unwrap().to_string(),
                details: None,
            });
        }
        let connection_by_id =
            connection_db_svc::get_connection_by_id(&db.unwrap(), connection_id.to_owned());
        if connection_by_id.is_ok() {
            let connection = connection_by_id.unwrap();
            let postgres_auth: PostgresAuthentication =
                serde_json::from_str::<PostgresAuthentication>(&connection.auth).unwrap();
            let postgres_connector = self._initialize_connector(postgres_auth);
            let table_info = postgres_connector.get_schema().await;

            return Ok((table_info, connection));
        }
        return Err(ErrorResponse {
            message: connection_by_id.err().unwrap().to_string(),
            details: None,
        });
    }
}
impl ConnectionSvc {
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        let connection_detail = input.authentication;
        match connection_detail {
            Some(authentication) => match authentication {
                create_connection_request::Authentication::Postgres(postgres_connection) => {
                    let new_id = uuid::Uuid::new_v4().to_string();
                    let db = self
                        .db_connection
                        .get()
                        .expect("couldn't get db connection from pool");
                    let inserted = connection_db_svc::create_connection(
                        &db,
                        DBModels::connection::Connection {
                            id: new_id,
                            auth: serde_json::to_string(&postgres_connection).unwrap(),
                            db_type: "postgres".to_owned(),
                        },
                    );

                    match inserted {
                        Ok(data) => Ok(data.into()),
                        Err(e) => Err(ErrorResponse {
                            message: e.to_string(),
                            details: None,
                        }),
                    }
                }
            },
            None => Err(ErrorResponse {
                message: "Missing authentication".to_owned(),
                details: None,
            }),
        }
    }

    pub fn get_all_connections(
        &self,
        input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let db = &self
            .db_connection
            .get()
            .expect("couldn't get db connection from pool");
        let result = connection_db_svc::get_connections(db, input);
        match result {
            Ok(data) => {
                let vec_connection_info: Vec<ConnectionInfo> = data
                    .iter()
                    .map(|x| ConnectionInfo::from(x.clone()))
                    .collect();
                Ok(GetAllConnectionResponse {
                    data: vec_connection_info,
                    pagination: Some(Pagination {
                        limit: 100,
                        page: 1,
                        page_size: 100,
                        total_records: 100,
                        total_pages: 33,
                    }),
                })
            }
            Err(e) => Err(ErrorResponse {
                message: e.to_string(),
                details: None,
            }),
        }
    }

    pub async fn get_schema(
        &self,
        input: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, ErrorResponse> {
        let result = self
            ._get_connection_details(input.connection_id.clone())
            .await;
        match result {
            Ok(info) => {
                let table_info = info.0;
                let connection_details = ConnectionDetails {
                    table_info: table_info
                        .iter()
                        .map(|x| TableInfo::from(x.clone()))
                        .collect(),
                };
                return Ok(GetSchemaResponse {
                    details: Some(connection_details),
                    connection_id: input.connection_id,
                });
            }
            Err(err) => Err(err),
        }
    }

    pub async fn get_connection_details(
        &self,
        input: GetConnectionDetailsRequest,
    ) -> Result<GetConnectionDetailsResponse, ErrorResponse> {
        let result = self
            ._get_connection_details(input.connection_id.clone())
            .await;
        match result {
            Ok(info) => {
                let table_info = info.0;
                let connection = info.1;
                let connection_details = ConnectionDetails {
                    table_info: table_info
                        .iter()
                        .map(|x| TableInfo::from(x.clone()))
                        .collect(),
                };
                return Ok(GetConnectionDetailsResponse {
                    details: Some(connection_details),
                    info: Some(ConnectionInfo::from(connection)),
                });
            }
            Err(err) => Err(err),
        }
    }

    pub async fn test_connection(
        &self,
        input: TestConnectionRequest,
    ) -> Result<TestConnectionResponse, ErrorResponse> {
        let connection_detail = input.authentication;
        match connection_detail {
            Some(authentication) => match authentication {
                test_connection_request::Authentication::Postgres(postgres_connection) => {
                    let postgres_connection = self._initialize_connector(postgres_connection);
                    if let Err(e) = postgres_connection.test_connection().await {
                        Err(ErrorResponse {
                            message: e.to_string().to_owned(),
                            details: None,
                        })
                    } else {
                        Ok(TestConnectionResponse { success: true })
                    }
                }
            },
            None => Err(ErrorResponse {
                message: "Missing authentication property".to_owned(),
                details: None,
            }),
        }
    }
}
