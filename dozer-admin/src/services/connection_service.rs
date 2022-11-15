use crate::{
    db::{
        persistable::Persistable,
        pool::{establish_connection, DbPool},
    },
    server::dozer_admin_grpc::{
        ConnectionDetails, ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse,
        ErrorResponse, GetAllConnectionRequest, GetAllConnectionResponse,
        GetConnectionDetailsRequest, GetConnectionDetailsResponse, GetSchemaRequest,
        GetSchemaResponse, Pagination, TableInfo, TestConnectionRequest, TestConnectionResponse,
        UpdateConnectionRequest, UpdateConnectionResponse,
    },
};
use dozer_orchestrator::get_connector;
use dozer_types::models::{self, connection::Connection};
use std::thread;

pub struct ConnectionService {
    db_pool: DbPool,
}
impl ConnectionService {
    pub fn new(database_url: String) -> Self {
        Self {
            db_pool: establish_connection(database_url),
        }
    }
}

impl ConnectionService {
    async fn _get_schema(
        &self,
        connection: Connection,
    ) -> Result<Vec<(String, dozer_types::types::Schema)>, ErrorResponse> {
        let get_schema_res = thread::spawn(|| {
            let connector = get_connector(connection).map_err(|err| err.to_string())?;
            connector.get_schemas(None).map_err(|err| err.to_string())
        });
        get_schema_res
            .join()
            .unwrap()
            .map_err(|err| ErrorResponse { message: err })
    }
}
impl ConnectionService {
    pub fn update(
        &self,
        input: UpdateConnectionRequest,
    ) -> Result<UpdateConnectionResponse, ErrorResponse> {
        let mut connection_info = input.info.ok_or(ErrorResponse {
            message: "Missing connection_info".to_owned(),
        })?;
        connection_info
            .upsert(self.db_pool.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(UpdateConnectionResponse {
            info: Some(connection_info),
        })
    }
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        let mut connection_info = input.info.ok_or(ErrorResponse {
            message: "Missing connection_info".to_owned(),
        })?;
        connection_info
            .save(self.db_pool.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(CreateConnectionResponse {
            info: Some(connection_info),
        })
    }

    pub fn get_all_connections(
        &self,
        input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let connection_infos: (Vec<ConnectionInfo>, Pagination) =
            ConnectionInfo::get_multiple(self.db_pool.clone(), input.limit, input.offset).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
        Ok(GetAllConnectionResponse {
            data: connection_infos.0,
            pagination: Some(connection_infos.1),
        })
    }

    pub async fn get_schema(
        &self,
        input: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, ErrorResponse> {
        let connection_info =
            ConnectionInfo::get_by_id(self.db_pool.clone(), input.connection_id.clone()).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
        let connection = Connection::try_from(connection_info).map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let schema = self._get_schema(connection).await?;
        Ok(GetSchemaResponse {
            connection_id: input.connection_id,
            details: Some(ConnectionDetails {
                table_info: schema
                    .iter()
                    .map(|x| TableInfo::try_from(x.clone()).unwrap())
                    .collect(),
            }),
        })
    }

    pub async fn get_connection_details(
        &self,
        input: GetConnectionDetailsRequest,
    ) -> Result<GetConnectionDetailsResponse, ErrorResponse> {
        let connection_by_id = ConnectionInfo::get_by_id(self.db_pool.clone(), input.connection_id)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        let connection = models::connection::Connection::try_from(connection_by_id.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        let schema = self._get_schema(connection.clone()).await?;
        Ok(GetConnectionDetailsResponse {
            info: Some(connection_by_id),
            details: Some(ConnectionDetails {
                table_info: schema
                    .iter()
                    .map(|x| TableInfo::try_from(x.clone()).unwrap())
                    .collect(),
            }),
        })
    }

    pub async fn test_connection(
        &self,
        input: TestConnectionRequest,
    ) -> Result<TestConnectionResponse, ErrorResponse> {
        let connection_info = input.info.ok_or(ErrorResponse {
            message: "Missing connection_info".to_owned(),
        })?;
        let connection = Connection::try_from(connection_info).map_err(|err| ErrorResponse {
            message: err.to_string(),
        })?;

        let connection_test = thread::spawn(|| {
            let connector = get_connector(connection).map_err(|err| err.to_string())?;
            connector.test_connection().map_err(|err| err.to_string())
        });
        connection_test
            .join()
            .unwrap()
            .map(|_op| TestConnectionResponse { success: true })
            .map_err(|err| ErrorResponse { message: err })
    }
}
// #[cfg(test)]
// mod test {
//     use super::ConnectionService;
//     use crate::server::dozer_admin_grpc::{
//         create_connection_request::Authentication, CreateConnectionRequest,
//         GetAllConnectionRequest, PostgresAuthentication,
//     };
//     #[test]
//     fn success_save_connection() {
//         let create_connection_request: CreateConnectionRequest = CreateConnectionRequest {
//             r#type: 0,
//             authentication: Some(Authentication::Postgres(PostgresAuthentication {
//                 database: "pagila".to_owned(),
//                 user: "postgres".to_owned(),
//                 host: "localhost".to_owned(),
//                 port: "5432".to_owned(),
//                 name: "postgres".to_owned(),
//                 password: "postgres".to_owned(),
//             })),
//         };
//         let service = ConnectionService::new("db/test_dozer.db".to_owned());
//         let result = service.create_connection(create_connection_request);
//         assert!(result.is_ok())
//     }
//     #[test]
//     fn success_get_connections() {
//         let create_connection_request: GetAllConnectionRequest = GetAllConnectionRequest {
//             page: 0,
//             page_size: 3,
//         };
//         let service = ConnectionService::new("db/test_dozer.db".to_owned());
//         let result = service.get_all_connections(create_connection_request);
//         assert!(result.is_ok())
//     }
// }
