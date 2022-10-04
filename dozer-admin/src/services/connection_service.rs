use std::thread;

use dozer_orchestrator::simple::Simple as Dozer;
use dozer_orchestrator::{models::connection::Connection, Orchestrator};

use crate::{
    db::{
        persistable::Persistable,
        pool::{establish_connection, DbPool},
    },
    server::dozer_api_grpc::{
        ConnectionDetails, ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse,
        ErrorResponse, GetAllConnectionRequest, GetAllConnectionResponse,
        GetConnectionDetailsRequest, GetConnectionDetailsResponse, GetSchemaRequest,
        GetSchemaResponse, Pagination, TableInfo, TestConnectionRequest, TestConnectionResponse,
    },
};

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
    ) -> Result<Vec<dozer_types::types::TableInfo>, ErrorResponse> {
        let get_schema_res = thread::spawn(|| {
            let result = Dozer::get_schema(connection).map_err(|err| err.to_string());
            return result;
        });
        get_schema_res.join().unwrap().map_err(|err| ErrorResponse {
            message: err,
            details: None,
        })
    }
}
impl ConnectionService {
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        let mut connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        connection
            .save(self.db_pool.clone())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        Ok(CreateConnectionResponse::from(connection.clone()))
    }

    pub fn get_all_connections(
        &self,
        _input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let result =
            Connection::get_multiple(self.db_pool.clone()).map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        let vec_connection_info: Vec<ConnectionInfo> = result
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

    pub async fn get_schema(
        &self,
        input: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, ErrorResponse> {
        let connection = Connection::get_by_id(self.db_pool.clone(), input.connection_id.clone())
            .map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        let schema = self._get_schema(connection).await?;
        Ok(GetSchemaResponse {
            connection_id: input.connection_id,
            details: Some(ConnectionDetails {
                table_info: schema.iter().map(|x| TableInfo::from(x.clone())).collect(),
            }),
        })
    }

    pub async fn get_connection_details(
        &self,
        input: GetConnectionDetailsRequest,
    ) -> Result<GetConnectionDetailsResponse, ErrorResponse> {
        let connection =
            Connection::get_by_id(self.db_pool.clone(), input.connection_id).map_err(|op| {
                ErrorResponse {
                    message: op.to_string(),
                    details: None,
                }
            })?;
        let schema = self._get_schema(connection.clone()).await?;
        Ok(GetConnectionDetailsResponse {
            info: Some(ConnectionInfo::from(connection)),
            details: Some(ConnectionDetails {
                table_info: schema.iter().map(|x| TableInfo::from(x.clone())).collect(),
            }),
        })
    }

    pub async fn test_connection(
        &self,
        input: TestConnectionRequest,
    ) -> Result<TestConnectionResponse, ErrorResponse> {
        let connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        let connection_test = thread::spawn(|| {
            let result = Dozer::test_connection(connection).map_err(|err| err.to_string());
            return result;
        });
        connection_test
            .join()
            .unwrap()
            .map(|_op| TestConnectionResponse { success: true })
            .map_err(|err| ErrorResponse {
                message: err,
                details: None,
            })
    }
}
#[cfg(test)]
mod test {
    use super::ConnectionService;
    use crate::server::dozer_api_grpc::{
        create_connection_request::Authentication, CreateConnectionRequest,
        GetAllConnectionRequest, PostgresAuthentication,
    };
    use mockall::mock;
    #[test]
    fn success_save_connection() {
        let create_connection_request: CreateConnectionRequest = CreateConnectionRequest {
            r#type: 0,
            authentication: Some(Authentication::Postgres(PostgresAuthentication {
                database: "pagila".to_owned(),
                user: "postgres".to_owned(),
                host: "localhost".to_owned(),
                port: "5432".to_owned(),
                name: "postgres".to_owned(),
                password: "postgres".to_owned(),
            })),
        };
        let service = ConnectionService::new("db/test_dozer.db".to_owned());
        let result = service.create_connection(create_connection_request);
        assert!(result.is_ok())
    }
    #[test]
    fn success_get_connections() {
        let create_connection_request: GetAllConnectionRequest = GetAllConnectionRequest {
            page: 0,
            page_size: 3,
        };
        let service = ConnectionService::new("db/test_dozer.db".to_owned());
        let result = service.get_all_connections(create_connection_request);
        assert!(result.is_ok())
    }
}
