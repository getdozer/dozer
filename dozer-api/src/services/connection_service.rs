use dozer_orchestrator::orchestration::{builder::Dozer, models::connection::Connection};

use crate::{
    persistent::{
        pool::{establish_connection, DbPool}, persistable::Persistable,
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
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        let mut connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        connection.save(self.db_pool.clone()).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        Ok(CreateConnectionResponse::from(connection.clone()))
    }

    pub fn get_all_connections(
        &self,
        _input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let result = Connection::get_multiple(self.db_pool.clone())
            .map_err(|op| ErrorResponse {
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
        let schema = Dozer::get_schema(connection).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
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
        let connection = Connection::get_by_id(self.db_pool.clone(), input.connection_id)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        let schema = Dozer::get_schema(connection.clone()).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        Ok(GetConnectionDetailsResponse {
            info: Some(ConnectionInfo::from(connection)),
            details: Some(ConnectionDetails {
                table_info: schema.iter().map(|x| TableInfo::from(x.clone())).collect(),
            }),
        })
    }

    pub fn test_connection(
        &self,
        input: TestConnectionRequest,
    ) -> Result<TestConnectionResponse, ErrorResponse> {
        let connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        Dozer::test_connection(connection).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        Ok(TestConnectionResponse { success: true })
    }
}
