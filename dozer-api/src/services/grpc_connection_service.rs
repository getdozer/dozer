use crate::server::dozer_api_grpc::{
    ConnectionDetails, ConnectionInfo, CreateConnectionRequest, CreateConnectionResponse,
    ErrorResponse, GetAllConnectionRequest, GetAllConnectionResponse, GetConnectionDetailsRequest,
    GetConnectionDetailsResponse, GetSchemaRequest, GetSchemaResponse, Pagination, TableInfo,
    TestConnectionRequest, TestConnectionResponse,
};
use dozer_orchestrator::connection::db::models::connection::Connection;
use dozer_orchestrator::connection::service::ConnectionSvc;
use dozer_orchestrator::connection::traits::connections_svc::ConnectionSvcTrait;
pub struct GRPCConnectionService {
    connection_svc: ConnectionSvc,
}
impl GRPCConnectionService {
    pub fn new(database_url: String) -> Self {
        Self {
            connection_svc: ConnectionSvc::new(database_url),
        }
    }
}
impl GRPCConnectionService {
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        let connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        self.connection_svc
            .create_connection(connection.clone())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        Ok(CreateConnectionResponse::from(connection))
    }

    pub fn get_all_connections(
        &self,
        _input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let result = self
            .connection_svc
            .get_all_connections()
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
        let result = self
            .connection_svc
            .get_schema(input.connection_id.clone())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        Ok(GetSchemaResponse {
            connection_id: input.connection_id,
            details: Some(ConnectionDetails {
                table_info: result.iter().map(|x| TableInfo::from(x.clone())).collect(),
            }),
        })
    }

    pub async fn get_connection_details(
        &self,
        input: GetConnectionDetailsRequest,
    ) -> Result<GetConnectionDetailsResponse, ErrorResponse> {
        todo!()
        // let result = self
        //     ._get_connection_details(input.connection_id.clone())
        //     .await;
        // match result {
        //     Ok(info) => {
        //         let table_info = info.0;
        //         let connection = info.1;
        //         let connection_details = ConnectionDetails {
        //             table_info: table_info
        //                 .iter()
        //                 .map(|x| TableInfo::from(x.clone()))
        //                 .collect(),
        //         };
        //         return Ok(GetConnectionDetailsResponse {
        //             details: Some(connection_details),
        //             info: Some(ConnectionInfo::from(connection)),
        //         });
        //     }
        //     Err(err) => Err(err),
        // }
    }

    pub async fn test_connection(
        &self,
        input: TestConnectionRequest,
    ) -> Result<TestConnectionResponse, ErrorResponse> {
        let connection = Connection::try_from(input).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        self.connection_svc
            .test_connection(connection)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        Ok(TestConnectionResponse { success: true })
    }
}
