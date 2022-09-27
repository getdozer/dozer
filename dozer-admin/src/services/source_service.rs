use crate::{
    db::{
        persistable::Persistable,
        pool::{establish_connection, DbPool},
    },
    server::dozer_api_grpc::{
        self,
        create_source_request::{self},
        CreateSourceRequest, CreateSourceResponse, ErrorResponse, GetSourceRequest,
        GetSourceResponse,
    },
};
use dozer_orchestrator::orchestration::models::{
    connection::Connection,
    source::{HistoryType, RefreshConfig, Source},
};

pub struct SourceService {
    db_pool: DbPool,
}
impl SourceService {
    pub fn new(database_url: String) -> Self {
        Self {
            db_pool: establish_connection(database_url.clone()),
        }
    }
}
impl SourceService {
    pub fn create_source(
        &self,
        input: CreateSourceRequest,
    ) -> Result<CreateSourceResponse, ErrorResponse> {
        let connection = input.connection.ok_or_else(|| ErrorResponse {
            message: "Missing connection info".to_owned(),
            details: None,
        })?;

        let valid_connection: Connection;
        valid_connection = match connection {
            create_source_request::Connection::ConnectionId(connection_id) => {
                let exist_connection =
                    Connection::get_by_id(self.db_pool.clone(), connection_id.clone()).map_err(
                        |op| ErrorResponse {
                            message: op.to_string(),
                            details: None,
                        },
                    )?;
                exist_connection
            }
            create_source_request::Connection::ConnectionInfo(connection) => {
                let connection = Connection::try_from(connection).map_err(|op| ErrorResponse {
                    message: op.to_string(),
                    details: None,
                })?;
                connection
            }
        };
        let history_type_value: dozer_api_grpc::HistoryType =
            input.history_type.ok_or_else(|| ErrorResponse {
                message: "Missing history type".to_owned(),
                details: None,
            })?;
        let history_type_value =
            HistoryType::try_from(history_type_value).map_err(|err| ErrorResponse {
                message: err.to_string(),
                details: None,
            })?;
        let refresh_config_value = input.refresh_config.ok_or_else(|| ErrorResponse {
            message: "Missing refresh config".to_owned(),
            details: None,
        })?;
        let refresh_config_value =
            RefreshConfig::try_from(refresh_config_value).map_err(|err| ErrorResponse {
                message: err.to_string(),
                details: None,
            })?;
        let mut source = Source {
            id: None,
            name: input.name,
            dest_table_name: input.dest_table_name,
            source_table_name: input.source_table_name,
            connection: valid_connection,
            history_type: history_type_value,
            refresh_config: refresh_config_value,
        };
        source
            .save(self.db_pool.clone())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        let source_info =
            dozer_api_grpc::SourceInfo::try_from(source.clone()).map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        return Ok(CreateSourceResponse {
            info: Some(source_info),
            id: source.id.unwrap(),
        });
    }
    pub fn get_source(&self, input: GetSourceRequest) -> Result<GetSourceResponse, ErrorResponse> {
        let result = Source::get_by_id(self.db_pool.clone(), input.id).map_err(|op| ErrorResponse {
            message: op.to_string(),
            details: None,
        })?;
        let source_info =
            dozer_api_grpc::SourceInfo::try_from(result.clone()).map_err(|op| ErrorResponse {
                message: op.to_string(),
                details: None,
            })?;
        return Ok(GetSourceResponse {
            info: Some(source_info),
            id: result.id.unwrap(),
        });
    }
}
