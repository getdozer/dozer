use crate::{
    db::{persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        create_source_request, update_source_request, ConnectionInfo, CreateSourceRequest,
        CreateSourceResponse, ErrorResponse, GetAllSourceRequest, GetAllSourceResponse,
        GetSourceRequest, GetSourceResponse, Pagination, SourceInfo, UpdateSourceRequest,
        UpdateSourceResponse,
    },
};
pub struct SourceService {
    db_pool: DbPool,
}
impl SourceService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}
impl SourceService {
    pub fn list(&self, input: GetAllSourceRequest) -> Result<GetAllSourceResponse, ErrorResponse> {
        let sources: (Vec<SourceInfo>, Pagination) = SourceInfo::list(
            self.db_pool.clone(),
            input.app_id,
            input.limit,
            input.offset,
        )
        .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        Ok(GetAllSourceResponse {
            data: sources.0,
            pagination: Some(sources.1),
        })
    }
    pub fn create_source(
        &self,
        input: CreateSourceRequest,
    ) -> Result<CreateSourceResponse, ErrorResponse> {
        if let Some(connection_info) = input.connection {
            let input_connection: ConnectionInfo = match connection_info {
                create_source_request::Connection::ConnectionId(connection_id) => {
                    ConnectionInfo::by_id(
                        self.db_pool.to_owned(),
                        connection_id,
                        input.app_id.to_owned(),
                    )
                    .map_err(|err| ErrorResponse {
                        message: err.to_string(),
                    })?
                }
                create_source_request::Connection::ConnectionInfo(connection_request) => {
                    let generated_id = uuid::Uuid::new_v4().to_string();
                    ConnectionInfo {
                        id: generated_id,
                        app_id: connection_request.app_id,
                        name: connection_request.name,
                        r#type: connection_request.r#type,
                        authentication: connection_request.authentication,
                    }
                }
            };
            let generated_id = uuid::Uuid::new_v4().to_string();
            let mut source_info = SourceInfo {
                id: generated_id,
                app_id: input.app_id,
                name: input.name,
                table_name: input.table_name,
                connection: Some(input_connection),
            };
            source_info
                .upsert(self.db_pool.to_owned())
                .map_err(|op| ErrorResponse {
                    message: op.to_string(),
                })?;
            return Ok(CreateSourceResponse {
                info: Some(source_info),
            });
        }
        Err(ErrorResponse {
            message: "Missing input connection info".to_owned(),
        })
    }

    pub fn get_source(&self, input: GetSourceRequest) -> Result<GetSourceResponse, ErrorResponse> {
        let source_info = SourceInfo::by_id(self.db_pool.to_owned(), input.id, input.app_id)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(GetSourceResponse {
            info: Some(source_info),
        })
    }

    pub fn update_source(
        &self,
        input: UpdateSourceRequest,
    ) -> Result<UpdateSourceResponse, ErrorResponse> {
        let mut source_by_id =
            SourceInfo::by_id(self.db_pool.to_owned(), input.id, input.app_id.to_owned()).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
        if let Some(update_name) = input.name {
            source_by_id.name = update_name;
        }
        if let Some(update_table_name) = input.table_name {
            source_by_id.table_name = update_table_name;
        }
        if let Some(connection) = input.connection {
            let input_connection: ConnectionInfo = match connection {
                update_source_request::Connection::ConnectionId(connection_id) => {
                    ConnectionInfo::by_id(
                        self.db_pool.to_owned(),
                        connection_id,
                        input.app_id.to_owned(),
                    )
                    .map_err(|err| ErrorResponse {
                        message: err.to_string(),
                    })?
                }
                update_source_request::Connection::ConnectionInfo(connection_request) => {
                    let generated_id = uuid::Uuid::new_v4().to_string();
                    ConnectionInfo {
                        id: generated_id,
                        app_id: connection_request.app_id,
                        name: connection_request.name,
                        r#type: connection_request.r#type,
                        authentication: connection_request.authentication,
                    }
                }
            };
            source_by_id.connection = Some(input_connection);
        }
        source_by_id
            .upsert(self.db_pool.to_owned())
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        Ok(UpdateSourceResponse {
            info: Some(source_by_id),
        })
    }
}
