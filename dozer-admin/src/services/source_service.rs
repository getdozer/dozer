use dozer_types::models::{connection::Authentication, source::Source};

use crate::{
    db::{persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        create_source_request, update_source_request, CreateSourceRequest, CreateSourceResponse,
        ErrorResponse, GetAllSourceRequest, GetAllSourceResponse, GetSourceRequest,
        GetSourceResponse, Pagination, UpdateSourceRequest, UpdateSourceResponse,
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
    pub fn create_source(
        &self,
        input: CreateSourceRequest,
    ) -> Result<CreateSourceResponse, ErrorResponse> {
        if let Some(connection_info) = input.connection {
            let input_connection = match connection_info {
                create_source_request::Connection::ConnectionId(connection_id) => {
                    dozer_types::models::connection::Connection::by_id(
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
                    dozer_types::models::connection::Connection {
                        id: Some(generated_id),
                        app_id: Some(connection_request.app_id),
                        name: connection_request.name,
                        authentication: Some(Authentication::from(
                            connection_request.authentication.unwrap_or_default(),
                        )),
                        db_type: connection_request.r#type,
                    }
                }
            };
            let generated_id = uuid::Uuid::new_v4().to_string();
            let mut source_info = dozer_types::models::source::Source {
                id: Some(generated_id),
                app_id: Some(input.app_id),
                name: input.name,
                table_name: input.table_name,
                connection: Some(input_connection),
                columns: input.columns,
                refresh_config: Some(dozer_types::models::source::RefreshConfig::default()),
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
        let source_info = dozer_types::models::source::Source::by_id(
            self.db_pool.to_owned(),
            input.id,
            input.app_id,
        )
        .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        Ok(GetSourceResponse {
            info: Some(source_info),
        })
    }

    pub fn list(&self, input: GetAllSourceRequest) -> Result<GetAllSourceResponse, ErrorResponse> {
        let sources: (Vec<Source>, Pagination) = Source::list(
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

    pub fn update_source(
        &self,
        input: UpdateSourceRequest,
    ) -> Result<UpdateSourceResponse, ErrorResponse> {
        let mut source_by_id =
            Source::by_id(self.db_pool.to_owned(), input.id, input.app_id.to_owned()).map_err(
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
            let input_connection = match connection {
                update_source_request::Connection::ConnectionId(connection_id) => {
                    dozer_types::models::connection::Connection::by_id(
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
                    dozer_types::models::connection::Connection {
                        id: Some(generated_id),
                        app_id: Some(connection_request.app_id),
                        name: connection_request.name,
                        authentication: Some(Authentication::from(
                            connection_request.authentication.unwrap_or_default(),
                        )),
                        db_type: connection_request.r#type,
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
