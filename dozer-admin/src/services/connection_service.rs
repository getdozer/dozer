use crate::{
    db::{persistable::Persistable, pool::DbPool},
    server::dozer_admin_grpc::{
        ConnectionDetails, CreateConnectionRequest, CreateConnectionResponse, ErrorResponse,
        GetAllConnectionRequest, GetAllConnectionResponse, GetConnectionDetailsRequest,
        GetConnectionDetailsResponse, GetSchemaRequest, GetSchemaResponse, Pagination, TableInfo,
        UpdateConnectionRequest, UpdateConnectionResponse, ValidateConnectionRequest,
        ValidateConnectionResponse,
    },
};
use dozer_orchestrator::get_connector;
use dozer_types::models::{
    self,
    connection::{Authentication, Connection},
};
use std::thread;

pub struct ConnectionService {
    db_pool: DbPool,
}
impl ConnectionService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
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
    pub fn create_connection(
        &self,
        input: CreateConnectionRequest,
    ) -> Result<CreateConnectionResponse, ErrorResponse> {
        if let Some(authentication) = input.authentication {
            let authentication = Authentication::from(authentication);
            let generated_id = uuid::Uuid::new_v4().to_string();
            let mut connection = dozer_types::models::connection::Connection {
                id: Some(generated_id),
                app_id: Some(input.app_id),
                db_type: input.r#type,
                name: input.name,
                authentication: Some(authentication),
            };
            connection
                .save(self.db_pool.clone())
                .map_err(|err| ErrorResponse {
                    message: err.to_string(),
                })?;
            return Ok(CreateConnectionResponse {
                data: Some(connection),
            });
        }
        Err(ErrorResponse {
            message: "Missing authentication input".to_owned(),
        })
    }

    pub async fn get_connection_details(
        &self,
        input: GetConnectionDetailsRequest,
    ) -> Result<GetConnectionDetailsResponse, ErrorResponse> {
        let connection_by_id =
            Connection::by_id(self.db_pool.clone(), input.connection_id, input.app_id).map_err(
                |op| ErrorResponse {
                    message: op.to_string(),
                },
            )?;
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

    pub async fn get_schema(
        &self,
        input: GetSchemaRequest,
    ) -> Result<GetSchemaResponse, ErrorResponse> {
        let connection_info = Connection::by_id(
            self.db_pool.clone(),
            input.connection_id.clone(),
            input.app_id,
        )
        .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
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

    pub fn list(
        &self,
        input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let connection_infos: (Vec<Connection>, Pagination) = Connection::list(
            self.db_pool.clone(),
            input.app_id,
            input.limit,
            input.offset,
        )
        .map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        Ok(GetAllConnectionResponse {
            data: connection_infos.0,
            pagination: Some(connection_infos.1),
        })
    }

    pub fn update(
        &self,
        input: UpdateConnectionRequest,
    ) -> Result<UpdateConnectionResponse, ErrorResponse> {
        let authentication = Authentication::from(input.authentication.unwrap_or_default());
        let mut connection_by_id =
            Connection::by_id(self.db_pool.clone(), input.connection_id, input.app_id).map_err(
                |err| ErrorResponse {
                    message: err.to_string(),
                },
            )?;
        connection_by_id.authentication = Some(authentication);
        connection_by_id.name = input.name;
        connection_by_id.db_type = input.r#type;
        connection_by_id
            .upsert(self.db_pool.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;
        Ok(UpdateConnectionResponse {
            info: Some(connection_by_id),
        })
    }

    pub async fn validate_connection(
        &self,
        input: ValidateConnectionRequest,
    ) -> Result<ValidateConnectionResponse, ErrorResponse> {
        let authentication = Authentication::from(input.authentication.unwrap_or_default());
        let connection = Connection {
            db_type: input.r#type,
            authentication: Some(authentication),
            name: input.name,
            id: None,
            ..Default::default()
        };
        let validate_result = thread::spawn(|| {
            let connector = get_connector(connection).map_err(|err| err.to_string())?;
            connector.validate().map_err(|err| err.to_string())
        });
        validate_result
            .join()
            .unwrap()
            .map(|_op| ValidateConnectionResponse { success: true })
            .map_err(|err| ErrorResponse { message: err })
    }
}
