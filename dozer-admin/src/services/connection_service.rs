use crate::{
    db::{
        connection::{DbConnection, NewConnection},
        pool::DbPool,
        schema::connections::dsl::*,
    },
    server::dozer_admin_grpc::{
        ConnectionResponse, CreateConnectionRequest, ErrorResponse, GetAllConnectionRequest,
        GetAllConnectionResponse, GetTablesRequest, GetTablesResponse, Pagination, TableInfo,
        UpdateConnectionRequest, ValidateConnectionRequest, ValidateConnectionResponse,
    },
};
use dozer_orchestrator::get_connector;
use dozer_types::types::SchemaWithChangesType;
use dozer_types::{
    log::error,
    models::connection::{Authentication, Connection},
};
use std::thread;

use diesel::{insert_into, QueryDsl, RunQueryDsl};

use super::constants;

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
    ) -> Result<Vec<SchemaWithChangesType>, ErrorResponse> {
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
    ) -> Result<ConnectionResponse, ErrorResponse> {
        if let Some(authentication) = input.authentication {
            let authentication = Authentication::from(authentication);
            let generated_id = uuid::Uuid::new_v4().to_string();
            let c = dozer_types::models::connection::Connection {
                db_type: input.r#type,
                name: input.name,
                authentication: Some(authentication),
            };

            let new_connection =
                NewConnection::from(c.clone(), generated_id.clone()).map_err(|err| {
                    ErrorResponse {
                        message: err.to_string(),
                    }
                })?;
            let mut db = self.db_pool.clone().get().map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

            let _ = insert_into(connections)
                .values(&new_connection)
                .execute(&mut db)
                .map_err(|err| ErrorResponse {
                    message: err.to_string(),
                })?;

            return Ok(ConnectionResponse {
                id: generated_id,
                connection: Some(c),
            });
        }
        Err(ErrorResponse {
            message: "Missing authentication input".to_owned(),
        })
    }

    pub async fn get_tables(
        &self,
        input: GetTablesRequest,
    ) -> Result<GetTablesResponse, ErrorResponse> {
        let mut db = self.db_pool.clone().get().map_err(|err| ErrorResponse {
            message: err.to_string(),
        })?;
        let result: DbConnection = connections
            .find(input.connection_id.clone())
            .first(&mut db)
            .map_err(|err| {
                error!("Error fetching schemas: {}", err);
                ErrorResponse {
                    message: err.to_string(),
                }
            })?;
        let connection = Connection::try_from(result).map_err(|err| ErrorResponse {
            message: err.to_string(),
        })?;

        let schema = self._get_schema(connection).await?;
        Ok(GetTablesResponse {
            connection_id: input.connection_id,
            tables: schema
                .iter()
                .map(|(n, schema, _)| TableInfo::try_from((n.clone(), schema.clone())).unwrap())
                .collect(),
        })
    }

    pub fn list(
        &self,
        input: GetAllConnectionRequest,
    ) -> Result<GetAllConnectionResponse, ErrorResponse> {
        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let offset = input.offset.unwrap_or(constants::OFFSET);
        let limit = input.limit.unwrap_or(constants::LIMIT);
        let results: Vec<DbConnection> = connections
            .offset(offset.into())
            .limit(limit.into())
            .load(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        let total: i64 = connections
            .count()
            .get_result(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;
        let conns: Vec<ConnectionResponse> = results
            .iter()
            .map(|result| {
                let c = Connection::try_from(result.clone()).unwrap();

                ConnectionResponse {
                    id: result.id.clone(),
                    connection: Some(c),
                }
            })
            .collect();

        Ok(GetAllConnectionResponse {
            connections: conns,
            pagination: Some(Pagination {
                limit,
                total: total.try_into().unwrap(),
                offset,
            }),
        })
    }

    pub fn update(
        &self,
        request: UpdateConnectionRequest,
    ) -> Result<ConnectionResponse, ErrorResponse> {
        let mut db = self.db_pool.clone().get().map_err(|op| ErrorResponse {
            message: op.to_string(),
        })?;
        let authentication = Authentication::from(request.authentication.unwrap_or_default());

        let c = dozer_types::models::connection::Connection {
            db_type: request.r#type,
            name: request.name,
            authentication: Some(authentication),
        };

        let new_connection = NewConnection::from(c.clone(), request.connection_id.clone())
            .map_err(|err| ErrorResponse {
                message: err.to_string(),
            })?;

        let _ = diesel::update(connections)
            .set(new_connection)
            .execute(&mut db)
            .map_err(|op| ErrorResponse {
                message: op.to_string(),
            })?;

        Ok(ConnectionResponse {
            id: request.connection_id,
            connection: Some(c),
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
            ..Default::default()
        };
        let validate_result = thread::spawn(|| {
            let connector = get_connector(connection).map_err(|err| err.to_string())?;
            connector.validate(None).map_err(|err| err.to_string())
        });
        validate_result
            .join()
            .unwrap()
            .map(|_op| ValidateConnectionResponse { success: true })
            .map_err(|err| ErrorResponse { message: err })
    }
}
