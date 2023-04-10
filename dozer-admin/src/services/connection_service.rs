use crate::db::{
    connection::{DbConnection, NewConnection},
    pool::DbPool,
    schema::connections::dsl::*,
};
use dozer_orchestrator::{get_connector, ConnectorError};
use dozer_types::grpc_types::admin::{
    ConnectionRequest, ConnectionResponse, ErrorResponse, GetAllConnectionRequest,
    GetAllConnectionResponse, GetTablesRequest, GetTablesResponse, Pagination,
    UpdateConnectionRequest, ValidateConnectionResponse,
};
use dozer_types::{log::error, models::connection::Connection};

use diesel::{insert_into, QueryDsl, RunQueryDsl};

use super::{constants, converter::convert_table};

pub struct ConnectionService {
    db_pool: DbPool,
}
impl ConnectionService {
    pub fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}

async fn get_tables(
    connection: Connection,
) -> Result<Vec<dozer_orchestrator::TableInfo>, ConnectorError> {
    let connector = get_connector(connection)?;
    connector.list_columns(connector.list_tables().await?).await
}

impl ConnectionService {
    async fn _get_tables(
        &self,
        connection: Connection,
    ) -> Result<Vec<dozer_orchestrator::TableInfo>, ErrorResponse> {
        let res = get_tables(connection).await.map_err(|err| err.to_string());

        res.map_err(|err| ErrorResponse { message: err })
    }
}
impl ConnectionService {
    pub fn create_connection(
        &self,
        input: ConnectionRequest,
    ) -> Result<ConnectionResponse, ErrorResponse> {
        if let Some(c) = input.connection {
            let generated_id = uuid::Uuid::new_v4().to_string();

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

        let tables = self._get_tables(connection).await?;
        Ok(GetTablesResponse {
            connection_id: input.connection_id,
            tables: tables.iter().map(|t| convert_table(t.clone())).collect(),
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
        let c = request.connection.unwrap();

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
        input: ConnectionRequest,
    ) -> Result<ValidateConnectionResponse, ErrorResponse> {
        let validate_result = validate_connection(input.connection.unwrap()).await;
        validate_result
            .map(|_op| ValidateConnectionResponse { success: true })
            .map_err(|err| ErrorResponse { message: err })
    }
}

async fn validate_connection(connection: Connection) -> Result<(), String> {
    let connector = get_connector(connection).map_err(|err| err.to_string())?;
    connector
        .validate_connection()
        .await
        .map_err(|err| err.to_string())
}
