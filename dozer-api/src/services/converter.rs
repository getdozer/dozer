use crate::server::dozer_api_grpc::{
    self, connection_info::Authentication, ConnectionInfo, ConnectionType, CreateConnectionRequest,
    CreateConnectionResponse, PostgresAuthentication, TestConnectionRequest,
};
use core::panic;
use dozer_orchestrator::orchestration::models::connection::{Connection, DBType, self};
use dozer_shared::types::{ColumnInfo, TableInfo};
use std::convert::From;

impl From<ColumnInfo> for dozer_api_grpc::ColumnInfo {
    fn from(item: ColumnInfo) -> Self {
        dozer_api_grpc::ColumnInfo {
            column_name: item.column_name,
            is_nullable: item.is_nullable,
            udt_name: item.udt_name,
            is_primary_key: item.is_primary_key,
        }
    }
}

impl From<TableInfo> for dozer_api_grpc::TableInfo {
    fn from(item: TableInfo) -> Self {
        dozer_api_grpc::TableInfo {
            table_name: item.table_name,
            columns: item
                .columns
                .iter()
                .map(|x| dozer_api_grpc::ColumnInfo::from(x.clone()))
                .collect(),
        }
    }
}

impl From<Connection> for Authentication {
    fn from(item: Connection) -> Self {
        match item.authentication {
            connection::Authentication::PostgresAuthentication {
                user,
                password,
                host,
                port,
                database,
            } => {
                let postgres_authentication: PostgresAuthentication = PostgresAuthentication {
                    database,
                    user,
                    host,
                    port: port.to_string(),
                    name: item.name,
                    password,
                };
                return Authentication::Postgres(postgres_authentication);
            }
        }
    }
}

impl From<Connection> for CreateConnectionResponse {
    fn from(item: Connection) -> Self {
        CreateConnectionResponse {
            info: Some(ConnectionInfo {
                id: item.to_owned().id.unwrap(),
                r#type: 0,
                authentication: Some(Authentication::from(item)),
            }),
        }
    }
}

impl From<Connection> for ConnectionInfo {
    fn from(item: Connection) -> Self {
        ConnectionInfo {
            id: item.to_owned().id.unwrap(),
            r#type: 0,
            authentication: Some(Authentication::from(item)),
        }
    }
}

impl From<i32> for ConnectionType {
    fn from(item: i32) -> Self {
        match item {
            0 => ConnectionType::Postgres,
            _ => panic!("ConnectionType enum not match"),
        }
    }
}
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
impl TryFrom<TestConnectionRequest> for Connection {
    type Error = &'static str;
    fn try_from(item: TestConnectionRequest) -> Result<Self, Self::Error> {
        let authentication = item.authentication;
        match authentication {
            Some(auth) => match auth {
                dozer_api_grpc::test_connection_request::Authentication::Postgres(
                    postgres_auth,
                ) => {
                    let json_string = serde_json::to_string(&postgres_auth)
                        .map_err(|err| string_to_static_str(err.to_string()));
                    if json_string.is_err() {
                        return Err(json_string.err().unwrap());
                    }

                    Ok(Connection {
                        id: None,
                        db_type: DBType::Postgres,
                        authentication: connection::Authentication::PostgresAuthentication {
                            user: postgres_auth.user,
                            password: postgres_auth.password,
                            host: postgres_auth.host,
                            port: postgres_auth.port.parse::<u32>().unwrap(),
                            database: postgres_auth.database,
                        },
                        name: postgres_auth.name,
                    })
                }
            },
            None => Err("Missing Authentication"),
        }
    }
}




impl TryFrom<CreateConnectionRequest> for Connection {
    type Error = &'static str;
    fn try_from(item: CreateConnectionRequest) -> Result<Self, Self::Error> {
        let authentication = item.authentication;
        match authentication {
            Some(auth) => match auth {
                dozer_api_grpc::create_connection_request::Authentication::Postgres(
                    postgres_auth,
                ) => {
                    return Ok(Connection {
                        db_type: DBType::Postgres,
                        authentication: connection::Authentication::PostgresAuthentication {
                            user: postgres_auth.user,
                            password: postgres_auth.password,
                            host: postgres_auth.host,
                            port: postgres_auth.port.parse::<u32>().unwrap(),
                            database: postgres_auth.database,
                        },
                        name: postgres_auth.name,
                        id: None,
                    })
                }
            },
            None => Err("Missing Authentication"),
        }
    }
}
