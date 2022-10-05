use crate::server::dozer_api_grpc::{
    self, connection_info::Authentication, ConnectionInfo, ConnectionType, CreateConnectionRequest,
    CreateConnectionResponse, PostgresAuthentication, TestConnectionRequest,
};
use dozer_orchestrator::models::connection::{self, Connection, DBType};
use dozer_types::types::Schema;
use std::convert::From;

impl From<(String, Schema)> for dozer_api_grpc::TableInfo {
    fn from(item: (String, Schema)) -> Self {
        let schema = item.1;
        let mut columns: Vec<dozer_api_grpc::ColumnInfo> = Vec::new();
        schema.fields.iter().enumerate().for_each(|(idx, f)| {
            columns.push(dozer_api_grpc::ColumnInfo {
                column_name: f.name.to_owned(),
                is_nullable: f.nullable,
                is_primary_key: schema.primary_index.contains(&idx),
                udt_name: serde_json::to_string(&f.typ).unwrap(),
            });
        });
        dozer_api_grpc::TableInfo {
            table_name: item.0,
            columns: columns,
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

impl TryFrom<i32> for ConnectionType {
    type Error = &'static str;
    fn try_from(item: i32) -> Result<Self, Self::Error> {
        match item {
            0 => Ok(ConnectionType::Postgres),
            _ => Err("ConnectionType enum not match"),
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

#[cfg(test)]
mod test {
    use crate::server::dozer_api_grpc::{
        create_connection_request::Authentication, ConnectionType, CreateConnectionRequest,
        PostgresAuthentication,
    };
    use dozer_orchestrator::models::connection::Connection;

    #[test]
    fn success_connection_from_request() {
        let test_connection_request: CreateConnectionRequest = CreateConnectionRequest {
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
        let converted = Connection::try_from(test_connection_request);
        assert!(converted.is_ok())
    }
    #[test]
    fn err_connection_from_request() {
        let test_connection_request: CreateConnectionRequest = CreateConnectionRequest {
            r#type: 0,
            authentication: None,
        };
        let converted = Connection::try_from(test_connection_request);
        assert!(converted.is_err())
    }
    #[test]
    fn success_from_i32_to_connection_type() {
        let converted = ConnectionType::try_from(0);
        assert!(converted.is_ok());
        assert_eq!(converted.unwrap(), ConnectionType::Postgres);
    }
    #[test]
    fn err_from_i32_to_connection_type() {
        let converted = ConnectionType::try_from(100).map_err(|err| err.to_string());
        assert!(converted.is_err());
        assert_eq!(converted.err().unwrap(), "ConnectionType enum not match");
    }
}
