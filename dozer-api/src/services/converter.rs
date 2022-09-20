use crate::server::dozer_api_grpc::{
    self, connection_info, ConnectionInfo, ConnectionType, CreateConnectionRequest,
    CreateConnectionResponse, PostgresAuthentication, TestConnectionRequest,
};
use core::panic;
use dozer_orchestrator::adapter::db::models::connection::Connection;
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

impl From<Connection> for connection_info::Authentication {
    fn from(item: Connection) -> Self {
        match item.db_type.as_str() {
            "postgres" => {
                let postgres_authentication: PostgresAuthentication =
                    serde_json::from_str::<PostgresAuthentication>(&item.auth).unwrap();
                return connection_info::Authentication::Postgres(postgres_authentication);
            }
            _ => panic!("No db_type match"),
        }
    }
}

impl From<Connection> for CreateConnectionResponse {
    fn from(item: Connection) -> Self {
        CreateConnectionResponse {
            info: Some(ConnectionInfo {
                id: item.id.clone(),
                r#type: 0,
                authentication: Some(item.into()),
            }),
        }
    }
}

impl From<Connection> for ConnectionInfo {
    fn from(item: Connection) -> Self {
        ConnectionInfo {
            id: item.id.clone(),
            r#type: 0,
            authentication: Some(item.into()),
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

                    let new_id = uuid::Uuid::new_v4().to_string();
                    Ok(Connection {
                        id: new_id,
                        auth: json_string.unwrap(),
                        db_type: "postgres".to_string(),
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
                    let json_string = serde_json::to_string(&postgres_auth)
                        .map_err(|err| string_to_static_str(err.to_string()));
                    json_string.map(|op| Connection {
                        id: uuid::Uuid::new_v4().to_string(),
                        auth: op,
                        db_type: "postgres".to_string(),
                    })
                }
            },
            None => Err("Missing Authentication"),
        }
    }
}
