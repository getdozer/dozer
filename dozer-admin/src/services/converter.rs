use crate::server::dozer_admin_grpc::{
    self, authentication, ConnectionInfo, PostgresAuthentication,
};
use dozer_orchestrator::orchestration::models::{self, connection::Connection};
use dozer_types::types::{ColumnInfo, TableInfo};
use std::error::Error;

impl TryFrom<ColumnInfo> for dozer_admin_grpc::ColumnInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: ColumnInfo) -> Result<Self, Self::Error> {
        return Ok(dozer_admin_grpc::ColumnInfo {
            column_name: item.column_name,
            is_nullable: item.is_nullable,
            udt_name: item.udt_name,
            is_primary_key: item.is_primary_key,
        });
    }
}
impl TryFrom<TableInfo> for dozer_admin_grpc::TableInfo {
    type Error = Box<dyn Error>;

    fn try_from(item: TableInfo) -> Result<Self, Self::Error> {
        Ok(dozer_admin_grpc::TableInfo {
            table_name: item.table_name,
            columns: item
                .columns
                .iter()
                .map(|x| dozer_admin_grpc::ColumnInfo::try_from(x.clone()).unwrap())
                .collect(),
        })
    }
}

impl TryFrom<ConnectionInfo> for models::connection::Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: ConnectionInfo) -> Result<Self, Self::Error> {
        let db_type_value = match item.r#type {
            0 => models::connection::DBType::Postgres,
            1 => models::connection::DBType::Snowflake,
            _ => models::connection::DBType::Databricks,
        };
        if item.authentication.is_none() {
            return Err("Missing authentication props when converting ".to_owned())?;
        } else {
            let auth_value =
                models::connection::Authentication::try_from(item.authentication.unwrap())?;
            Ok(models::connection::Connection {
                db_type: db_type_value,
                authentication: auth_value,
                name: item.name,
                id: item.id,
            })
        }
    }
}
impl TryFrom<models::connection::Connection> for ConnectionInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: Connection) -> Result<Self, Self::Error> {
        let authentication_value = dozer_admin_grpc::Authentication::try_from(item.to_owned())?;
        Ok(ConnectionInfo {
            id: item.id,
            r#type: 0,
            authentication: Some(authentication_value),
            name: item.name,
        })
    }
}
impl TryFrom<Connection> for dozer_admin_grpc::Authentication {
    type Error = Box<dyn Error>;
    fn try_from(item: Connection) -> Result<Self, Self::Error> {
        let auth = item.authentication;
        let authentication_value = match auth {
            models::connection::Authentication::PostgresAuthentication {
                user,
                password,
                host,
                port,
                database,
            } => authentication::Authentication::Postgres(PostgresAuthentication {
                database,
                user,
                host,
                port: port.to_string(),
                password,
            }),
        };
        return Ok(dozer_admin_grpc::Authentication {
            authentication: Some(authentication_value),
        });
    }
}
impl TryFrom<dozer_admin_grpc::Authentication> for models::connection::Authentication {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_admin_grpc::Authentication) -> Result<Self, Self::Error> {
        if item.authentication.is_none() {
            return Err("Missing authentication props when converting ".to_owned())?;
        } else {
            let authentication = item.authentication.unwrap();
            let result = match authentication {
                authentication::Authentication::Postgres(postgres_authentication) => {
                    let port_int = postgres_authentication.port.parse::<u32>()?;
                    models::connection::Authentication::PostgresAuthentication {
                        user: postgres_authentication.user,
                        password: postgres_authentication.password,
                        host: postgres_authentication.host,
                        port: port_int,
                        database: postgres_authentication.database,
                    }
                }
            };
            return Ok(result);
        }
    }
}
