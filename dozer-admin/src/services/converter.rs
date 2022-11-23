use crate::server::dozer_admin_grpc::{
    self, authentication, ConnectionInfo, PostgresAuthentication,
};
use dozer_types::models;
use dozer_types::types::Schema;
use std::{convert::From, error::Error};

impl From<(String, Schema)> for dozer_admin_grpc::TableInfo {
    fn from(item: (String, Schema)) -> Self {
        let schema = item.1;
        let mut columns: Vec<dozer_admin_grpc::ColumnInfo> = Vec::new();
        schema.fields.iter().enumerate().for_each(|(idx, f)| {
            columns.push(dozer_admin_grpc::ColumnInfo {
                column_name: f.name.to_owned(),
                is_nullable: f.nullable,
                is_primary_key: schema.primary_index.contains(&idx),
                udt_name: serde_json::to_string(&f.typ).unwrap(),
            });
        });
        dozer_admin_grpc::TableInfo {
            table_name: item.0,
            columns,
        }
    }
}

impl TryFrom<ConnectionInfo> for models::connection::Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: ConnectionInfo) -> Result<Self, Self::Error> {
        let db_type_value = match item.r#type {
            0 => models::connection::DBType::Postgres,
            _ => models::connection::DBType::Ethereum,
        };
        if item.authentication.is_none() {
            Err("Missing authentication props when converting ".to_owned())?
        } else {
            let auth_value =
                models::connection::Authentication::try_from(item.authentication.unwrap())?;
            Ok(models::connection::Connection {
                db_type: db_type_value,
                authentication: auth_value,
                name: item.name,
                id: None,
            })
        }
    }
}
impl TryFrom<models::connection::Connection> for dozer_admin_grpc::Authentication {
    type Error = Box<dyn Error>;
    fn try_from(item: models::connection::Connection) -> Result<Self, Self::Error> {
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
                port: port as u32,
                password,
            }),
            models::connection::Authentication::EthereumAuthentication {
                filter: _,
                wss_url: _,
            } => {
                todo!()
            }
            #[cfg(feature = "snowflake")]
            models::connection::Authentication::SnowflakeAuthentication { .. } => {
                todo!()
            }
            models::connection::Authentication::Events {} => todo!(),
        };
        Ok(dozer_admin_grpc::Authentication {
            authentication: Some(authentication_value),
        })
    }
}
impl TryFrom<dozer_admin_grpc::Authentication> for models::connection::Authentication {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_admin_grpc::Authentication) -> Result<Self, Self::Error> {
        if item.authentication.is_none() {
            Err("Missing authentication props when converting ".to_owned())?
        } else {
            let authentication = item.authentication.unwrap();
            let result = match authentication {
                authentication::Authentication::Postgres(postgres_authentication) => {
                    let port_int = postgres_authentication.port as u16;
                    models::connection::Authentication::PostgresAuthentication {
                        user: postgres_authentication.user,
                        password: postgres_authentication.password,
                        host: postgres_authentication.host,
                        port: port_int,
                        database: postgres_authentication.database,
                    }
                }
            };
            Ok(result)
        }
    }
}
#[cfg(test)]
mod test {
    use crate::server::dozer_admin_grpc::ConnectionType;

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
