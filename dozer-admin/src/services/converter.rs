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
                name: item.name,
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

// impl From<Connection> for Authentication {
//     fn from(item: Connection) -> Self {
//         match item.authentication {
//             connection::Authentication::PostgresAuthentication {
//                 user,
//                 password,
//                 host,
//                 port,
//                 database,
//             } => {
//                 let postgres_authentication: PostgresAuthentication = PostgresAuthentication {
//                     database,
//                     user,
//                     host,
//                     port: port.to_string(),
//                     name: item.name,
//                     password,
//                 };
//                 return Authentication::Postgres(postgres_authentication);
//             }
//         }
//     }
// }

// impl From<Connection> for CreateConnectionResponse {
//     fn from(item: Connection) -> Self {
//         CreateConnectionResponse {
//             info: Some(ConnectionInfo {
//                 id: item.to_owned().id.unwrap(),
//                 r#type: 0,
//                 authentication: Some(Authentication::from(item)),
//             }),
//         }
//     }
// }

// impl TryFrom<i32> for ConnectionType {
//     type Error = &'static str;
//     fn try_from(item: i32) -> Result<Self, Self::Error> {
//         match item {
//             0 => Ok(ConnectionType::Postgres),
//             _ => Err("ConnectionType enum not match"),
//         }
//     }
// }
// impl TryFrom<transactional_history_type::Config> for TransactionalHistoryConfig {
//     type Error = Box<dyn Error>;
//     fn try_from(
//         item: dozer_admin_grpc::transactional_history_type::Config,
//     ) -> Result<Self, Self::Error> {
//         match item {
//             dozer_admin_grpc::transactional_history_type::Config::AppendOnly(retain_partial) => {
//                 return Ok(TransactionalHistoryConfig::RetainPartial {
//                     timestamp_field: retain_partial.timestamp_field,
//                     retention_period: retain_partial.retention_period,
//                 })
//             }
//         }
//     }
// }
// impl TryFrom<master_history_type::Config> for MasterHistoryConfig {
//     type Error = Box<dyn Error>;
//     fn try_from(item: master_history_type::Config) -> Result<Self, Self::Error> {
//         match item {
//             master_history_type::Config::AppendOnly(append_only_config) => {
//                 return Ok(MasterHistoryConfig::AppendOnly {
//                     unique_key_field: append_only_config.unique_key_field,
//                     open_date_field: append_only_config.open_date_field,
//                     closed_date_field: append_only_config.closed_date_field,
//                 })
//             }
//             master_history_type::Config::Overwrite(_) => return Ok(MasterHistoryConfig::Overwrite),
//         }
//     }
// }
// impl TryFrom<dozer_admin_grpc::RefreshConfig> for RefreshConfig {
//     type Error = Box<dyn Error>;
//     fn try_from(item: dozer_admin_grpc::RefreshConfig) -> Result<Self, Self::Error> {
//         if item.config.is_none() {
//             return Err("RefreshConfig is empty".to_owned())?;
//         }
//         let config = item.config.unwrap();
//         match config {
//             dozer_admin_grpc::refresh_config::Config::Hour(config_hour) => {
//                 Ok(RefreshConfig::Hour {
//                     minute: config_hour.minute,
//                 })
//             }
//             dozer_admin_grpc::refresh_config::Config::Day(config_day) => Ok(RefreshConfig::Day {
//                 time: config_day.time,
//             }),
//             dozer_admin_grpc::refresh_config::Config::CronExpression(config_cron) => {
//                 Ok(RefreshConfig::CronExpression {
//                     expression: config_cron.expression,
//                 })
//             }
//             dozer_admin_grpc::refresh_config::Config::Realtime(_) => Ok(RefreshConfig::RealTime),
//         }
//     }
// }
// impl TryFrom<dozer_admin_grpc::HistoryType> for HistoryType {
//     type Error = Box<dyn Error>;
//     fn try_from(item: dozer_admin_grpc::HistoryType) -> Result<Self, Self::Error> {
//         if item.r#type.is_none() {
//             return Err("HistoryType is empty".to_owned())?;
//         }
//         let config = item.r#type.unwrap();
//         match config {
//             history_type::Type::Master(master_history) => {
//                 let master_history_config = master_history.config;
//                 if master_history_config.is_none() {
//                     return Err("Missing master_history_config".to_owned())?;
//                 }
//                 let master_history_config = master_history_config.unwrap();
//                 let master_history_config = MasterHistoryConfig::try_from(master_history_config)?;
//                 return Ok(HistoryType::Master(master_history_config));
//             }
//             history_type::Type::Transactional(transactional_history) => {
//                 let transactional_history_config = transactional_history.config;
//                 if transactional_history_config.is_none() {
//                     return Err("Missing transactional_history_config".to_owned())?;
//                 }
//                 let transactional_history_config = transactional_history_config.unwrap();
//                 let transactional_history_config =
//                     TransactionalHistoryConfig::try_from(transactional_history_config)?;
//                 return Ok(HistoryType::Transactional(transactional_history_config));
//             }
//         }
//     }
// }
// impl TryFrom<TestConnectionRequest> for Connection {
//     type Error = Box<dyn Error>;
//     fn try_from(item: TestConnectionRequest) -> Result<Self, Self::Error> {
//         let authentication = item.authentication;
//         match authentication {
//             Some(auth) => match auth {
//                 dozer_admin_grpc::test_connection_request::Authentication::Postgres(
//                     postgres_auth,
//                 ) => Ok(Connection {
//                     id: None,
//                     db_type: DBType::Postgres,
//                     authentication: connection::Authentication::PostgresAuthentication {
//                         user: postgres_auth.user,
//                         password: postgres_auth.password,
//                         host: postgres_auth.host,
//                         port: postgres_auth.port.parse::<u32>().unwrap(),
//                         database: postgres_auth.database,
//                     },
//                     name: postgres_auth.name,
//                 }),
//             },
//             None => Err("Missing Authentication info".to_owned())?,
//         }
//     }
// }
// impl TryFrom<CreateConnectionRequest> for Connection {
//     type Error = &'static str;
//     fn try_from(item: CreateConnectionRequest) -> Result<Self, Self::Error> {
//         let authentication = item.authentication;
//         match authentication {
//             Some(auth) => match auth {
//                 dozer_admin_grpc::create_connection_request::Authentication::Postgres(
//                     postgres_auth,
//                 ) => {
//                     return Ok(Connection {
//                         db_type: DBType::Postgres,
//                         authentication: connection::Authentication::PostgresAuthentication {
//                             user: postgres_auth.user,
//                             password: postgres_auth.password,
//                             host: postgres_auth.host,
//                             port: postgres_auth.port.parse::<u32>().unwrap(),
//                             database: postgres_auth.database,
//                         },
//                         name: postgres_auth.name,
//                         id: None,
//                     })
//                 }
//             },
//             None => Err("Missing Authentication"),
//         }
//     }
// }

// impl TryFrom<RefreshConfig> for dozer_admin_grpc::RefreshConfig {
//     type Error = Box<dyn Error>;
//     fn try_from(item: RefreshConfig) -> Result<Self, Self::Error> {
//         //dozer_admin_grpc::refresh_config::Config::Day(())
//         let config = match item {
//             RefreshConfig::Hour { minute } => {
//                 refresh_config::Config::Hour(dozer_admin_grpc::RefreshConfigHour { minute })
//             }
//             RefreshConfig::Day { time } => {
//                 refresh_config::Config::Day(dozer_admin_grpc::RefreshConfigDay { time })
//             }
//             RefreshConfig::CronExpression { expression } => refresh_config::Config::CronExpression(
//                 dozer_admin_grpc::RefreshConfigCronExpression { expression },
//             ),
//             RefreshConfig::RealTime => {
//                 refresh_config::Config::Realtime(dozer_admin_grpc::RefreshConfigRealTime {
//                 })
//             }
//         };
//         Ok(dozer_admin_grpc::RefreshConfig {
//             config: Some(config),
//         })
//     }
// }
// impl TryFrom<HistoryType> for dozer_admin_grpc::HistoryType {
//     type Error = Box<dyn Error>;
//     fn try_from(item: HistoryType) -> Result<Self, Self::Error> {
//         match item {
//             HistoryType::Master(master_history_config) => {
//                 let result = master_history_type::Config::try_from(master_history_config)?;
//                 let result = history_type::Type::Master(dozer_admin_grpc::MasterHistoryType {
//                     config: Some(result),
//                 });
//                 let result = dozer_admin_grpc::HistoryType {
//                     r#type: Some(result),
//                 };
//                 return Ok(result);
//             }
//             HistoryType::Transactional(transactional_history_config) => {
//                 let result =
//                     transactional_history_type::Config::try_from(transactional_history_config)?;
//                 let result =
//                     history_type::Type::Transactional(dozer_admin_grpc::TransactionalHistoryType {
//                         config: Some(result),
//                     });
//                 let result = dozer_admin_grpc::HistoryType {
//                     r#type: Some(result),
//                 };
//                 return Ok(result);
//             }
//         }
//     }
// }
// impl TryFrom<TransactionalHistoryConfig> for transactional_history_type::Config {
//     type Error = Box<dyn Error>;
//     fn try_from(item: TransactionalHistoryConfig) -> Result<Self, Self::Error> {
//         match item {
//             TransactionalHistoryConfig::RetainPartial {
//                 timestamp_field,
//                 retention_period,
//             } => Ok(transactional_history_type::Config::AppendOnly(
//                 dozer_admin_grpc::TransactionalHistoryConfigRetainPartial {
//                     timestamp_field,
//                     retention_period,
//                 },
//             )),
//         }
//     }
// }
// impl TryFrom<MasterHistoryConfig> for master_history_type::Config {
//     type Error = Box<dyn Error>;
//     fn try_from(item: MasterHistoryConfig) -> Result<Self, Self::Error> {
//         match item {
//             MasterHistoryConfig::AppendOnly {
//                 unique_key_field,
//                 open_date_field,
//                 closed_date_field,
//             } => {
//                 return Ok(master_history_type::Config::AppendOnly(
//                     dozer_admin_grpc::MasterHistoryConfigAppendOnly {
//                         r#type: "append_only".to_owned(),
//                         unique_key_field,
//                         open_date_field,
//                         closed_date_field,
//                     },
//                 ))
//             }
//             MasterHistoryConfig::Overwrite => Ok(master_history_type::Config::Overwrite(
//                 dozer_admin_grpc::MasterHistoryConfigOverwrite {
//                     r#type: "overwrite".to_owned(),
//                 },
//             )),
//         }
//     }
// }
// impl TryFrom<Source> for dozer_admin_grpc::SourceInfo {
//     type Error = Box<dyn Error>;
//     fn try_from(item: Source) -> Result<Self, Self::Error> {
//         let connection_info = ConnectionInfo::try_from(item.connection)?;
//         let history_type_value = dozer_admin_grpc::HistoryType::try_from(item.history_type)?;
//         let refresh_config_value = dozer_admin_grpc::RefreshConfig::try_from(item.refresh_config)?;
//         return Ok(dozer_admin_grpc::SourceInfo {
//             name: item.name,
//             dest_table_name: item.dest_table_name,
//             source_table_name: item.source_table_name,
//             connection_id: Some(connection_info.clone().id),
//             connection: Some(connection_info),
//             history_type: Some(history_type_value),
//             refresh_config: Some(refresh_config_value),
//         });
//     }
// }
// // impl TryFrom<dozer_admin_grpc::SourceInfo> for Source {
// //     type Error = Box<dyn Error>;
// //     fn try_from(item: dozer_admin_grpc::SourceInfo) -> Result<Self, Self::Error> {

// //         Ok(Source {
// //             id: None,
// //             name: item.name,
// //             dest_table_name: item.dest_table_name,
// //             source_table_name: item.source_table_name,
// //             connection: todo!(),
// //             history_type: todo!(),
// //             refresh_config: todo!(),
// //         })
// //     }

// // }

// #[cfg(test)]
// mod test {
//     use crate::server::dozer_admin_grpc::{
//         create_connection_request::Authentication, ConnectionType, CreateConnectionRequest,
//         PostgresAuthentication,
//     };
//     use dozer_orchestrator::orchestration::models::connection::Connection;

//     #[test]
//     fn success_connection_from_request() {
//         let test_connection_request: CreateConnectionRequest = CreateConnectionRequest {
//             r#type: 0,
//             authentication: Some(Authentication::Postgres(PostgresAuthentication {
//                 database: "pagila".to_owned(),
//                 user: "postgres".to_owned(),
//                 host: "localhost".to_owned(),
//                 port: "5432".to_owned(),
//                 name: "postgres".to_owned(),
//                 password: "postgres".to_owned(),
//             })),
//         };
//         let converted = Connection::try_from(test_connection_request);
//         assert!(converted.is_ok())
//     }
//     #[test]
//     fn err_connection_from_request() {
//         let test_connection_request: CreateConnectionRequest = CreateConnectionRequest {
//             r#type: 0,
//             authentication: None,
//         };
//         let converted = Connection::try_from(test_connection_request);
//         assert!(converted.is_err())
//     }
//     #[test]
//     fn success_from_i32_to_connection_type() {
//         let converted = ConnectionType::try_from(0);
//         assert!(converted.is_ok());
//         assert_eq!(converted.unwrap(), ConnectionType::Postgres);
//     }
//     #[test]
//     fn err_from_i32_to_connection_type() {
//         let converted = ConnectionType::try_from(100).map_err(|err| err.to_string());
//         assert!(converted.is_err());
//         assert_eq!(converted.err().unwrap(), "ConnectionType enum not match");
//     }
// }
