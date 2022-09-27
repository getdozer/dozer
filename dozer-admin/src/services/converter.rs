use crate::server::dozer_api_grpc::{
    self, connection_info::Authentication, create_source_request, ConnectionInfo, ConnectionType,
    CreateConnectionRequest, CreateConnectionResponse, PostgresAuthentication,
    TestConnectionRequest,
};
use dozer_orchestrator::orchestration::models::{
    connection::{self, Connection, DBType},
    source::{HistoryType, MasterHistoryConfig, RefreshConfig, Source, TransactionalHistoryConfig},
};
use dozer_types::types::{ColumnInfo, TableInfo};
use std::{convert::From, error::Error};

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

impl TryFrom<i32> for ConnectionType {
    type Error = &'static str;
    fn try_from(item: i32) -> Result<Self, Self::Error> {
        match item {
            0 => Ok(ConnectionType::Postgres),
            _ => Err("ConnectionType enum not match"),
        }
    }
}
impl TryFrom<dozer_api_grpc::transactional_history_type::Config> for TransactionalHistoryConfig {
    type Error = Box<dyn Error>;
    fn try_from(
        item: dozer_api_grpc::transactional_history_type::Config,
    ) -> Result<Self, Self::Error> {
        match item {
            dozer_api_grpc::transactional_history_type::Config::AppendOnly(retain_partial) => {
                return Ok(TransactionalHistoryConfig::RetainPartial {
                    timestamp_field: retain_partial.timestamp_field,
                    retention_period: retain_partial.retention_period,
                })
            }
        }
    }
}
impl TryFrom<dozer_api_grpc::master_history_type::Config> for MasterHistoryConfig {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_api_grpc::master_history_type::Config) -> Result<Self, Self::Error> {
        match item {
            dozer_api_grpc::master_history_type::Config::AppendOnly(append_only_config) => {
                return Ok(MasterHistoryConfig::AppendOnly {
                    unique_key_field: append_only_config.unique_key_field,
                    open_date_field: append_only_config.open_date_field,
                    closed_date_field: append_only_config.closed_date_field,
                })
            }
            dozer_api_grpc::master_history_type::Config::Overwrite(_) => {
                return Ok(MasterHistoryConfig::Overwrite)
            }
        }
    }
}
impl TryFrom<dozer_api_grpc::RefreshConfig> for RefreshConfig {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_api_grpc::RefreshConfig) -> Result<Self, Self::Error> {
        if item.config.is_none() {
            return Err("RefreshConfig is empty".to_owned())?;
        }
        let config = item.config.unwrap();
        match config {
            dozer_api_grpc::refresh_config::Config::Hour(config_hour) => Ok(RefreshConfig::Hour {
                minute: config_hour.minute,
            }),
            dozer_api_grpc::refresh_config::Config::Day(config_day) => Ok(RefreshConfig::Day {
                time: config_day.time,
            }),
            dozer_api_grpc::refresh_config::Config::CronExpression(config_cron) => {
                Ok(RefreshConfig::CronExpression {
                    expression: config_cron.expression,
                })
            }
            dozer_api_grpc::refresh_config::Config::Realtime(_) => Ok(RefreshConfig::RealTime),
        }
    }
}
impl TryFrom<dozer_api_grpc::HistoryType> for HistoryType {
    type Error = Box<dyn Error>;
    fn try_from(item: dozer_api_grpc::HistoryType) -> Result<Self, Self::Error> {
        if item.r#type.is_none() {
            return Err("HistoryType is empty".to_owned())?;
        }
        let config = item.r#type.unwrap();
        match config {
            dozer_api_grpc::history_type::Type::Master(master_history) => {
                let master_history_config = master_history.config;
                if master_history_config.is_none() {
                    return Err("Missing master_history_config".to_owned())?;
                }
                let master_history_config = master_history_config.unwrap();
                let master_history_config = MasterHistoryConfig::try_from(master_history_config)?;
                return Ok(HistoryType::Master(master_history_config));
            }
            dozer_api_grpc::history_type::Type::Transactional(transactional_history) => {
                let transactional_history_config = transactional_history.config;
                if transactional_history_config.is_none() {
                    return Err("Missing transactional_history_config".to_owned())?;
                }
                let transactional_history_config = transactional_history_config.unwrap();
                let transactional_history_config =
                    TransactionalHistoryConfig::try_from(transactional_history_config)?;
                return Ok(HistoryType::Transactional(transactional_history_config));
            }
        }
    }
}
impl TryFrom<TestConnectionRequest> for Connection {
    type Error = Box<dyn Error>;
    fn try_from(item: TestConnectionRequest) -> Result<Self, Self::Error> {
        let authentication = item.authentication;
        match authentication {
            Some(auth) => match auth {
                dozer_api_grpc::test_connection_request::Authentication::Postgres(
                    postgres_auth,
                ) => Ok(Connection {
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
                }),
            },
            None => Err("Missing Authentication info".to_owned())?,
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
impl TryFrom<Source> for dozer_api_grpc::SourceInfo {
    type Error = Box<dyn Error>;
    fn try_from(item: Source) -> Result<Self, Self::Error> {
        let connection_info = ConnectionInfo::try_from(item.connection)?;
        let history_type = item.history_type;
        return Ok(dozer_api_grpc::SourceInfo {
            name: item.name,
            dest_table_name: item.dest_table_name,
            source_table_name: item.source_table_name,
            connection_id: Some(connection_info.clone().id),
            connection: Some(connection_info),
            history_type: todo!(),
            refresh_config: todo!(),
        });
    }
}
#[cfg(test)]
mod test {
    use crate::server::dozer_api_grpc::{
        create_connection_request::Authentication, ConnectionType, CreateConnectionRequest,
        PostgresAuthentication,
    };
    use dozer_orchestrator::orchestration::models::connection::Connection;

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
