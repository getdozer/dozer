use crate::{
    db::{
        application::ApplicationDetail, connection::DbConnection, endpoint::DbEndpoint,
        source::DBSource,
    },
    server::dozer_admin_grpc::{
        self, authentication, ConnectionInfo, EthereumAuthentication, PostgresAuthentication,
    },
};
use dozer_types::ingestion_types::EthFilter;
use dozer_types::models::{
    self,
    api_endpoint::{ApiEndpoint, ApiIndex},
    source::Source,
};
use dozer_types::types::Schema;
use std::{convert::From, error::Error};

fn convert_to_source(input: (DBSource, DbConnection)) -> Result<Source, Box<dyn Error>> {
    let db_source = input.0;
    let connection_info = ConnectionInfo::try_from(input.1)?;
    let connection = models::connection::Connection::try_from(connection_info)?;
    Ok(Source {
        id: Some(db_source.id),
        name: db_source.name,
        table_name: db_source.table_name,
        connection,
        history_type: None,
        refresh_config: models::source::RefreshConfig::RealTime,
    })
}

fn convert_to_api_endpoint(input: DbEndpoint) -> Result<ApiEndpoint, Box<dyn Error>> {
    let primary_keys_arr: Vec<String> = input
        .primary_keys
        .split(',')
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    Ok(ApiEndpoint {
        id: Some(input.id),
        name: input.name,
        path: input.path,
        enable_rest: input.enable_rest,
        enable_grpc: input.enable_grpc,
        sql: input.sql,
        index: ApiIndex {
            primary_key: primary_keys_arr,
        },
    })
}

impl TryFrom<ApplicationDetail> for dozer_orchestrator::cli::Config {
    type Error = Box<dyn Error>;
    fn try_from(input: ApplicationDetail) -> Result<Self, Self::Error> {
        let sources = input
            .sources_connections
            .iter()
            .map(|sc| convert_to_source(sc.to_owned()).unwrap())
            .collect();
        let endpoints = input
            .endpoints
            .iter()
            .map(|sc| convert_to_api_endpoint(sc.to_owned()).unwrap())
            .collect();
        Ok(dozer_orchestrator::cli::Config { sources, endpoints })
    }
}

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
            1 => models::connection::DBType::Snowflake,
            3 => models::connection::DBType::Ethereum,
            _ => models::connection::DBType::Events,
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
impl TryFrom<EthFilter> for dozer_admin_grpc::EthereumFilter {
    type Error = Box<dyn Error>;

    fn try_from(item: EthFilter) -> Result<Self, Self::Error> {
        Ok(dozer_admin_grpc::EthereumFilter {
            from_block: item.from_block,
            addresses: item.addresses,
            topics: item.topics,
        })
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
            models::connection::Authentication::EthereumAuthentication { filter, wss_url } => {
                authentication::Authentication::Ethereum(EthereumAuthentication {
                    wss_url: wss_url,
                    filter: Some(dozer_admin_grpc::EthereumFilter::try_from(filter).unwrap()),
                })
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
                authentication::Authentication::Ethereum(ethereum_authentication) => {
                    let eth_filter = ethereum_authentication.filter.unwrap();
                    models::connection::Authentication::EthereumAuthentication {
                        filter: EthFilter {
                            from_block: eth_filter.from_block,
                            addresses: eth_filter.addresses,
                            topics: eth_filter.topics,
                        },
                        wss_url: ethereum_authentication.wss_url,
                    }
                }
                authentication::Authentication::Snowflake(_) => todo!(),
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
