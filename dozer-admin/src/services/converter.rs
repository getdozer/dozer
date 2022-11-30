use crate::{
    db::{
        application::ApplicationDetail, connection::DbConnection, endpoint::DbEndpoint,
        source::DBSource,
    },
    server::dozer_admin_grpc::{
        self, authentication, ConnectionInfo, EthereumAuthentication, PostgresAuthentication,
        SnowflakeAuthentication,
    },
};
use dozer_types::{
    ingestion_types::EthFilter,
    models::{
        self,
        api_endpoint::{ApiEndpoint, ApiIndex},
        source::Source,
    },
    types::Schema,
};
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
                    wss_url,
                    filter: Some(dozer_admin_grpc::EthereumFilter::try_from(filter).unwrap()),
                })
            }
            models::connection::Authentication::SnowflakeAuthentication {
                server,
                port,
                user,
                password,
                database,
                schema,
                warehouse,
                driver,
            } => authentication::Authentication::Snowflake(SnowflakeAuthentication {
                server,
                port,
                user,
                password,
                database,
                schema,
                driver,
                warehouse,
            }),
            models::connection::Authentication::Events {} => todo!(),
            _ => todo!(),
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
                    let eth_filter = match ethereum_authentication.filter {
                        Some(filter) => EthFilter {
                            from_block: filter.from_block,
                            addresses: filter.addresses,
                            topics: filter.topics,
                        },
                        None => EthFilter {
                            ..Default::default()
                        },
                    };
                    models::connection::Authentication::EthereumAuthentication {
                        filter: eth_filter,
                        wss_url: ethereum_authentication.wss_url,
                    }
                }
                authentication::Authentication::Snowflake(snow_flake) => {
                    models::connection::Authentication::SnowflakeAuthentication {
                        server: snow_flake.server,
                        port: snow_flake.port,
                        user: snow_flake.user,
                        password: snow_flake.password,
                        database: snow_flake.database,
                        schema: snow_flake.schema,
                        warehouse: snow_flake.warehouse,
                        driver: snow_flake.driver,
                    }
                }
            };
            Ok(result)
        }
    }
}
#[cfg(test)]
mod test {
    use crate::{
        db::connection::DbConnection,
        db::{
            application::{Application, ApplicationDetail},
            endpoint::DbEndpoint,
            source::DBSource,
        },
        services::converter::{
            convert_to_api_endpoint, convert_to_source, dozer_admin_grpc::ConnectionType,
            ConnectionInfo,
        },
    };
    use dozer_types::models::{
        api_endpoint::ApiIndex,
        connection::{self, DBType},
    };

    fn fake_application() -> Application {
        let generated_id = uuid::Uuid::new_v4().to_string();
        Application {
            id: generated_id,
            name: "app_name".to_owned(),
            ..Default::default()
        }
    }
    fn fake_db_endpoint() -> DbEndpoint {
        DbEndpoint {
            name: "endpoint_name".to_owned(),
            path: "/users".to_owned(),
            enable_rest: true,
            enable_grpc: true,
            sql: "Select id, user_id from users;".to_owned(),
            primary_keys: "id, user_id".to_owned(),
            ..Default::default()
        }
    }
    fn fake_dbconnection(db_type: DBType) -> DbConnection {
        match db_type {
            DBType::Postgres => DbConnection {
                auth: r#"{"authentication":{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}}"#.to_owned(),
                name: "postgres_connection".to_owned(),
                db_type: "postgres".to_owned(),
                ..Default::default()
            },
            DBType::Ethereum => DbConnection {
                auth: r#"{"authentication":{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}}"#.to_owned(),
                name: "eth_connection".to_owned(),
                db_type: "eth".to_owned(),
                ..Default::default()
            },
            DBType::Events => DbConnection {
                auth: r#"{"authentication":{"Events":{"database":"users"}}}"#.to_owned(),
                name: "events_connection".to_owned(),
                db_type: "events".to_owned(),
                ..Default::default()
            },
            DBType::Snowflake => DbConnection {
                auth: r#"{"authentication":{"Snowflake":{"server":"tx06321.eu-north-1.aws.snowflakecomputing.com","port":"443","user":"karolisgud","password":"uQ@8S4856G9SHP6","database":"DOZER_SNOWFLAKE_SAMPLE_DATA","schema":"PUBLIC","warehouse":"TEST","driver":"{/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib}"}}}"#.to_owned(),
                name: "snowflake_connection".to_owned(),
                db_type: "snowflake".to_owned(),
                ..Default::default()
            },
            DBType::Kafka => DbConnection {
                auth: r#"{"authentication":{"Kafka":{"broker":"localhost:9092","topic":"dbserver1.public.products"}}}"#.to_owned(),
                name: "kafka_debezium_connection".to_owned(),
                db_type: "kafka".to_owned(),
                ..Default::default()
            }
        }
    }

    fn fake_sources_connections() -> Vec<(DBSource, DbConnection)> {
        let vec_db_type = vec![DBType::Postgres, DBType::Ethereum, DBType::Snowflake];
        let sources_connections: Vec<(DBSource, DbConnection)> = vec_db_type
            .iter()
            .map(|db_type| {
                let db_connection = fake_dbconnection(db_type.to_owned());
                let generated_id = uuid::Uuid::new_v4().to_string();
                let db_source = DBSource {
                    id: generated_id,
                    name: format!("{:}_source", db_type),
                    table_name: "some_table_name".to_owned(),
                    ..Default::default()
                };
                (db_source, db_connection)
            })
            .collect();
        sources_connections
    }

    #[test]
    fn success_from_db_application_to_dozer_config() {
        let sources_connections = fake_sources_connections();
        let application_detail = ApplicationDetail {
            app: fake_application(),
            sources_connections,
            endpoints: vec![fake_db_endpoint()],
        };
        let converted = dozer_orchestrator::cli::Config::try_from(application_detail.to_owned());
        assert!(converted.is_ok());
        assert_eq!(
            converted.unwrap().sources.len(),
            application_detail.sources_connections.len()
        )
    }

    #[test]
    fn success_from_i32_to_connection_type() {
        let converted = ConnectionType::try_from(0);
        assert!(converted.is_ok());
        assert_eq!(converted.unwrap(), ConnectionType::Postgres);
    }

    #[test]
    fn success_from_db_source_to_source() {
        let db_source = DBSource {
            name: "source_name".to_owned(),
            table_name: "table_name".to_owned(),
            ..Default::default()
        };
        let db_connection = DbConnection {
            auth: r#"{"authentication":{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}}"#.to_owned(),
            name: "postgres_connection".to_owned(),
            db_type: "postgres".to_owned(),
            ..Default::default()
        };
        let converted = convert_to_source((db_source.to_owned(), db_connection.to_owned()));
        assert!(converted.is_ok());
        let converted_source = converted.unwrap();
        assert_eq!(converted_source.name, db_source.name);
        let connection = ConnectionInfo::try_from(db_connection).unwrap();
        let connection = connection::Connection::try_from(connection).unwrap();
        assert_eq!(converted_source.connection.db_type, DBType::Postgres);
        assert_eq!(converted_source.connection, connection);
    }

    #[test]
    fn success_from_db_endpoint_to_api_endpoint() {
        let db_endpoint = DbEndpoint {
            name: "endpoint_name".to_owned(),
            path: "/users".to_owned(),
            enable_rest: true,
            enable_grpc: true,
            sql: "Select id, user_id from users;".to_owned(),
            primary_keys: "id, user_id".to_owned(),
            ..Default::default()
        };
        let converted = convert_to_api_endpoint(db_endpoint.to_owned());
        assert!(converted.is_ok());
        let converted_endpoint = converted.unwrap();
        assert_eq!(converted_endpoint.name, db_endpoint.name);
        assert_eq!(
            converted_endpoint.index,
            ApiIndex {
                primary_key: db_endpoint
                    .primary_keys
                    .split(',')
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            }
        );
    }

    #[test]
    fn err_from_i32_to_connection_type() {
        let converted = ConnectionType::try_from(100).map_err(|err| err.to_string());
        assert!(converted.is_err());
        assert_eq!(converted.err().unwrap(), "ConnectionType enum not match");
    }
}
