use crate::{
    db::{
        application::ApplicationDetail, connection::DbConnection, endpoint::DbEndpoint,
        source::DBSource,
    },
    server::dozer_admin_grpc::{self},
};
use dozer_types::{
    models::{
        self,
        api_config::{ApiConfig, ApiGrpc, ApiInternal, ApiRest},
        api_endpoint::{ApiEndpoint, ApiIndex},
        app_config::Config,
        source::{RefreshConfig, Source},
    },
    types::Schema,
};
use std::{convert::From, error::Error};

//TODO: Add grpc method to create ApiConfig
pub fn default_api_config() -> ApiConfig {
    ApiConfig {
        rest: Some(ApiRest {
            port: 8080,
            url: "[::0]".to_owned(),
            cors: true,
        }),
        grpc: Some(ApiGrpc {
            port: 50051,
            url: "[::0]".to_owned(),
            cors: true,
            web: true,
        }),
        auth: false,
        internal: Some(ApiInternal {
            port: 50052,
            host: "[::1]".to_owned(),
        }),
        ..Default::default()
    }
}

fn convert_to_source(input: (DBSource, DbConnection)) -> Result<Source, Box<dyn Error>> {
    let db_source = input.0;
    let connection = models::connection::Connection::try_from(input.1)?;
    Ok(Source {
        id: Some(db_source.id),
        app_id: Some(db_source.app_id),
        name: db_source.name,
        table_name: db_source.table_name,
        columns: vec![],
        connection: Some(connection),
        refresh_config: Some(RefreshConfig::default()),
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
        sql: input.sql,
        index: Some(ApiIndex {
            primary_key: primary_keys_arr,
        }),
        app_id: Some(input.app_id),
    })
}

impl TryFrom<ApplicationDetail> for Config {
    type Error = Box<dyn Error>;
    fn try_from(input: ApplicationDetail) -> Result<Self, Self::Error> {
        let sources_connections: Vec<(Source, models::connection::Connection)> = input
            .sources_connections
            .iter()
            .map(|sc| {
                let source = convert_to_source(sc.to_owned()).unwrap();
                let connection = models::connection::Connection::try_from(sc.to_owned().1).unwrap();
                (source, connection)
            })
            .collect();
        let endpoints = input
            .endpoints
            .iter()
            .map(|sc| convert_to_api_endpoint(sc.to_owned()).unwrap())
            .collect();
        let sources = sources_connections
            .iter()
            .map(|sc| sc.0.to_owned())
            .collect();
        let connections = sources_connections
            .iter()
            .map(|sc| sc.1.to_owned())
            .collect();

        Ok(Config {
            sources,
            endpoints,
            app_name: input.app.name,
            api: Some(default_api_config()),
            connections,
            ..Default::default()
        })
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
#[cfg(test)]
mod test {
    use crate::{
        db::connection::DbConnection,
        db::{
            application::{Application, ApplicationDetail},
            endpoint::DbEndpoint,
            source::DBSource,
        },
    };
    use dozer_types::models::{app_config::Config, connection::DBType};

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
            sql: "Select id, user_id from users;".to_owned(),
            primary_keys: "id, user_id".to_owned(),
            ..Default::default()
        }
    }
    fn fake_dbconnection(db_type: DBType) -> DbConnection {
        match db_type {
            DBType::Postgres => DbConnection {
                auth: r#"{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}"#.to_owned(),
                name: "postgres_connection".to_owned(),
                db_type: "postgres".to_owned(),
                ..Default::default()
            },
            DBType::Ethereum => DbConnection {
                auth: r#"{"Postgres":{"database":"users","user":"postgres","host":"localhost","port":5432,"password":"postgres"}}"#.to_owned(),
                name: "eth_connection".to_owned(),
                db_type: "ethereum".to_owned(),
                ..Default::default()
            },
            DBType::Events => DbConnection {
                auth: r#"{"Events":{"database":"users"}}}"#.to_owned(),
                name: "events_connection".to_owned(),
                db_type: "events".to_owned(),
                ..Default::default()
            },
            DBType::Snowflake => DbConnection {
                auth: r#"{"Snowflake":{"server":"tx06321.eu-north-1.aws.snowflakecomputing.com","port":"443","user":"karolisgud","password":"uQ@8S4856G9SHP6","database":"DOZER_SNOWFLAKE_SAMPLE_DATA","schema":"PUBLIC","warehouse":"TEST","driver":"{/opt/snowflake/snowflakeodbc/lib/universal/libSnowflake.dylib}"}}"#.to_owned(),
                name: "snowflake_connection".to_owned(),
                db_type: "snowflake".to_owned(),
                ..Default::default()
            },
            DBType::Kafka => DbConnection {
                auth: r#"{"Kafka":{"broker":"localhost:9092","topic":"dbserver1.public.products"}}"#.to_owned(),
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
        let converted = Config::try_from(application_detail.to_owned());
        assert!(converted.is_ok());
        assert_eq!(
            converted.unwrap().sources.len(),
            application_detail.sources_connections.len()
        )
    }
}
