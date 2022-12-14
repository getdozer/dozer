use crate::server::dozer_admin_grpc::{self};
use dozer_types::{
    models::api_config::{ApiConfig, ApiGrpc, ApiInternal, ApiRest},
    types::Schema,
};
use std::convert::From;

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
        api_internal: Some(ApiInternal {
            port: 50052,
            host: "[::1]".to_owned(),
        }),
        pipeline_internal: Some(ApiInternal {
            port: 50053,
            host: "[::1]".to_owned(),
        }),
        ..Default::default()
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
        db::{application::Application, endpoint::DbEndpoint, source::DBSource},
    };
    use dozer_types::models::connection::DBType;

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
}
