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
}
