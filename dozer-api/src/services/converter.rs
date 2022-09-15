use std::convert::From;

use dozer_shared::types::{ColumnInfo, TableInfo};

use crate::{
    db::models::connection::Connection,
    server::dozer_api_grpc::{
        self, connection_info, ConnectionInfo, CreateConnectionResponse, PostgresAuthentication,
    },
};

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
