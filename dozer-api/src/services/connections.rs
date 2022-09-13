use crate::db::{models as DBModels, schema};
use crate::diesel::RunQueryDsl;
use crate::lib::error::Error;
use crate::lib::errors::db_error::DbError;
use crate::models::ConnectionRequest;
use diesel::prelude::*;
use diesel::{insert_into, SqliteConnection};
use dozer_ingestion::connectors::connector::Connector;
use dozer_ingestion::connectors::postgres;
use dozer_ingestion::connectors::postgres::connector::PostgresConnector;
use dozer_shared::types::TableInfo;
use schema::connections::dsl::*;
use serde_json;

pub fn get_connections(db: &SqliteConnection) -> Vec<DBModels::connection::Connection> {
    let result: Result<Vec<DBModels::connection::Connection>, diesel::result::Error> =
        connections.load(db);
    match result {
        Ok(fetched_connections) => fetched_connections,
        Err(error) => {
            panic!("Load db connections Err {}", error)
        }
    }
}

pub async fn test_connection(connection_input: ConnectionRequest) -> Result<Vec<TableInfo>, Error> {
    let connection_detail = connection_input.authentication;
    let port: String = connection_detail.port.unwrap();
    let conn_str = format!(
        "host={} port={} user={} dbname={} password={}",
        connection_detail.host,
        port,
        connection_detail.user,
        connection_detail.database,
        connection_detail.password
    );
    let postgres_config = postgres::connector::PostgresConfig {
        name: connection_detail.name,
        tables: None,
        conn_str: conn_str.clone(),
    };
    let postgres_connection = PostgresConnector::new(postgres_config);
    let schemas = postgres_connection.get_schema().await;
    Ok(schemas)
}

pub fn create_connection(
    db: &SqliteConnection,
    connection: ConnectionRequest,
) -> Result<DBModels::connection::Connection, DbError<String>> {
    let new_id = uuid::Uuid::new_v4();
    let _inserted_rows = insert_into(connections)
        .values((
            auth.eq(serde_json::to_string(&connection.authentication).unwrap()),
            db_type.eq(&connection.r#type.to_string()),
            id.eq(new_id.to_string()),
        ))
        .execute(db);
    let new_inserted = connections
        .filter(id.eq(new_id.to_string()))
        .first::<DBModels::connection::Connection>(db);
    match new_inserted {
        Ok(new_value) => Ok(new_value),
        Err(error) => Err(DbError {
            details: error.to_string(),
            message: "create_connection error".to_string(),
        }),
    }
}
