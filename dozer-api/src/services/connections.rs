use crate::db::{models, schema};
use crate::diesel::RunQueryDsl;
use crate::lib::error::Error;
use crate::lib::errors::db_error::DbError;
use crate::models::{ConnectionRequest, ConnectionResponse};
use diesel::prelude::*;
use diesel::{insert_into, SqliteConnection};
use dozer_ingestion::connectors::postgres;
use schema::connections::dsl::*;
use serde_json;

pub fn get_connections(db: &SqliteConnection) -> Vec<models::connection::Connection> {
    let result = connections.load::<Connection>(db);
    match result {
        Ok(fetched_connections) => fetched_connections,
        Err(error) => {
            panic!("{}", error)
        }
    }
}

pub async fn test_connection(request: models::connection::Connection) -> Result<(), Error> {
    let connection_input: models::connection::Connection = request.into_inner();
    let connection_detail = connection_input.detail.unwrap();
    let port: u32 = connection_detail.port.to_string().trim().parse().unwrap();
    // let storage_client = &crate::storage_client::initialize().await;
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
    let mut connector = postgres::connector::PostgresConnector::new(postgres_config);
    let schema = connector.get_schema().await;
    Ok("")
    // let mut views = Vec::new();
    // views.push(prost_types::Value {
    //     kind: Some(prost_types::value::Kind::StringValue(String::from(
    //         "views1",
    //     ))),
    // });
    // Ok(Response::new(ConnectionResponse {
    //     response: Some(
    //         dozer_shared::ingestion::connection_response::Response::Success(ConnectionDetails {
    //             table_info: schema,
    //         }),
    //     ),
    // }))
}

pub fn create_connection(
    db: &SqliteConnection,
    connection: ConnectionRequest,
) -> Result<(), DbError<String>> {
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
        .first::<dyn Connection>(db);
    match new_inserted {
        Ok(new_value) => Ok(new_value),
        Err(error) => Err(DbError {
            details: error.to_string(),
            message: "create_connection error".to_string(),
        }),
    }
    // new_inserted
}
