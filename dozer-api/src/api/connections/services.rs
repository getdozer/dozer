use diesel::{SqliteConnection, insert_into};
use diesel::prelude::*;
use crate::db::{schema, models::connection::Connection};
use crate::errors::db_error::DbError;
use crate::models::ConnectionRequest;
use crate::{diesel::RunQueryDsl};
use serde_json;
use schema::connections::dsl::*;

pub fn get_connections(db: &SqliteConnection) -> Vec<Connection>  {
    let result = connections.load::<Connection>(db);
    match result {
        Ok(fetched_connections) => fetched_connections,
        Err(error) => {
            panic!("{}", error)
        }
    }
}

pub fn create_connection(db: &SqliteConnection, connection: ConnectionRequest) -> Result<Connection, DbError<String>> {
    let new_id = uuid::Uuid::new_v4();
    let _inserted_rows =  insert_into(connections)
    .values(
        (auth.eq(serde_json::to_string(&connection.authentication).unwrap()),
        db_type.eq(&connection.r#type.to_string()),
        id.eq(new_id.to_string())
    )).execute(db);
    let new_inserted = connections.filter(id.eq(new_id.to_string())).first::<Connection>(db);
    match new_inserted {
        Ok(new_value) => Ok(new_value),
        Err(error) => {
           Err(DbError {
                details: error.to_string(),
                message: "create_connection error".to_string(),
            })
        }
    }
    // new_inserted
}