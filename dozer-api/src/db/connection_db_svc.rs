use super::models::connection::Connection;
use super::pool::DbPool;
use crate::db::models as DBModels;
use crate::db::schema;
use diesel::prelude::*;
use diesel::{insert_into, RunQueryDsl, SqliteConnection};
use schema::connections::dsl::*;
use std::error::Error;
use crate::db::connection_db_trait::ConnectionDbTrait;
#[derive(Clone)]
pub struct ConnectionDbSvc {
    db_connection: DbPool,
}

impl ConnectionDbSvc {
    pub fn new(db_connection: DbPool) -> Self {
        Self { db_connection }
    }
}

impl ConnectionDbTrait<Connection> for ConnectionDbSvc {
    fn get_connections(&self) -> Result<Vec<Connection>, Box<dyn Error>> {
        let db = self.db_connection.get();
        if db.is_err() {
            return Err(Box::new(db.err().unwrap()));
        }
        let db: &SqliteConnection = &db.unwrap();
        let query_result: Result<Vec<DBModels::connection::Connection>, diesel::result::Error> =
            connections.load(db);
        match query_result {
            Ok(result) => Ok(result),
            Err(err) => Err(Box::new(err)),
        }
    }

    fn save_connection(&self, input: Connection) -> Result<String, Box<dyn Error>> {
        let db = self.db_connection.get();
        if db.is_err() {
            return Err(Box::new(db.err().unwrap()));
        }
        let db: &SqliteConnection = &db.unwrap();
        let _inserted_rows = insert_into(connections)
            .values((
                auth.eq(&input.auth),
                db_type.eq("postgres"),
                id.eq(input.id.clone()),
            ))
            .execute(db);
        if let Err(err) = _inserted_rows {
            return Err(Box::new(err));
        } else {
            Ok(input.id)
        }
    }

    fn get_connection_by_id(&self, connection_id: String) -> Result<Connection, Box<dyn Error>> {
        let db = self.db_connection.get();
        if db.is_err() {
            return Err(Box::new(db.err().unwrap()));
        }
        let db: &SqliteConnection = &db.unwrap();
        let result = connections
            .filter(id.eq(connection_id))
            .first::<DBModels::connection::Connection>(db);
        if let Err(err) = result {
            return Err(Box::new(err));
        } else {
            Ok(result.ok().unwrap())
        }
    }
}
