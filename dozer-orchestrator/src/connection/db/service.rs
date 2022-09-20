use crate::connection::traits::db_persistent::DbPersistentTrait;
use super::models as DBModels;
use super::pool::{DbPool, establish_connection};
use diesel::prelude::*;
use diesel::{insert_into, RunQueryDsl, SqliteConnection};
use super::schema::connections::dsl::*;
use std::error::Error;
#[derive(Clone)]
pub struct ConnectionDbSvc {
    db_connection: DbPool,
}

impl ConnectionDbSvc {
    pub fn new(database_url: String) -> Self {
        let db_connection = establish_connection(database_url);
        Self { db_connection }
    }
}

impl DbPersistentTrait<DBModels::connection::Connection> for ConnectionDbSvc {
    fn get_multiple(&self) -> Result<Vec<DBModels::connection::Connection>, Box<dyn Error>> {
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

    fn save(&self, input: DBModels::connection::Connection) -> Result<String, Box<dyn Error>> {
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

    fn get_by_id(&self, connection_id: String) -> Result<DBModels::connection::Connection, Box<dyn Error>> {
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
