use crate::db::{models as DBModels, schema};
use crate::server::dozer_api_grpc::GetAllConnectionRequest;
use diesel::prelude::*;
use diesel::{insert_into, result::Error, RunQueryDsl, SqliteConnection};
use schema::connections::dsl::*;

pub fn get_connections(
    db: &SqliteConnection,
    _param: GetAllConnectionRequest,
) -> Result<Vec<DBModels::connection::Connection>, Error> {
    let result: Result<Vec<DBModels::connection::Connection>, diesel::result::Error> =
        connections.load(db);
    result
}

pub fn get_connection_by_id(
    db: &SqliteConnection,
    connection_id: String,
) -> Result<DBModels::connection::Connection, Error> {
    let result = connections
        .filter(id.eq(connection_id))
        .first::<DBModels::connection::Connection>(db);
    result
}

pub fn create_connection(
    db: &SqliteConnection,
    connection: DBModels::connection::Connection,
) -> Result<DBModels::connection::Connection, Error> {
    let _inserted_rows = insert_into(connections)
        .values((
            auth.eq(&connection.auth),
            db_type.eq("postgres"),
            id.eq(connection.id.clone()),
        ))
        .execute(db);
    if let Err(e) = _inserted_rows {
        return Err(e);
    } else {
        let new_inserted = connections
            .filter(id.eq(connection.id))
            .first::<DBModels::connection::Connection>(db);
        new_inserted
    }
}
