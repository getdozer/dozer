use diesel::SqliteConnection;
use crate::{diesel::RunQueryDsl, models::Connection};

pub fn get_connections(db: &SqliteConnection) -> Vec<Connection> {
    use crate::schema::connections::dsl::*;
    
    let result = connections.load::<Connection>(db);

    match result {
        Ok(fetched_connections) => fetched_connections,
        Err(error) => {
            panic!("{}", error)
        }
    }
}