use diesel::r2d2::ConnectionManager;
use r2d2::Pool;

use super::Connection;

pub type DbPool = Pool<ConnectionManager<Connection>>;
pub fn establish_connection(database_url: String) -> DbPool {
    let manager = ConnectionManager::new(database_url);
    r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create DB pool.")
}
