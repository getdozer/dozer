use diesel::r2d2::ConnectionManager;
use diesel::sqlite::SqliteConnection;
use r2d2::Pool;

pub type DbPool = Pool<ConnectionManager<SqliteConnection>>;
pub fn establish_connection(database_url: String) -> DbPool {
    let manager = ConnectionManager::<SqliteConnection>::new(&database_url);
    r2d2::Pool::builder()
        .build(manager)
        .expect("Failed to create DB pool.")
}
