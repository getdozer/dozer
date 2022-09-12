use diesel::sqlite::SqliteConnection;
use dotenvy::dotenv;
use std::env;
use r2d2_diesel::ConnectionManager;
use r2d2::Pool;

pub type DbPool = Pool<ConnectionManager<SqliteConnection>>;

pub fn establish_connection() -> DbPool {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<SqliteConnection>::new(&database_url);
    return r2d2::Pool::builder().build(manager).expect("Failed to create DB pool.");
}