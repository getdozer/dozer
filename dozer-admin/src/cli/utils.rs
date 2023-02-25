use crate::db::pool::establish_connection;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness};
use std::{error::Error, path::Path};

type DB = diesel::sqlite::Sqlite;
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
fn run_migrations(
    connection: &mut impl MigrationHarness<DB>,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    connection.revert_all_migrations(MIGRATIONS)?;
    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}

pub fn get_db_path() -> String {
    "dozer.db".to_string()
}

pub fn init_db() {
    let db_path = get_db_path();
    if !Path::new(&db_path).exists() {
        let db_pool = establish_connection(db_path);
        let mut db_connection = db_pool.get().unwrap();
        // run migration
        run_migrations(&mut db_connection).unwrap();
    }
}
